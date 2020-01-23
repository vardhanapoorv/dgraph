package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2/y"
	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"github.com/dgraph-io/dgraph/testutil"
	"github.com/dgraph-io/dgraph/x"
	"google.golang.org/grpc"
)

func getClient() (*dgo.Dgraph, error) {
	ports := []int{9180}
	conns := make([]api.DgraphClient, len(ports))
	for i, port := range ports {
		conn, err := grpc.Dial(fmt.Sprintf("localhost:%v", port), grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Error in creating grpc con: %v", err)
		}

		conns[i] = api.NewDgraphClient(conn)
	}
	dg := dgo.NewDgraphClient(conns...)

	ctx := context.Background()
	for {
		// keep retrying until we succeed or receive a non-retriable error
		err := dg.Login(ctx, x.GrootId, "password")
		if err == nil || !strings.Contains(err.Error(), "Please retry") {
			break
		}
		time.Sleep(time.Second)
	}

	return dg, nil
}

func TestCountIndexInBG(t *testing.T) {
	total := 100000
	numAccts := uint64(total)
	acctsBal := make([]int, total+100000)
	acctsLock := make([]sync.Mutex, total+100000)

	// dg, err := testutil.DgraphClient(testutil.SockAddr)
	dg, err := getClient()
	if err != nil {
		log.Fatalf("Error while getting a dgraph client: %v", err)
	}

	testutil.DropAll(t, dg)
	if err := dg.Alter(context.Background(), &api.Operation{
		Schema: "balance: [string] .",
	}); err != nil {
		log.Fatalf("error in setting up schema :: %v\n", err)
	}

	if err := testutil.AssignUids(uint64(total * 10)); err != nil {
		log.Fatalf("error in assigning UIDs :: %v", err)
	}

	// first insert bank accounts
	fmt.Println("inserting accounts")
	th := y.NewThrottle(10000)
	for i := 1; i <= int(numAccts); i++ {
		th.Do()
		go func(uid int) {
			defer th.Done(nil)
			bb := &bytes.Buffer{}
			acctsBal[uid] = rand.Intn(1000)
			for j := 0; j < acctsBal[uid]; j++ {
				_, err := bb.WriteString(fmt.Sprintf("<%v> <balance> \"%v\" .\n", uid, j))
				if err != nil {
					log.Fatalf("error in mutation %v\n", err)
				}
			}
			dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				SetNquads: bb.Bytes(),
			})
		}(i)
	}
	th.Finish()

	fmt.Println("building indexes in background")
	if err := dg.Alter(context.Background(), &api.Operation{
		Schema: "balance: [string] @count .",
	}); err != nil {
		log.Fatalf("error in adding indexes :: %v\n", err)
	}

	if resp, err := dg.NewReadOnlyTxn().Query(context.Background(), "schema{}"); err != nil {
		log.Fatalf("error in adding indexes :: %v\n", err)
	} else {
		fmt.Printf("new schema: %v\n", resp)
	}

	// perform mutations until ctrl+c
	mutateUID := func(uid int) {
		acctsLock[uid].Lock()
		defer acctsLock[uid].Unlock()
		nb := acctsBal[uid]
		switch rand.Intn(1000) % 3 {
		case 0:
			if _, err := dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				SetNquads: []byte(fmt.Sprintf(`<%v> <balance> "%v" .`, uid, nb)),
			}); err != nil && !errors.Is(err, dgo.ErrAborted) {
				log.Fatalf("error in mutation :: %v\n", err)
			} else if errors.Is(err, dgo.ErrAborted) {
				return
			}
			nb++
		case 1:
			if nb <= 0 {
				return
			}
			if _, err := dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				DelNquads: []byte(fmt.Sprintf(`<%v> <balance> "%v" .`, uid, nb-1)),
			}); err != nil && !errors.Is(err, dgo.ErrAborted) {
				log.Fatalf("error in deletion :: %v\n", err)
			} else if errors.Is(err, dgo.ErrAborted) {
				return
			}
			nb--
		case 2:
			uid = int(atomic.AddUint64(&numAccts, 1))
			if _, err := dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				SetNquads: []byte(fmt.Sprintf(`<%v> <balance> "0" .`, uid)),
			}); err != nil && !errors.Is(err, dgo.ErrAborted) {
				log.Fatalf("error in insertion :: %v\n", err)
			} else if errors.Is(err, dgo.ErrAborted) {
				return
			}
			nb = 1
		}

		acctsBal[uid] = nb
	}

	var sig chan os.Signal
	sig = make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	// perform mutations until ctrl+c
	var swg sync.WaitGroup
	var counter uint64
	quit := make(chan struct{})
	runLoop := func() {
		defer swg.Done()
		for {
			select {
			case <-quit:
				return
			default:
				n := int(atomic.LoadUint64(&numAccts))
				mutateUID(rand.Intn(n) + 1)
				atomic.AddUint64(&counter, 1)
			}
		}
	}
	printStats := func() {
		defer swg.Done()
		for {
			select {
			case <-quit:
				return
			case <-time.After(2 * time.Second):
			}
			fmt.Println("mutations:", atomic.LoadUint64(&counter))
		}
	}
	swg.Add(101)
	for i := 0; i < 100; i++ {
		go runLoop()
	}
	go printStats()
	<-sig
	close(quit)
	swg.Wait()
	signal.Reset(os.Interrupt)
	fmt.Println("mutations done")

	// compute count index
	balIndex := make(map[int][]int)
	for uid := 1; uid <= int(numAccts); uid++ {
		bal := acctsBal[uid]
		balIndex[bal] = append(balIndex[bal], uid)
	}
	for _, aa := range balIndex {
		sort.Ints(aa)
	}

	checkDelete := func(uid int) error {
		q := fmt.Sprintf(`{ q(func: uid(%v)) {balance:count(balance)}}`, uid)
		resp, err := dg.NewReadOnlyTxn().Query(context.Background(), q)
		if err != nil {
			log.Fatalf("error in query: %v :: %v\n", q, err)
		}
		var data struct {
			Q []struct {
				Balance int
			}
		}
		if err := json.Unmarshal(resp.Json, &data); err != nil {
			log.Fatalf("error in json.Unmarshal :: %v", err)
		}

		if len(data.Q) != 1 && data.Q[0].Balance != 0 {
			return fmt.Errorf("found a deleted UID, %v", uid)
		}
		return nil
	}

	// check values now
	checkBalance := func(b int, uids []int) error {
		q := fmt.Sprintf(`{ q(func: eq(count(balance), "%v")) {uid}}`, b)
		resp, err := dg.NewReadOnlyTxn().Query(context.Background(), q)
		if err != nil {
			log.Fatalf("error in query: %v :: %v\n", q, err)
		}
		var data struct {
			Q []struct {
				UID string
			}
		}
		if err := json.Unmarshal(resp.Json, &data); err != nil {
			log.Fatalf("error in json.Unmarshal :: %v", err)
		}

		actual := make([]int, len(data.Q))
		for i, ui := range data.Q {
			v, err := strconv.ParseInt(ui.UID, 0, 64)
			if err != nil {
				return err
			}
			actual[i] = int(v)
		}
		sort.Ints(actual)

		if len(actual) != len(uids) {
			return fmt.Errorf("length not equal :: exp: %v, actual %v", uids, actual)
		}
		for i := range uids {
			if uids[i] != actual[i] {
				return fmt.Errorf("value not equal :: exp: %v, actual %v", uids, actual)
			}
		}

		return nil
	}

	type pair struct {
		key int
		err string
	}
	ch := make(chan pair, numAccts)

	fmt.Println("starting to query")
	var count uint64
	th = y.NewThrottle(50000)
	th.Do()
	go func() {
		defer th.Done(nil)
		for {
			time.Sleep(2 * time.Second)
			cur := atomic.LoadUint64(&count)
			fmt.Printf("%v/%v done\n", cur, len(balIndex))
			if int(cur) == len(balIndex) {
				break
			}
		}
	}()

	for balance, uids := range balIndex {
		th.Do()
		go func(bal int, uidList []int) {
			defer th.Done(nil)
			if bal <= 0 {
				for _, uid := range uidList {
					if err := checkDelete(uid); err != nil {
						ch <- pair{uid, err.Error()}
					}
				}
			} else {
				if err := checkBalance(bal, uidList); err != nil {
					ch <- pair{bal, err.Error()}
				}
			}
			atomic.AddUint64(&count, 1)
		}(balance, uids)
	}
	th.Finish()

	close(ch)
	for p := range ch {
		t.Logf("failed for %v, :: %v\n", p.key, p.err)
	}
}

func TestIndexInBG(t *testing.T) {
	total := 1000000
	numAccts := uint64(total)
	acctsBal := make(map[int]int, numAccts)
	var lock sync.Mutex

	// dg, err := testutil.DgraphClient(testutil.SockAddr)
	dg, err := getClient()
	if err != nil {
		log.Fatalf("Error while getting a dgraph client: %v", err)
	}

	testutil.DropAll(t, dg)
	if err := dg.Alter(context.Background(), &api.Operation{
		Schema: "balance: string .",
	}); err != nil {
		log.Fatalf("error in setting up schema :: %v\n", err)
	}

	if err := testutil.AssignUids(uint64(total * 10)); err != nil {
		log.Fatalf("error in assignig UIDs :: %v", err)
	}

	// first insert bank accounts
	fmt.Println("inserting accounts")
	for i := 1; i <= int(numAccts); {
		bb := &bytes.Buffer{}
		for j := 0; j < 10000; j++ {
			acctsBal[i] = rand.Intn(total * 100)
			_, err := bb.WriteString(fmt.Sprintf("<%v> <balance> \"%v\" .\n", i, acctsBal[i]))
			if err != nil {
				log.Fatalf("error in mutation %v\n", err)
			}
			i++
		}
		dg.NewTxn().Mutate(context.Background(), &api.Mutation{
			CommitNow: true,
			SetNquads: bb.Bytes(),
		})
	}

	fmt.Println("building indexes in background")
	if err := dg.Alter(context.Background(), &api.Operation{
		Schema: "balance: string @index(fulltext, term, exact) .",
	}); err != nil {
		log.Fatalf("error in adding indexes :: %v\n", err)
	}

	if resp, err := dg.NewReadOnlyTxn().Query(context.Background(), "schema{}"); err != nil {
		log.Fatalf("error in adding indexes :: %v\n", err)
	} else {
		fmt.Printf("new schema: %v\n", resp)
	}

	// perform mutations until ctrl+c
	mutateUID := func(uid int) {
		nb := rand.Intn(total * 100)
		switch uid % 3 {
		case 0:
			if _, err := dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				SetNquads: []byte(fmt.Sprintf(`<%v> <balance> "%v" .`, uid, nb)),
			}); err != nil && !errors.Is(err, dgo.ErrAborted) {
				log.Fatalf("error in mutation :: %v\n", err)
			} else if errors.Is(err, dgo.ErrAborted) {
				return
			}
		case 1:
			if _, err := dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				DelNquads: []byte(fmt.Sprintf(`<%v> <balance> * .`, uid)),
			}); err != nil && !errors.Is(err, dgo.ErrAborted) {
				log.Fatalf("error in deletion :: %v\n", err)
			} else if errors.Is(err, dgo.ErrAborted) {
				return
			}
			nb = -1
		case 2:
			uid = int(atomic.AddUint64(&numAccts, 1))
			if _, err := dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				SetNquads: []byte(fmt.Sprintf(`<%v> <balance> "%v" .`, uid, nb)),
			}); err != nil && !errors.Is(err, dgo.ErrAborted) {
				log.Fatalf("error in insertion :: %v\n", err)
			} else if errors.Is(err, dgo.ErrAborted) {
				return
			}
		}

		lock.Lock()
		acctsBal[uid] = nb
		lock.Unlock()
	}

	var sig chan os.Signal
	sig = make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	// perform mutations until ctrl+c
	var swg sync.WaitGroup
	var counter uint64
	quit := make(chan struct{})
	runLoop := func() {
		defer swg.Done()
		for {
			select {
			case <-quit:
				return
			default:
				n := int(atomic.LoadUint64(&numAccts))
				mutateUID(rand.Intn(n) + 1)
				atomic.AddUint64(&counter, 1)
			}
		}
	}
	printStats := func() {
		defer swg.Done()
		for {
			select {
			case <-quit:
				return
			case <-time.After(2 * time.Second):
			}
			fmt.Println("mutations:", atomic.LoadUint64(&counter))
		}
	}
	swg.Add(101)
	for i := 0; i < 100; i++ {
		go runLoop()
	}
	go printStats()
	<-sig
	close(quit)
	swg.Wait()
	signal.Reset(os.Interrupt)
	fmt.Println("mutations done")

	// compute reverse index
	balIndex := make(map[int][]int)
	for uid, bal := range acctsBal {
		balIndex[bal] = append(balIndex[bal], uid)
	}
	for _, aa := range balIndex {
		sort.Ints(aa)
	}

	checkDelete := func(uid int) error {
		q := fmt.Sprintf(`{ q(func: uid(%v)) {balance}}`, uid)
		resp, err := dg.NewReadOnlyTxn().Query(context.Background(), q)
		if err != nil {
			log.Fatalf("error in query: %v :: %v\n", q, err)
		}
		var data struct {
			Q []struct {
				Balance string
			}
		}
		if err := json.Unmarshal(resp.Json, &data); err != nil {
			log.Fatalf("error in json.Unmarshal :: %v", err)
		}

		if len(data.Q) != 0 {
			return fmt.Errorf("found a deleted UID, %v", uid)
		}
		return nil
	}

	// check values now
	checkBalance := func(b int, uids []int) error {
		q := fmt.Sprintf(`{ q(func: anyoftext(balance, "%v")) {uid}}`, b)
		resp, err := dg.NewReadOnlyTxn().Query(context.Background(), q)
		if err != nil {
			log.Fatalf("error in query: %v :: %v\n", q, err)
		}
		var data struct {
			Q []struct {
				UID string
			}
		}
		if err := json.Unmarshal(resp.Json, &data); err != nil {
			log.Fatalf("error in json.Unmarshal :: %v", err)
		}

		actual := make([]int, len(data.Q))
		for i, ui := range data.Q {
			v, err := strconv.ParseInt(ui.UID, 0, 64)
			if err != nil {
				return err
			}
			actual[i] = int(v)
		}
		sort.Ints(actual)

		if len(actual) != len(uids) {
			return fmt.Errorf("length not equal :: exp: %v, actual %v", uids, actual)
		}
		for i := range uids {
			if uids[i] != actual[i] {
				return fmt.Errorf("value not equal :: exp: %v, actual %v", uids, actual)
			}
		}

		return nil
	}

	type pair struct {
		key int
		err string
	}
	ch := make(chan pair, numAccts)

	fmt.Println("starting to query")
	var count uint64
	th := y.NewThrottle(50000)
	th.Do()
	go func() {
		defer th.Done(nil)
		for {
			time.Sleep(2 * time.Second)
			cur := atomic.LoadUint64(&count)
			fmt.Printf("%v/%v done\n", cur, len(balIndex))
			if int(cur) == len(balIndex) {
				break
			}
		}
	}()

	for balance, uids := range balIndex {
		th.Do()
		go func(bal int, uidList []int) {
			defer th.Done(nil)
			if bal == -1 {
				for _, uid := range uidList {
					if err := checkDelete(uid); err != nil {
						ch <- pair{uid, err.Error()}
					}
				}
			} else {
				if err := checkBalance(bal, uidList); err != nil {
					ch <- pair{bal, err.Error()}
				}
			}
			atomic.AddUint64(&count, 1)
		}(balance, uids)
	}
	th.Finish()

	close(ch)
	for p := range ch {
		t.Logf("failed for %v, :: %v\n", p.key, p.err)
	}
}

func TestReverseIndexInBG(t *testing.T) {
	total := 1000000

	dg, err := getClient()
	if err != nil {
		log.Fatalf("Error while getting a dgraph client: %v", err)
	}

	testutil.DropAll(t, dg)
	if err := dg.Alter(context.Background(), &api.Operation{
		Schema: "balance: [uid] .",
	}); err != nil {
		log.Fatalf("error in setting up schema :: %v\n", err)
	}

	if err := testutil.AssignUids(uint64(total * 10)); err != nil {
		log.Fatalf("error in assignig UIDs :: %v", err)
	}

	// first insert bank accounts
	fmt.Println("inserting edges")
	for i := 1; i < total; {
		bb := &bytes.Buffer{}
		for j := 0; j < 10000; j++ {
			_, err := bb.WriteString(fmt.Sprintf("<%v> <balance> <%v> .\n", i, i+1))
			if err != nil {
				log.Fatalf("error in mutation %v\n", err)
			}
			i++
		}
		dg.NewTxn().Mutate(context.Background(), &api.Mutation{
			CommitNow: true,
			SetNquads: bb.Bytes(),
		})
	}

	fmt.Println("building indexes in background")
	if err := dg.Alter(context.Background(), &api.Operation{
		Schema: "balance: [uid] @reverse .",
	}); err != nil {
		log.Fatalf("error in adding indexes :: %v\n", err)
	}

	numEdges := int64(total)
	updated := sync.Map{}
	mutateUID := func(uid int) {
		switch uid % 4 {
		case 0:
			if _, err := dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				SetNquads: []byte(fmt.Sprintf(`<%v> <balance> <%v> .`, uid-2, uid)),
			}); err != nil {
				log.Fatalf("error in mutation :: %v\n", err)
			}
			updated.Store(uid, nil)
		case 1:
			v := atomic.AddInt64(&numEdges, 1)
			if _, err := dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				SetNquads: []byte(fmt.Sprintf(`<%v> <balance> <%v> .`, v-1, v)),
			}); err != nil {
				log.Fatalf("error in insertion :: %v\n", err)
			}
		case 2:
			if _, err := dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				DelNquads: []byte(fmt.Sprintf(`<%v> <balance> <%v> .`, uid-1, uid)),
			}); err != nil {
				log.Fatalf("error in deletion :: %v\n", err)
			}
			updated.Store(uid, nil)
		case 3:
			if _, err := dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				SetNquads: []byte(fmt.Sprintf(`<%v> <balance> <%v> .
											  <%v> <balance> <%v> .`,
					uid+1, uid, uid-2, uid)),
			}); err != nil {
				log.Fatalf("error in mutation :: %v\n", err)
			}
			updated.Store(uid, nil)
		}
	}

	var sig chan os.Signal
	sig = make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	// perform mutations until ctrl+c
	var swg sync.WaitGroup
	quit := make(chan struct{})
	runLoop := func() {
		defer swg.Done()
		for {
			select {
			case <-quit:
				return
			default:
				mutateUID(rand.Intn(total) + 1)
			}
		}
	}
	swg.Add(1)
	go runLoop()
	// go runLoop()
	<-sig
	close(quit)
	swg.Wait()
	fmt.Println("mutations done")

	// check values now
	checkUID := func(i int) error {
		q := fmt.Sprintf(`{ q(func: uid(%v)) { ~balance { uid }}}`, i)
		resp, err := dg.NewReadOnlyTxn().Query(context.Background(), q)
		if err != nil {
			log.Fatalf("error in query :: %v\n", err)
		}
		var data struct {
			Q []struct {
				Balance []struct {
					UID string
				} `json:"~balance"`
			}
		}
		if err := json.Unmarshal(resp.Json, &data); err != nil {
			log.Fatalf("error in json.Unmarshal :: %v", err)
		}

		_, ok := updated.Load(i)
		switch {
		case !ok || i > total || i%4 == 1:
			if len(data.Q) != 1 || len(data.Q[0].Balance) != 1 {
				return fmt.Errorf("length not equal, no mod, got: %+v", data)
			}
			v1, err := strconv.ParseInt(data.Q[0].Balance[0].UID, 0, 64)
			if err != nil {
				return err
			}
			if int(v1) != i-1 {
				return fmt.Errorf("value not equal, got: %+v", data)
			}
		case i%4 == 0:
			if len(data.Q) != 1 || len(data.Q[0].Balance) != 2 {
				return fmt.Errorf("length not equal, got: %+v", data)
			}
			v1, err := strconv.ParseInt(data.Q[0].Balance[0].UID, 0, 64)
			if err != nil {
				return err
			}
			v2, err := strconv.ParseInt(data.Q[0].Balance[1].UID, 0, 64)
			if err != nil {
				return err
			}
			l := []int{int(v1), int(v2)}
			sort.Ints(l)
			if l[0] != i-2 || l[1] != i-1 {
				return fmt.Errorf("value not equal, got: %+v", data)
			}
		case i%4 == 2:
			if len(data.Q) != 0 {
				return fmt.Errorf("length not equal, del, got: %+v", data)
			}
		case i%4 == 3:
			if len(data.Q) != 1 || len(data.Q[0].Balance) != 3 {
				return fmt.Errorf("length not equal, got: %+v", data)
			}
			v1, err := strconv.ParseInt(data.Q[0].Balance[0].UID, 0, 64)
			if err != nil {
				return err
			}
			v2, err := strconv.ParseInt(data.Q[0].Balance[1].UID, 0, 64)
			if err != nil {
				return err
			}
			v3, err := strconv.ParseInt(data.Q[0].Balance[2].UID, 0, 64)
			if err != nil {
				return err
			}
			l := []int{int(v1), int(v2), int(v3)}
			sort.Ints(l)
			if l[0] != i-2 || l[1] != i-1 || l[2] != i+1 {
				return fmt.Errorf("value not equal, got: %+v", data)
			}
		}

		return nil
	}

	type pair struct {
		uid int
		err string
	}
	ch := make(chan pair, numEdges)

	signal.Reset(os.Interrupt)
	fmt.Println("starting to query")
	var wg sync.WaitGroup
	var count uint64
	for i := 2; i <= int(numEdges); i += 100 {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			for k := j; k < j+100 && k < int(numEdges); k++ {
				if err := checkUID(k); err != nil {
					ch <- pair{k, err.Error()}
				}
				atomic.AddUint64(&count, 1)
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			time.Sleep(2 * time.Second)
			cur := atomic.LoadUint64(&count)
			fmt.Printf("%v/%v done\n", cur, numEdges)
			if cur+1 == uint64(numEdges) {
				break
			}
		}
	}()
	wg.Wait()

	close(ch)
	for p := range ch {
		t.Errorf("failed for %v, :: %v\n", p.uid, p.err)
	}
}
