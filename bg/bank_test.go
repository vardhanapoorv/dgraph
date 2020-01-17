package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/dgo/v2/protos/api"
	"github.com/dgraph-io/dgraph/testutil"
)

func TestIndexInBG(t *testing.T) {
	total := 1000000
	ib := total
	numAccts := total
	mod := make(map[int]int, numAccts)
	var lock sync.Mutex

	dg, err := testutil.DgraphClient(testutil.SockAddr)
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
	for i := 1; i <= numAccts; {
		bb := &bytes.Buffer{}
		for j := 0; j < 10000; j++ {
			mod[i] = ib
			_, err := bb.WriteString(fmt.Sprintf("<%v> <balance> \"%v\" .\n", i, ib))
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

	// perform mutations until ctrl+c
	mutateUID := func(uid int) {
		nb := rand.Intn(ib)
		switch uid % 3 {
		case 0:
			if _, err := dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				SetNquads: []byte(fmt.Sprintf(`<%v> <balance> "%v" .`, uid, nb)),
			}); err != nil {
				log.Fatalf("error in mutation :: %v\n", err)
			}
		case 1:
			if _, err := dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				DelNquads: []byte(fmt.Sprintf(`<%v> <balance> * .`, uid)),
			}); err != nil {
				log.Fatalf("error in deletion :: %v\n", err)
			}
			nb = -1
		case 2:
			numAccts++
			if _, err := dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				SetNquads: []byte(fmt.Sprintf(`<%v> <balance> "%v" .`, numAccts, ib)),
			}); err != nil {
				log.Fatalf("error in insertion :: %v\n", err)
			}
		}

		lock.Lock()
		mod[uid] = nb
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
				mutateUID(rand.Intn(total) + 1)
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
	swg.Add(3)
	go runLoop()
	go runLoop()
	go printStats()
	<-sig
	close(quit)
	swg.Wait()
	signal.Reset(os.Interrupt)
	fmt.Println("mutations done")

	// compute reverse index
	rmod := make(map[int][]int)
	for uid, b := range mod {
		if aa, ok := rmod[b]; ok {
			rmod[b] = append(aa, uid)
		} else {
			rmod[b] = []int{b}
		}
	}
	for _, aa := range rmod {
		sort.Ints(aa)
	}

	// check values now
	checkBalance := func(b int, uids []int) error {
		q := fmt.Sprintf(`{ q(func: eq(balance, "%v")) {uid}}`, b)
		resp, err := dg.NewTxn().Query(context.Background(), q)
		if err != nil {
			log.Fatalf("error in query :: %v\n", err)
		}
		var data struct {
			Q []struct {
				UID string
			}
		}
		if err := json.Unmarshal(resp.Json, &data); err != nil {
			log.Fatalf("error in json.Unmarshal :: %v", err)
		}

		e := make([]int, len(data.Q))
		for i, ui := range data.Q {
			v, err := strconv.ParseInt(ui.UID, 0, 64)
			if err != nil {
				return err
			}
			e[i] = int(v)
		}
		sort.Ints(e)

		if len(e) != len(uids) {
			return fmt.Errorf("length not equal :: exp: %v, actual %v", e, uids)
		}
		for i := range uids {
			if uids[i] != e[i] {
				return fmt.Errorf("value not equal :: exp: %v, actual %v", e, uids)
			}
		}

		return nil
	}

	type pair struct {
		uid int
		err string
	}
	ch := make(chan pair, numAccts)

	fmt.Println("starting to query")
	var wg sync.WaitGroup
	var count uint64
	for b, uids := range rmod {
		wg.Add(1)
		go func(ba int, luid []int) {
			defer wg.Done()
			if err := checkBalance(ba, luid); err != nil {
				ch <- pair{ba, err.Error()}
			}
			atomic.AddUint64(&count, 1)
		}(b, uids)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			time.Sleep(2 * time.Second)
			cur := atomic.LoadUint64(&count)
			fmt.Printf("%v/%v done\n", cur, numAccts)
			if cur+1 == uint64(numAccts) {
				break
			}
		}
	}()
	wg.Wait()

	close(ch)
	for p := range ch {
		t.Logf("failed for %v, :: %v\n", p.uid, p.err)
	}
}

func TestReverseIndexInBG(t *testing.T) {
	total := 1000000

	dg, err := testutil.DgraphClient(testutil.SockAddr)
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
	mod := sync.Map{}
	mutateUID := func(uid int) {
		switch uid % 4 {
		case 0:
			if _, err := dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				SetNquads: []byte(fmt.Sprintf(`<%v> <balance> <%v> .`, uid-2, uid)),
			}); err != nil {
				log.Fatalf("error in mutation :: %v\n", err)
			}
			mod.Store(uid, nil)
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
			mod.Store(uid, nil)
		case 3:
			if _, err := dg.NewTxn().Mutate(context.Background(), &api.Mutation{
				CommitNow: true,
				SetNquads: []byte(fmt.Sprintf(`<%v> <balance> <%v> .
											  <%v> <balance> <%v> .`,
					uid+1, uid, uid-2, uid)),
			}); err != nil {
				log.Fatalf("error in mutation :: %v\n", err)
			}
			mod.Store(uid, nil)
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
		resp, err := dg.NewTxn().Query(context.Background(), q)
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

		_, ok := mod.Load(i)
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
				return fmt.Errorf("length not equal, got: %+v", data)
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
			if cur+2 == uint64(numEdges) {
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
