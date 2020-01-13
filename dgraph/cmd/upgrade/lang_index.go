/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package upgrade

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"

	"github.com/dgraph-io/dgraph/types"
	"github.com/dgraph-io/dgraph/x"
	"github.com/spf13/cobra"
)

// LangIndex is the sub-command invoked when calling "dgraph lang index tool".
var (
	LangIndex x.SubCommand
	sch       schema
)

type predicate struct {
	Predicate  string   `json:"predicate,omitempty"`
	Type       string   `json:"type,omitempty"`
	Tokenizer  []string `json:"tokenizer,omitempty"`
	Count      bool     `json:"count,omitempty"`
	List       bool     `json:"list,omitempty"`
	Lang       bool     `json:"lang,omitempty"`
	Index      bool     `json:"index,omitempty"`
	Upsert     bool     `json:"upsert,omitempty"`
	Reverse    bool     `json:"reverse,omitempty"`
	NoConflict bool     `json:"noconflict,omitempty"`
	ValueType  types.TypeID
	Actual     string
}

type schema struct {
	Predicates []*predicate `json:"schema,omitempty"`
	preds      map[string]*predicate
}

func (l *schema) init() {
	l.preds = make(map[string]*predicate)
	for _, i := range l.Predicates {
		i.ValueType, _ = types.TypeForName(i.Type)
		l.preds[i.Predicate] = i
	}
}

func init() {
	LangIndex.Cmd = &cobra.Command{
		Use:   "rebuild",
		Short: "Reindexes language predicate",
		Run: func(cmd *cobra.Command, args []string) {
			run()
		},
	}

	flag := LangIndex.Cmd.Flags()
	flag.String("alpha", "localhost:9080", "Address of Dgraph Alpha.")
	flag.StringP("user", "u", "", "Username if login is required.")
	flag.StringP("password", "p", "", "Password of the user.")
	x.RegisterClientTLSFlags(flag)
}

func getSchema(ctx context.Context, dgraphClient *dgo.Dgraph) (*schema, error) {
	txn := dgraphClient.NewTxn()
	defer txn.Discard(ctx)

	res, err := txn.Query(ctx, "schema {}")
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(res.GetJson(), &sch)
	if err != nil {
		return nil, err
	}
	sch.init()
	return &sch, nil
}

func convert(sch *schema, removeLangIndex bool, requiredPredicates map[string]struct{}) string {
	schema := ""

	for _, v := range sch.preds {
		if strings.HasPrefix(v.Predicate, "dgraph.") {
			continue
		}

		if _, found := requiredPredicates[v.Predicate]; !found {
			continue
		}

		typ := v.Type
		if v.List {
			typ = fmt.Sprintf("[%s]", typ)
		}

		indexes := ""
		if !removeLangIndex && v.Index {
			indexes = fmt.Sprintf("@index(%s) ", strings.Join(v.Tokenizer, ","))
		}

		reverse := ""
		if v.Reverse {
			reverse = "@reverse "
		}

		count := ""
		if v.Count {
			count = "@count "
		}

		lang := ""
		if v.Lang {
			lang = "@lang "
		}

		noconflict := ""
		if v.NoConflict {
			noconflict = "@noconflict "
		}

		upsert := ""
		if v.Upsert {
			upsert = "@upsert "
		}

		schema += fmt.Sprintf("%s: %s %s%s%s%s%s%s.\n", v.Predicate, typ, indexes, reverse, noconflict, count, lang, upsert)
	}
	return schema
}

func run() {
	dg, closeFunc := x.GetDgraphClient(LangIndex.Conf, true)
	defer closeFunc()

	ctx := context.Background()
	sch, err := getSchema(ctx, dg)
	if err != nil {
		fmt.Printf("Error while loading schema from alpha %s\n", err)
		return
	}

	requiredPredicates := make(map[string]struct{})
	for k, v := range sch.preds {
		if v.Lang && v.Index {
			requiredPredicates[k] = struct{}{}
		}
	}

	if len(requiredPredicates) == 0 {
		return
	}

	actualSchema := convert(sch, false, requiredPredicates)
	fmt.Println("Actual Schema: ", actualSchema)
	updatedSchema := convert(sch, true, requiredPredicates)
	fmt.Println("Updated Schema: ", updatedSchema)

	op := &api.Operation{
		Schema: updatedSchema,
	}

	fmt.Println("Removing lang index.")
	err = dg.Alter(context.Background(), op)
	if err != nil {
		fmt.Printf("Error while reindexing lang predicate %s\n", err)
	}

	op = &api.Operation{
		Schema: actualSchema,
	}
	fmt.Println("Adding lang index.")
	err = dg.Alter(context.Background(), op)
	if err != nil {
		fmt.Printf("Error while reindexing lang predicate %s\n", err)
	}
}
