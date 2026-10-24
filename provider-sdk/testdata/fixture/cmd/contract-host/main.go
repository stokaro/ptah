// Package main is a host that uses only the API of contract 25.0. The SDK's
// process tests build it against provider-sdk v0.2.0, so it stands in for a
// host of an older contract minor: they start a provider built from the SDK in
// the tree with it and check that every exchange still works.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

// run starts the provider at the executable path and prints what it saw, one
// fact a line: the features offered, then, for a session address, the
// outcome of a batch and the tables inspected after it, or, for lint, the
// facts a split judged.
func run(executable, mode, address string) error {
	binary, err := os.ReadFile(executable) // #nosec G703 -- the test harness explicitly selects the executable to checksum and start
	if err != nil {
		return err
	}
	checksum := sha256.Sum256(binary)
	ctx := context.Background()
	client, err := runtime.Start(ctx, runtime.Options{Executable: executable, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture"})
	if err != nil {
		return err
	}
	defer client.Close()
	features := client.Description().GetFeatures()
	for _, engine := range client.Description().GetEngines() {
		features = append(features, engine.GetFeatures()...)
	}
	slices.Sort(features)
	fmt.Println("features=" + strings.Join(slices.Compact(features), ","))
	if mode == "lint" {
		return lint(ctx, client)
	}
	return execute(ctx, client, address)
}

func execute(ctx context.Context, client *runtime.Client, address string) error {
	session, err := client.Open(ctx, &protocol.OpenRequest{Engine: "fixture", Address: address})
	if err != nil {
		return err
	}
	defer func() { _ = client.CloseSession(ctx, session.GetId()) }()
	result, err := client.Execute(ctx, &protocol.ExecuteRequest{SessionId: session.GetId(), RequestId: "contract", Units: []*protocol.Unit{
		{ChangeId: "items", Body: &protocol.Unit_Sql{Sql: `CREATE TABLE "items" ("id" INTEGER NOT NULL);`}},
	}})
	if err != nil {
		return err
	}
	fmt.Println("outcome=" + result.GetOutcome().String())
	schema, err := client.Inspect(ctx, &protocol.InspectRequest{SessionId: session.GetId()})
	if err != nil {
		return err
	}
	for _, table := range schema.GetTables() {
		fmt.Println("table=" + table.GetIdentity().GetName())
	}
	return nil
}

func lint(ctx context.Context, client *runtime.Client) error {
	split, err := client.SplitLintStatements(ctx, &protocol.SplitLintRequest{Engine: "fixture",
		Sources: []*protocol.LintSource{{Name: "1.sql", Sql: "CREATE TABLE a (id INTEGER);\nDROP TABLE a;", Up: true}}})
	if err != nil {
		return err
	}
	for _, fact := range split.GetJudged() {
		fmt.Println("judged=" + fact.String())
	}
	fmt.Printf("statements=%d\n", len(split.GetFiles()[0].GetStatements()))
	return nil
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "expected executable, mode and address")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2], os.Args[3]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
