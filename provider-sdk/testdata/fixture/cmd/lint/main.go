// Package main exercises provider-owned migration lint through an independent process.
package main

import (
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	// Session locks, transactions and history let a command that applies or
	// plans under a lock, such as `schema apply`, reach the lint pass.
	description := backend.Description()
	description.SemanticIdentity += "-lint"
	description.Features = append(description.Features, provider.LintFeature, provider.TransactionsFeature, provider.HistoryFeature, provider.LocksFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.LintFeature, provider.TransactionsFeature, provider.HistoryFeature, provider.LocksFeature)
	engine.LintRules = backend.LintRules()
	owner := &backend.TransactionalProvider{}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		Transactions: owner, History: owner, Locks: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, Linter: backend.Linter{}})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
