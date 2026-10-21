// Package main serves the history-transaction independent fixture.
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

	owner := &backend.Provider{}
	description := backend.Description()
	description.SemanticIdentity = "fixture-data-history-1"
	description.Features = append(description.Features, provider.TransactionsFeature, provider.HistoryFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.TransactionsFeature, provider.HistoryFeature)
	if err := runtime.Serve(runtime.Server{
		Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, Transactions: owner, History: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
