// Package main serves the independent fixture without common table semantics.
package main

import (
	"fmt"
	"os"
	"slices"
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

	owner := &backend.TransactionalProvider{}
	description := backend.Description()
	description.SemanticIdentity = "fixture-native-migrations-1"
	description.Features = append(description.Features, provider.TransactionsFeature, provider.HistoryFeature, provider.LocksFeature, provider.MetadataFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.TransactionsFeature, provider.HistoryFeature, provider.LocksFeature, provider.MetadataFeature)
	description.Engines[0].Features = slices.DeleteFunc(description.Engines[0].Features, func(feature string) bool {
		return feature == provider.CommonTableIdentityFeature
	})
	if err := runtime.Serve(runtime.Server{
		Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, Transactions: owner, History: owner, Locks: owner, Metadata: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
