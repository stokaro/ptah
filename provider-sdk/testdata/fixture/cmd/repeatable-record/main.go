// Package main serves the native-migrations fixture with the record of
// repeatable migrations, so the host runs R__ files through the provider.
// FIXTURE_REPEATABLE_ENGINE=off keeps the feature on the provider and leaves it
// off the engine, which the host refuses as it refuses a provider without it.
package main

import (
	"fmt"
	"os"
	"slices"

	"example.com/ptah-fixture-provider/backend"
	"example.com/ptah-fixture-provider/repeatables"

	"ptah.run/provider-sdk/provider"
	"ptah.run/provider-sdk/runtime"
)

func main() {
	owner := &repeatables.Provider{}
	description := backend.Description()
	description.SemanticIdentity = "fixture-repeatable-record-1"
	features := []string{provider.TransactionsFeature, provider.HistoryFeature, provider.LocksFeature, provider.MetadataFeature}
	description.Features = append(description.Features, features...)
	description.Features = append(description.Features, provider.RepeatableRecordFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, features...)
	if os.Getenv("FIXTURE_REPEATABLE_ENGINE") != "off" {
		description.Engines[0].Features = append(description.Engines[0].Features, provider.RepeatableRecordFeature)
	}
	description.Engines[0].Features = slices.DeleteFunc(description.Engines[0].Features, func(feature string) bool {
		return feature == provider.CommonTableIdentityFeature
	})
	if err := runtime.Serve(runtime.Server{
		Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, Transactions: owner, History: owner,
		Locks: owner, Metadata: owner, Repeatables: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
