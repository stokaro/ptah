// Package main serves the fixture's operator statements with typed arguments:
// the provider lists sessions.typed-arguments, so a host that lists it too
// sends each argument with the type its caller bound it at. SQLite binds the
// value and drops the type, and binds a list as the JSON array of its
// elements.
package main

import (
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	"example.com/ptah-fixture-provider/statements"

	"ptah.run/provider-sdk/provider"
	"ptah.run/provider-sdk/runtime"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-typed-arguments"
	description.Features = append(description.Features, statements.Features()...)
	description.Features = append(description.Features, provider.TypedArgumentsFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, statements.Features()...)
	owner := &backend.Provider{}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner,
		Statements: &statements.Service{Sessions: owner}})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
