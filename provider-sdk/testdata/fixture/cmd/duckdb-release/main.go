// Package main serves the fixture under the identity the official DuckDB
// provider declares when it is installed with `ptah providers install duckdb`:
// provider github.com/stokaro/ptah-duckdb-plugin, engine duckdb, as the
// catalog lists them. DuckDB has no built-in implementation, so a host serves
// a duckdb:// address or the duckdb dialect from the provider the user store
// holds, with no flag: the shape every official engine takes once its
// provider serves it, YDB among them.
package main

import (
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	fixture := backend.Description()
	engine := fixture.Engines[0]
	// The schema lock is what schema apply asks for before it writes.
	description := &protocol.Description{Revision: fixture.Revision, ProviderId: "github.com/stokaro/ptah-duckdb-plugin", Version: fixture.Version,
		SemanticIdentity: fixture.SemanticIdentity + "-duckdb-release", Features: append(fixture.Features, provider.LocksFeature),
		Engines: []*protocol.Engine{{Id: "duckdb", DefaultSchema: engine.DefaultSchema, Features: append(engine.Features, provider.LocksFeature),
			Identifiers: engine.Identifiers}}}
	owner := &backend.Provider{Engine: "duckdb"}
	err := runtime.Serve(runtime.Server{Description: description, Locks: owner, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
