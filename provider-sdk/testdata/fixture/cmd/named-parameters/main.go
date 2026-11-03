// Package main serves the fixture's operator statements for engine
// fixture-named, whose query dialect declares the dollar-named placeholder
// style: the query builder writes its parameters $p1, $p2 and so on, and the
// provider binds the Nth argument of a statement to $pN by name, as the YDB
// provider does. SQLite takes a parameter of that spelling, so a statement the
// builder renders runs here as it runs on YDB.
package main

import (
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	"example.com/ptah-fixture-provider/statements"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

// engine is the engine the program serves and the URL scheme of its
// addresses. It is not fixture, whose query dialect binds by position: the
// query builder keeps one spelling per engine for the life of a process.
const engine = "fixture-named"

func main() {
	fixture := backend.Description()
	served := fixture.Engines[0]
	description := &protocol.Description{Revision: fixture.Revision, ProviderId: fixture.ProviderId, Version: fixture.Version,
		SemanticIdentity: fixture.SemanticIdentity + "-named-parameters", Features: append(fixture.Features, statements.Features()...),
		Engines: []*protocol.Engine{{Id: engine, DefaultSchema: served.DefaultSchema, Features: append(served.Features, statements.Features()...),
			Identifiers: served.Identifiers, QueryDialect: &protocol.QueryDialect{Placeholder: "dollar-named", Quoting: &protocol.QueryQuoting{Style: "double"}}}}}
	owner := &backend.Provider{Engine: engine}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner,
		Statements: &statements.Service{Sessions: owner, ByName: true}})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
