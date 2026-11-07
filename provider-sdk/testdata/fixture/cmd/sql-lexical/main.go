// Package main runs the fixture with the lexical rules of contract 25.34: the
// fixture engine reads its SQL documents with the shared grammar (the
// FRAGMENTS mode) and states, in the data fixture's query dialect, the rules
// of a PostgreSQL-family engine: escape strings, continued and Unicode
// strings and nested block comments. With FIXTURE_LEXICAL=none it declares
// the same query dialect without the rules, as a provider of contract 25.33
// does.
package main

import (
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	"example.com/ptah-fixture-provider/data"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, fmt.Appendf(nil, "%d", os.Getpid()), 0o600); err != nil { // #nosec G703 -- the host explicitly selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	owner := &backend.Provider{}
	err := runtime.Serve(runtime.Server{Description: description(), Locks: owner, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// description is the base fixture in the FRAGMENTS mode, with the lexical
// rules FIXTURE_LEXICAL selects.
func description() *protocol.Description {
	description := backend.Description()
	description.SemanticIdentity += "-sql-lexical"
	description.Features = append(description.Features, provider.LocksFeature)
	engine := description.GetEngines()[0]
	engine.Features = append(engine.Features, provider.LocksFeature)
	engine.SqlSchema = &protocol.SQLSchemaDeclaration{Mode: protocol.SQLSchemaDeclaration_FRAGMENTS}
	engine.QueryDialect = data.QueryDialect()
	if os.Getenv("FIXTURE_LEXICAL") != "none" {
		description.Features = append(description.Features, provider.QueryLexicalFeature)
		engine.Features = append(engine.Features, provider.QueryLexicalFeature)
		engine.QueryDialect.Lexical = &protocol.QueryLexical{EscapeStrings: true, ContinuedStrings: true, UnicodeStrings: true, NestedComments: true}
	}
	return description
}
