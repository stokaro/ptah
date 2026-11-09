// Package main runs the fixture with the SQL statement facts of contract
// 25.33: the fixture engine reads its SQL documents with the shared grammar
// (the FRAGMENTS mode) and states that its server does nothing for DROP VIEW
// IF EXISTS of a name another kind of object holds, as the Spanner emulator
// does. With FIXTURE_STATEMENT_FACTS=none it lists the feature and states
// every fact false, as an engine whose server refuses the statement does,
// such as DuckDB.
package main

import (
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	"ptah.run/provider-sdk/runtime"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, fmt.Appendf(nil, "%d", os.Getpid()), 0o600); err != nil { // #nosec G703 -- the host explicitly selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	owner := &backend.Provider{}
	err := runtime.Serve(runtime.Server{Description: description(), Locks: owner, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// description is the base fixture in the FRAGMENTS mode, stating the
// statement facts FIXTURE_STATEMENT_FACTS selects.
func description() *protocol.Description {
	description := backend.Description()
	description.SemanticIdentity += "-sql-statements"
	description.Features = append(description.Features, provider.SQLStatementFactsFeature, provider.LocksFeature)
	engine := description.GetEngines()[0]
	engine.Features = append(engine.Features, provider.SQLStatementFactsFeature, provider.LocksFeature)
	engine.SqlSchema = &protocol.SQLSchemaDeclaration{Mode: protocol.SQLSchemaDeclaration_FRAGMENTS,
		DropViewIfExistsSkipsOtherKinds: os.Getenv("FIXTURE_STATEMENT_FACTS") != "none"}
	return description
}
