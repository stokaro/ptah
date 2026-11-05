// Package main serves the fixture engine with a SQL scanner of its own
// (contract 25.26, sql.scan) and a query dialect that names a table by one
// path in backticks. Its text rules are not the shared lexer's: both quote
// styles delimit strings, @@...@@ is a string in which @@@@ stands for @@,
// and a backticked identifier holds a path such as `shop/items`. It is
// transport evidence for ScanSQL, not an engine grammar.
//
// The engine is not fixture, whose query dialect quotes with double quotes:
// the host keeps one spelling per engine for the life of a process.
package main

import (
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

// engine is the engine the program serves and the URL scheme of its
// addresses.
const engine = "fixture-scan"

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host explicitly selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	owner := &scanningProvider{Provider: &backend.Provider{Engine: engine}}
	description := backend.Description()
	description.SemanticIdentity = "fixture-sql-scan-1"
	description.Features = append(description.Features, provider.ScanSQLFeature)
	served := description.Engines[0]
	served.Id = engine
	served.Features = append(served.Features, provider.ScanSQLFeature)
	served.QueryDialect = &protocol.QueryDialect{Placeholder: "question", Quoting: &protocol.QueryQuoting{Style: "backtick", Bare: "never", Paths: true}}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, SQLScanner: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type scanningProvider struct{ *backend.Provider }
