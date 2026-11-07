// Package main serves the fixture's dev databases with namespaces on the
// common-object wire of contract 25.29: a read lists the schemas of its
// scope, and the executor creates and removes them.
//
// SQLite has no schemas, so the program keeps the schemas it creates in a file
// beside the database, and stores a table of schema app as the SQLite table
// "app.t", which its reads report as table t of schema app. Its executor
// refuses a table of a schema the file does not hold, and the removal of a
// schema that still holds a table, as a server with schemas does.
//
// FIXTURE_NAMESPACES selects how the engine takes part: "off" lists no
// namespace feature, "uninspected" lists it and reads every schema as
// uninspected, and any other value reads them completely.
package main

import (
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/devdb"
	"example.com/ptah-fixture-provider/statements"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
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
	dir := os.Getenv("FIXTURE_DEV_DIR")
	if dir == "" {
		dir = os.TempDir()
	}
	description := devdb.Description()
	description.SemanticIdentity += "-namespaces"
	mode := os.Getenv("FIXTURE_NAMESPACES")
	if mode != "off" {
		added := []string{provider.CommonObjectsFeature, provider.CommonNamespacesFeature}
		description.Features = append(description.Features, added...)
		engine := description.Engines[0]
		engine.Features = append(engine.Features, added...)
		engine.CommonObjects = &protocol.CommonObjectSupport{Kinds: []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_SCHEMA}}
	}
	owner := &namespaces{Provider: &devdb.Provider{Dir: dir}, carried: mode != "off", uninspected: mode == "uninspected"}
	server := runtime.Server{
		Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, Transactions: owner, History: owner, Locks: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner,
		DevProvisioner: owner, DevCleaner: owner, ReplayGuard: owner, Statements: &statements.Service{Sessions: owner.Provider},
	}
	if mode != "off" {
		server.CommonObjects = owner
	}
	if err := runtime.Serve(server); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
