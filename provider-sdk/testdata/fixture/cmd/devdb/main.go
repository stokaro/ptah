// Package main serves the fixture's dev databases through an independent
// process: provisioning, cleanup, the replay guard, realms and statement
// timeouts. FIXTURE_DEV_ENGINE=bare serves an engine that declares none of
// the cleanup, the guard and realms, which the provider still declares, and
// any other value serves the engine under that name, as the provider
// FIXTURE_DEV_PROVIDER_ID names when it is set. FIXTURE_LINKED_LINE=true
// declares the engine as one linked into the provider, which certifies its
// line by the version it links.
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
	owner := &devdb.Provider{Dir: dir}
	description := devdb.Description()
	switch engine := os.Getenv("FIXTURE_DEV_ENGINE"); engine {
	case "":
	case "bare":
		description = devdb.BareDescription()
	default:
		// The engine serves under the name of an engine Ptah builds in, as
		// a provider that replaces a built-in engine does, with the identity
		// of the provider the catalog lists for it.
		owner.Engine = engine
		description.SemanticIdentity += "-" + engine
		description.Engines[0].Id = engine
		if id := os.Getenv("FIXTURE_DEV_PROVIDER_ID"); id != "" {
			description.ProviderId = id
		}
	}
	if os.Getenv("FIXTURE_LINKED_LINE") == "true" {
		description.Features = append(description.Features, provider.LinkedReleaseLinesFeature)
		engine := description.Engines[0]
		engine.Features = append(engine.Features, provider.LinkedReleaseLinesFeature)
		engine.ReleaseLines = []*protocol.ReleaseLine{
			{Line: "3.0", Support: protocol.ReleaseLine_CERTIFIED, Linked: "3.0.1", Note: "The fixture's tests run the SQLite it links"},
			{Line: "2.0", Support: protocol.ReleaseLine_BEST_EFFORT, Unprobed: "the fixture links 3.0"},
		}
	}
	err := runtime.Serve(runtime.Server{
		Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, Transactions: owner, History: owner, Locks: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner,
		DevProvisioner: owner, DevCleaner: owner, ReplayGuard: owner, Statements: &statements.Service{Sessions: owner},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
