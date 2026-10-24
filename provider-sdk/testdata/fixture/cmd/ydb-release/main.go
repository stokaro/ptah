// Package main serves the fixture under the identity the official YDB provider
// declares when it is installed with `ptah providers install ydb`: provider
// github.com/stokaro/ptah-ydb-plugin, engine ydb with the alias ydbs, as the
// catalog lists them. A host that selects it from the user store checks those
// declarations against the catalog, so the test that installs it from a fake
// release reaches a process that answers the handshake.
package main

import (
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	fixture := backend.Description()
	engine := fixture.Engines[0]
	description := &protocol.Description{Revision: fixture.Revision, ProviderId: "github.com/stokaro/ptah-ydb-plugin", Version: fixture.Version,
		SemanticIdentity: fixture.SemanticIdentity + "-ydb-release", Features: fixture.Features,
		Engines: []*protocol.Engine{{Id: "ydb", Aliases: []string{"ydbs"}, DefaultSchema: engine.DefaultSchema, Features: engine.Features,
			Identifiers: engine.Identifiers}}}
	owner := &backend.Provider{}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
