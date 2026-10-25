// Package main serves the fixture's dev databases and its migration lint
// through one independent process: the dev database services of cmd/devdb
// and the rules of cmd/lint. A host replays and lints a migration directory or
// a plan on a dev database only this provider can clean.
package main

import (
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	"example.com/ptah-fixture-provider/devdb"
	"example.com/ptah-fixture-provider/statements"
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
	description.SemanticIdentity += "-lint"
	description.Features = append(description.Features, provider.LintFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.LintFeature)
	engine.LintRules = backend.LintRules()
	err := runtime.Serve(runtime.Server{
		Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, Transactions: owner, History: owner, Locks: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner,
		DevProvisioner: owner, DevCleaner: owner, ReplayGuard: owner, Statements: &statements.Service{Sessions: owner}, Linter: backend.Linter{},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
