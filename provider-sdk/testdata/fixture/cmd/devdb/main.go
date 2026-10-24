// Package main serves the fixture's dev databases through an independent
// process: provisioning, cleanup, the replay guard, realms and statement
// timeouts. FIXTURE_DEV_ENGINE=bare serves an engine that declares none of
// the cleanup, the guard and realms, which the provider still declares.
package main

import (
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/devdb"
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
	if os.Getenv("FIXTURE_DEV_ENGINE") == "bare" {
		description = devdb.BareDescription()
	}
	err := runtime.Serve(runtime.Server{
		Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, Transactions: owner, History: owner, Locks: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner,
		DevProvisioner: owner, DevCleaner: owner, ReplayGuard: owner,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
