// Package main serves the fixture with migration splits; see
// example.com/ptah-fixture-provider/migrationunits for the toy grammar and
// the environment that selects its faults.
package main

import (
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/migrationunits"
	"example.com/ptah-fixture-provider/statements"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	owner, err := migrationunits.FromEnvironment()
	if err == nil {
		err = runtime.Serve(runtime.Server{
			Description: migrationunits.Description(), Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, Transactions: owner,
			History: owner, Locks: owner, Metadata: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner,
			Statements: &statements.Service{Sessions: owner}, MigrationSplits: owner,
		})
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
