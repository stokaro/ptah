package migraterollout_test

import (
	"os"
	"testing"
)

// TestMain refuses to run this package's tests in a process started as a
// rollout's target run.
//
// A rollout runs each target with the binary it is running in, and under
// `go test` that binary is this test binary. Every test here expects a refusal
// before any target runs; one that stopped refusing would reach the target
// runs, each would run every test here again, and a single regression would
// start processes without limit. A process started with the arguments of a
// target run exits at once instead, which the run records as unknown.
func TestMain(m *testing.M) {
	os.Exit(runUnlessTargetRun(m, os.Args))
}

func runUnlessTargetRun(m *testing.M, args []string) int {
	if len(args) > 1 && args[1] == "migrations" {
		return 3
	}
	return m.Run()
}
