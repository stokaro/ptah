package root_test

import (
	"os"
	"testing"
)

// TestMain refuses to run this package's tests in a process started as a
// rollout's target run.
//
// The OCI census drives `ptah migrations rollout run` and `resume` at a closed
// registry, where binding the release fails before any target runs. A rollout
// runs each target with the binary it is running in, which under `go test` is
// this test binary, so a binding that stopped failing would run every test here
// once per target and start processes without limit. A process started with the
// arguments of a target run exits at once instead.
func TestMain(m *testing.M) {
	os.Exit(runUnlessTargetRun(m, os.Args))
}

func runUnlessTargetRun(m *testing.M, args []string) int {
	if len(args) > 1 && args[1] == "migrations" {
		return 3
	}
	return m.Run()
}
