//go:build !windows

package dbtest_test

// The overlap assertion needs a barrier the cases can block on, and the
// barrier here is a POSIX shell script. The file is constrained by a build tag
// rather than by a test asserting which platform it is not on -- that shape
// fails on the platform it was meant to excuse.
//
// The Windows contour therefore does not assert that parallel cases overlap.
// Isolation, report order and completeness are asserted on every platform in
// parallel_test.go; what is missing there is the schedule, which is
// platform-independent Go concurrency rather than anything the operating
// system decides. Saying so is better than a Windows fixture that would only
// re-measure the barrier.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/dbtest"
)

// TestRunTest_ParallelCasesActuallyOverlap is the assertion the rest of this
// file cannot make.
//
// Every other test here passes just as well when the cases run one after
// another: isolation, report order and completeness are all properties of the
// result rather than of the schedule. Running them serially is a mutation none
// of them kills, so the concurrency itself needs an assertion of its own.
//
// It is a barrier rather than a stopwatch. Each case runs a program that
// announces itself and then waits until every participant has announced, so a
// serial run blocks on the first case until its own timeout fires and the case
// fails. There is no threshold to tune and nothing to be slow enough to break:
// a loaded machine takes longer to pass, not more likely to fail.
//
// Parallelism is set explicitly for the same reason -- the default reads
// GOMAXPROCS, and a barrier of three would deadlock on a two-processor runner
// no matter how correct the code was.
func TestRunTest_ParallelCasesActuallyOverlap(t *testing.T) {
	c := qt.New(t)

	const participants = 3

	dir := t.TempDir()
	arrivals := filepath.Join(dir, "arrivals")
	c.Assert(os.Mkdir(arrivals, 0o700), qt.IsNil)

	barrier := filepath.Join(dir, "barrier.sh")
	script := "#!/bin/sh\n" +
		"set -e\n" +
		": > \"$1/$2\"\n" +
		"waited=0\n" +
		"while [ \"$(ls \"$1\" | wc -l)\" -lt \"$3\" ]; do\n" +
		"  waited=$((waited + 1))\n" +
		"  [ \"$waited\" -gt 600 ] && exit 1\n" +
		"  sleep 0.05\n" +
		"done\n" +
		"echo arrived\n"
	// #nosec G306 -- the fixture is a program the step has to execute, so it
	// needs the execute bit; it lives in this test's own temporary directory.
	c.Assert(os.WriteFile(barrier, []byte(script), 0o700), qt.IsNil)

	cases := make([]dbtest.Case, 0, participants)
	for i := range participants {
		cases = append(cases, dbtest.Case{
			Name:     fmt.Sprintf("waiter-%d", i),
			Parallel: true,
			Steps: []dbtest.Step{{
				Name: "wait for the others",
				External: &dbtest.ExternalStep{
					Program: []string{barrier, arrivals, fmt.Sprint(i), fmt.Sprint(participants)},
					Output:  new("arrived"),
				},
			}},
		})
	}

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		AllowExternalCommands: true,
		Parallelism:           participants,
		ExternalTimeout:       60 * time.Second,
		Cases:                 cases,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse, qt.Commentf("report: %s", report.Text()))
	c.Assert(report.Cases, qt.HasLen, participants)
}
