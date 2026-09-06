//go:build !windows

package dbtest_test

// The external step executes a program, and a program is a platform's own. The
// fixtures here name POSIX ones, so the file is constrained by a build tag
// rather than by each test asserting which platform it is not on -- an
// assertion of that shape fails on the platform it was meant to excuse, which
// is exactly what it did.
//
// The Windows half is external_windows_test.go. Two properties are covered
// there and two are not: a `cmd /C` fixture is itself a shell, so the no-shell
// guarantee has no meaningful Windows fixture, and the timeout fixture is a
// POSIX sleep. Both behaviors are platform-independent Go, and saying so is
// better than a Windows test that asserts a tautology.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/dbtest"
)

// TestRunTest_AnAuthorizedExternalStepRuns is the control on the refusal.
//
// Without it, deleting the feature entirely would read as a passing security
// test: every case containing an external step would be refused, and the
// refusal test alone cannot tell that apart from the step working.
func TestRunTest_AnAuthorizedExternalStepRuns(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		AllowExternalCommands: true,
		Cases: []dbtest.Case{{
			Name: "runs a program",
			Steps: []dbtest.Step{
				{Name: "echo", External: &dbtest.ExternalStep{
					Program: []string{"/bin/echo", "hello"},
					Output:  new("hello"),
				}},
			},
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse,
		qt.Commentf("detail: %s", report.Cases[0].Steps[0].Detail))
}

// TestRunTest_AnExternalStepRunsWithoutAShell is the property that makes the
// argument vector a security boundary rather than a spelling.
//
// The single argument holds shell metacharacters. Executed as argv it is one
// literal string; interpreted by a shell it would run `id` and the output would
// not match. A fixture whose argument had no metacharacters would pass under
// both implementations and establish nothing.
func TestRunTest_AnExternalStepRunsWithoutAShell(t *testing.T) {
	c := qt.New(t)

	const hostile = "a; id > /dev/null && echo pwned"

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		AllowExternalCommands: true,
		Cases: []dbtest.Case{{
			Name: "no shell",
			Steps: []dbtest.Step{
				{External: &dbtest.ExternalStep{
					Program: []string{"/bin/echo", hostile},
					Output:  new(hostile),
				}},
			},
		}},
	})

	c.Assert(err, qt.IsNil)
	// Equality with the whole literal is the discriminator, and it is exact for
	// a reason: through a shell, `echo` would receive only `a` before the
	// separator and the output would be one character, so an implementation
	// that built a command line fails this row rather than merely looking
	// different.
	c.Assert(report.Failed(), qt.IsFalse,
		qt.Commentf("detail: %s", report.Cases[0].Steps[0].Detail))
	c.Assert(report.Cases[0].Steps[0].Detail, qt.Contains, hostile)
}

// TestRunTest_AnExternalStepObeysItsTimeout keeps an unattended suite from
// being held open by a program that never exits.
//
// The bound is the runner's rather than the document's, so a test file cannot
// raise or remove it; this asserts that a short one actually fires.
func TestRunTest_AnExternalStepObeysItsTimeout(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		AllowExternalCommands: true,
		ExternalTimeout:       50 * time.Millisecond,
		Cases: []dbtest.Case{{
			Name: "hangs",
			Steps: []dbtest.Step{
				{Name: "sleep", External: &dbtest.ExternalStep{Program: []string{"/bin/sleep", "30"}}},
			},
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsTrue)
	c.Assert(report.Cases[0].Steps[0].Detail, qt.Contains, "failed")
}

// TestRunTest_ExternalStepFailures_FailurePath covers the outcomes a program
// can produce, each asserted on its detail rather than only on the verdict.
func TestRunTest_ExternalStepFailures_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		step       dbtest.ExternalStep
		wantDetail string
	}{
		{
			name:       "a non-zero exit fails the case",
			step:       dbtest.ExternalStep{Program: []string{"/bin/false"}},
			wantDetail: "failed",
		},
		{
			name:       "output that does not match",
			step:       dbtest.ExternalStep{Program: []string{"/bin/echo", "ada"}, Output: new("grace")},
			wantDetail: `expected external output "grace", got "ada"`,
		},
		{
			name:       "a pattern the output does not match",
			step:       dbtest.ExternalStep{Program: []string{"/bin/echo", "ada"}, Match: "^zz"},
			wantDetail: `expected external output "ada" to match "^zz"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
				AllowExternalCommands: true,
				Cases: []dbtest.Case{{
					Name:  test.name,
					Steps: []dbtest.Step{{External: &test.step}},
				}},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(report.Failed(), qt.IsTrue)
			c.Assert(report.Cases[0].Steps[0].Detail, qt.Contains, test.wantDetail)
		})
	}
}

// TestRunTest_AnExternalStepRunsInItsWorkingDirectory covers the one remaining
// attribute, with a fixture that could not pass by accident.
func TestRunTest_AnExternalStepRunsInItsWorkingDirectory(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "marker.txt"), []byte("here"), 0o600), qt.IsNil)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		AllowExternalCommands: true,
		Cases: []dbtest.Case{{
			Name: "working dir",
			Steps: []dbtest.Step{
				{External: &dbtest.ExternalStep{
					Program:    []string{"/bin/cat", "marker.txt"},
					WorkingDir: dir,
					Output:     new("here"),
				}},
			},
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse,
		qt.Commentf("detail: %s", report.Cases[0].Steps[0].Detail))
}
