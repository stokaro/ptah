//go:build windows

package dbtest_test

// The Windows half of the external step's execution tests. The POSIX half is
// external_posix_test.go, and neither is a subset of the other.
//
// Two properties are covered here and two deliberately are not. The no-shell
// guarantee has no meaningful fixture on Windows: the ordinary way to run a
// trivial command is `cmd /C`, which IS a shell, so a test written around it
// would assert that a shell behaves like a shell. The timeout fixture is a
// POSIX sleep. Both behaviors are platform-independent Go rather than anything
// the operating system decides, and leaving them to the other half with a
// reason is better than a test that quietly asserts a tautology here.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/dbtest"
)

// TestRunTest_AnAuthorizedExternalStepRunsOnWindows is the control on the
// refusal, run where the refusal test also runs.
//
// Without a passing counterpart on this platform, deleting the feature would
// read as a passing security test here: every case containing an external step
// would be refused, and the refusal test alone cannot tell that apart from the
// step working.
func TestRunTest_AnAuthorizedExternalStepRunsOnWindows(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		AllowExternalCommands: true,
		Cases: []dbtest.Case{{
			Name: "runs a program",
			Steps: []dbtest.Step{
				{Name: "echo", External: &dbtest.ExternalStep{
					Program: []string{"cmd", "/C", "echo hello"},
					Output:  new("hello"),
				}},
			},
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse,
		qt.Commentf("detail: %s", report.Cases[0].Steps[0].Detail))
}

// TestRunTest_ExternalStepFailuresOnWindows_FailurePath covers the outcomes a
// program can produce, asserted on the detail rather than only on the verdict.
func TestRunTest_ExternalStepFailuresOnWindows_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		step       dbtest.ExternalStep
		wantDetail string
	}{
		{
			name:       "a non-zero exit fails the case",
			step:       dbtest.ExternalStep{Program: []string{"cmd", "/C", "exit 1"}},
			wantDetail: "failed",
		},
		{
			name: "output that does not match",
			step: dbtest.ExternalStep{
				Program: []string{"cmd", "/C", "echo ada"},
				Output:  new("grace"),
			},
			wantDetail: `expected external output "grace", got "ada"`,
		},
		{
			name: "a pattern the output does not match",
			step: dbtest.ExternalStep{
				Program: []string{"cmd", "/C", "echo ada"},
				Match:   "^zz",
			},
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

// TestRunTest_AnExternalStepRunsInItsWorkingDirectoryOnWindows covers the
// remaining attribute with a fixture that could not pass by accident.
func TestRunTest_AnExternalStepRunsInItsWorkingDirectoryOnWindows(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "marker.txt"), []byte("here"), 0o600), qt.IsNil)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		AllowExternalCommands: true,
		Cases: []dbtest.Case{{
			Name: "working dir",
			Steps: []dbtest.Step{
				{External: &dbtest.ExternalStep{
					Program:    []string{"cmd", "/C", "type marker.txt"},
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
