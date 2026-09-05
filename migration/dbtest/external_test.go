package dbtest_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/dbtest"
)

// TestRunTest_AnExternalStepIsRefusedWithoutAuthorization is the security
// property, and it asserts two separate things.
//
// The run is refused, and it is refused as a LOAD failure rather than as a
// failing case: the returned error is non-nil and no report comes back. That
// distinction is what proves the refusal happened before the engine provisioned
// a database, applied a schema, and reached the step -- a refusal reported as a
// case result would mean everything before it had already run.
func TestRunTest_AnExternalStepIsRefusedWithoutAuthorization(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{
			{
				Name:  "ordinary",
				Steps: []dbtest.Step{{Exec: "SELECT 1"}},
			},
			{
				Name: "runs a program",
				Steps: []dbtest.Step{
					{External: &dbtest.ExternalStep{Program: []string{"/bin/echo", "hello"}}},
				},
			},
		},
	})

	c.Assert(err, qt.ErrorIs, dbtest.ErrExternalNotAuthorized)
	c.Assert(report, qt.IsNil)

	// The refusal names the case, the step, the program, and the way to
	// authorize it, because an operator reading it has to decide whether they
	// meant to run that program.
	c.Assert(err.Error(), qt.Contains, `test case "runs a program"`)
	c.Assert(err.Error(), qt.Contains, "/bin/echo")
	c.Assert(err.Error(), qt.Contains, "PTAH_ALLOW_EXTERNAL_TEST_COMMAND")
}

// TestRunTest_AnAuthorizedExternalStepRuns is the control on the refusal.
//
// Without it, deleting the feature entirely would read as a passing security
// test: every case containing an external step would be refused, and the
// refusal test alone cannot tell that apart from the step working.
func TestRunTest_AnAuthorizedExternalStepRuns(t *testing.T) {
	c := qt.New(t)
	c.Assert(runtime.GOOS, qt.Not(qt.Equals), "windows",
		qt.Commentf("this file's fixtures are POSIX; the Windows contour covers the step separately"))

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
	c.Assert(runtime.GOOS, qt.Not(qt.Equals), "windows",
		qt.Commentf("this file's fixtures are POSIX; the Windows contour covers the step separately"))

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
	c.Assert(runtime.GOOS, qt.Not(qt.Equals), "windows",
		qt.Commentf("this file's fixtures are POSIX; the Windows contour covers the step separately"))

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
			c.Assert(runtime.GOOS, qt.Not(qt.Equals), "windows",
				qt.Commentf("POSIX fixtures"))

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

// TestRunTest_AnInvalidExternalStepIsRefusedBeforeAnyDatabase keeps a
// malformed step a load error.
//
// The acceptance this covers is explicit: provision no database before an
// invalid external step has been rejected. A returned report would mean one had
// already been created.
func TestRunTest_AnInvalidExternalStepIsRefusedBeforeAnyDatabase(t *testing.T) {
	tests := []struct {
		name    string
		step    dbtest.ExternalStep
		wantErr string
	}{
		{
			name:    "no program",
			step:    dbtest.ExternalStep{Program: nil},
			wantErr: "(?s).*external requires a program to run.*",
		},
		{
			name:    "both expectations",
			step:    dbtest.ExternalStep{Program: []string{"/bin/echo"}, Output: new("x"), Match: "x"},
			wantErr: "(?s).*at most one of output or match.*",
		},
		{
			name:    "an invalid pattern",
			step:    dbtest.ExternalStep{Program: []string{"/bin/echo"}, Match: "([unclosed"},
			wantErr: "(?s).*not a valid regular expression.*",
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

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(report, qt.IsNil)
		})
	}
}

// TestRunTest_AnExternalStepRunsInItsWorkingDirectory covers the one remaining
// attribute, with a fixture that could not pass by accident.
func TestRunTest_AnExternalStepRunsInItsWorkingDirectory(t *testing.T) {
	c := qt.New(t)
	c.Assert(runtime.GOOS, qt.Not(qt.Equals), "windows",
		qt.Commentf("POSIX fixtures"))

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

// TestParseAtlasTestCases_ExternalBlock covers the translation, including the
// refusal that keeps an argument vector from becoming a command line.
func TestParseAtlasTestCases_ExternalBlock(t *testing.T) {
	const document = `
test "schema" "translated" {
  external {
    program     = ["/bin/echo", "hello"]
    working_dir = "/tmp"
    output      = "hello"
  }
}
`

	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases([]byte(document), "s.test.hcl", dbtest.AtlasTestKindSchema)

	c.Assert(err, qt.IsNil)
	c.Assert(cases[0].Steps, qt.HasLen, 1)
	c.Assert(cases[0].Steps[0].External, qt.IsNotNil)
	c.Assert(cases[0].Steps[0].External.Program, qt.DeepEquals, []string{"/bin/echo", "hello"})
	c.Assert(cases[0].Steps[0].External.WorkingDir, qt.Equals, "/tmp")
	c.Assert(*cases[0].Steps[0].External.Output, qt.Equals, "hello")
}

// TestParseAtlasTestCases_ExternalRefusals_FailurePath keeps the block failing
// closed.
//
// The string-instead-of-list row is the one that matters: accepting it and
// splitting on spaces is exactly how an argument vector silently becomes a
// command line, so it is refused by name rather than interpreted.
func TestParseAtlasTestCases_ExternalRefusals_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     string
	}{
		{
			name:     "program must be a list, not a command line",
			document: "test \"schema\" \"a\" {\n  external {\n    program = \"/bin/echo hello\"\n  }\n}\n",
			want:     ".*program must be a list of strings, got string.*",
		},
		{
			name:     "every element must be a string",
			document: "test \"schema\" \"a\" {\n  external {\n    program = [\"/bin/echo\", 7]\n  }\n}\n",
			want:     ".*every program element must be a string, got number.*",
		},
		{
			name:     "external requires a program",
			document: "test \"schema\" \"a\" {\n  external {\n    working_dir = \"/tmp\"\n  }\n}\n",
			want:     ".*`external` requires program.*",
		},
		{
			name:     "output and match together",
			document: "test \"schema\" \"a\" {\n  external {\n    program = [\"/bin/echo\"]\n    output  = \"x\"\n    match   = \"x\"\n  }\n}\n",
			want:     ".*`external` takes `output` or `match`, not both.*",
		},
		{
			name:     "a timeout is the runner's, not the document's",
			document: "test \"schema\" \"a\" {\n  external {\n    program = [\"/bin/echo\"]\n    timeout = \"5s\"\n  }\n}\n",
			want:     ".*`external` does not take \\[timeout\\].*",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := dbtest.ParseAtlasTestCases([]byte(test.document), "s.test.hcl", dbtest.AtlasTestKindSchema)

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}
