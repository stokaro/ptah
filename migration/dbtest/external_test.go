package dbtest_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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

// TestRunTest_AnExternalStepInCleanupIsRefusedWithoutAuthorization is the hole
// the first version of this guard left open.
//
// A case holds its steps in two slices, and the guard walked one. Measured on
// the shipped code: with AllowExternalCommands false, a program named in
// Cleanup RAN -- the run returned no error and the program's side effect was
// present on disk afterwards. The authorization promised something it did not
// deliver, and a teardown is not a lesser place to run a program from.
//
// The assertion is on the side effect rather than only on the error, because an
// error alone does not say the program was never started.
func TestRunTest_AnExternalStepInCleanupIsRefusedWithoutAuthorization(t *testing.T) {
	c := qt.New(t)

	marker := filepath.Join(t.TempDir(), "ran")

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{{
			Name:  "teardown runs a program",
			Steps: []dbtest.Step{{Exec: "SELECT 1"}},
			Cleanup: []dbtest.Step{{External: &dbtest.ExternalStep{
				Program: []string{"/usr/bin/touch", marker},
			}}},
		}},
	})

	c.Assert(err, qt.ErrorIs, dbtest.ErrExternalNotAuthorized)
	c.Assert(report, qt.IsNil)

	// The refusal names which half of the case the step is in: an author told
	// only "step 1" would go looking at the body.
	c.Assert(err.Error(), qt.Contains, "cleanup step 1")

	_, statErr := os.Stat(marker)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue,
		qt.Commentf("the program left a marker, so it ran"))
}
