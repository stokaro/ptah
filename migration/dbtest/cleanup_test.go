package dbtest_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/dbtest"
)

// TestRunTest_CleanupRunsAfterTheBodyPasses is the ordinary path, and it pins
// the order rather than only the fact.
//
// Cleanup steps run in reverse written order, so a cleanup written beside the
// setup it undoes is correct without the author thinking about it. The fixture
// creates two tables and drops them with a foreign key between, so a run in
// written order fails on the referenced table and a run in reverse order does
// not -- the order is the thing under test, not a detail of the fixture.
func TestRunTest_CleanupRunsAfterTheBodyPasses(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{{
			Name: "teardown in reverse",
			Steps: []dbtest.Step{
				{Exec: "CREATE TABLE parent (id INTEGER PRIMARY KEY)"},
				{Exec: "CREATE TABLE child (id INTEGER REFERENCES parent(id))"},
				{Exec: "PRAGMA foreign_keys = ON"},
			},
			Cleanup: []dbtest.Step{
				{Name: "drop parent", Exec: "DROP TABLE parent"},
				{Name: "drop child", Exec: "DROP TABLE child"},
			},
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse,
		qt.Commentf("report: %s", report.Text()))
	c.Assert(report.Cases[0].CleanupFailed, qt.IsFalse)

	// Three body steps, then the two cleanup steps in reverse written order.
	c.Assert(report.Cases[0].Steps, qt.HasLen, 5)
	c.Assert(report.Cases[0].Steps[3].Name, qt.Equals, "drop child")
	c.Assert(report.Cases[0].Steps[3].Kind, qt.Equals, dbtest.StepKindCleanup)
	c.Assert(report.Cases[0].Steps[4].Name, qt.Equals, "drop parent")
	c.Assert(report.Cases[0].Steps[4].Kind, qt.Equals, dbtest.StepKindCleanup)
}

// TestRunTest_CleanupRunsAfterTheBodyFails is the property the whole design
// exists for.
//
// A cleanup that only ran on success would release nothing on exactly the runs
// that left something behind. The body here fails at its second step, and the
// cleanup still has to appear.
func TestRunTest_CleanupRunsAfterTheBodyFails(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{{
			Name: "body fails",
			Steps: []dbtest.Step{
				{Name: "make it", Exec: "CREATE TABLE leftover (id INTEGER)"},
				{Name: "break", Exec: "SELECT * FROM missing_table"},
				{Name: "never", Exec: "SELECT 1"},
			},
			Cleanup: []dbtest.Step{{Name: "drop it", Exec: "DROP TABLE leftover"}},
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsTrue)

	// Two body steps -- the third never ran -- and then the cleanup.
	c.Assert(report.Cases[0].Steps, qt.HasLen, 3)
	c.Assert(report.Cases[0].Steps[1].Name, qt.Equals, "break")
	c.Assert(report.Cases[0].Steps[1].Passed, qt.IsFalse)
	c.Assert(report.Cases[0].Steps[2].Name, qt.Equals, "drop it")
	c.Assert(report.Cases[0].Steps[2].Kind, qt.Equals, dbtest.StepKindCleanup)
	c.Assert(report.Cases[0].Steps[2].Passed, qt.IsTrue)

	// The body's failure is what failed the case, not the cleanup.
	c.Assert(report.Cases[0].CleanupFailed, qt.IsFalse)
}

// TestRunTest_ACleanupFailureDoesNotDisplaceTheBodyFailure is the reporting
// half, and it is why CleanupFailed is its own field.
//
// Both failures are real and they have different owners: a failed check is a
// defect in what the case asserts, and a failed teardown is a database left
// dirty. Reporting only the second would send a reader to the wrong one.
func TestRunTest_ACleanupFailureDoesNotDisplaceTheBodyFailure(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{{
			Name:  "both fail",
			Steps: []dbtest.Step{{Name: "check", Exec: "SELECT * FROM missing_table"}},
			Cleanup: []dbtest.Step{
				{Name: "second teardown", Exec: "DROP TABLE also_missing"},
				{Name: "first teardown", Exec: "DROP TABLE still_missing"},
			},
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsTrue)
	c.Assert(report.Cases[0].CleanupFailed, qt.IsTrue)

	// The body's failure is still there, first, with its own detail.
	c.Assert(report.Cases[0].Steps, qt.HasLen, 3)
	c.Assert(report.Cases[0].Steps[0].Name, qt.Equals, "check")
	c.Assert(report.Cases[0].Steps[0].Detail, qt.Contains, "missing_table")

	// Every cleanup step ran, even though the first of them failed: a cleanup
	// stops at nothing, or it leaves held whatever came after the failure.
	c.Assert(report.Cases[0].Steps[1].Name, qt.Equals, "first teardown")
	c.Assert(report.Cases[0].Steps[1].Passed, qt.IsFalse)
	c.Assert(report.Cases[0].Steps[2].Name, qt.Equals, "second teardown")
	c.Assert(report.Cases[0].Steps[2].Passed, qt.IsFalse)
}

// TestRunTest_ASkippedCaseRunsNoCleanup keeps the two features from
// interfering.
//
// A skipped case set nothing up, so tearing down would run statements against a
// database the case never touched -- and a cleanup step in the report would say
// the case did something when it did not.
func TestRunTest_ASkippedCaseRunsNoCleanup(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{{
			Name:    "skipped",
			Skip:    true,
			Steps:   []dbtest.Step{{Exec: "SELECT 1"}},
			Cleanup: []dbtest.Step{{Exec: "THIS WOULD FAIL LOUDLY"}},
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse)
	c.Assert(report.Cases[0].Skipped, qt.IsTrue)
	c.Assert(report.Cases[0].Steps, qt.HasLen, 0)
	c.Assert(report.Cases[0].CleanupFailed, qt.IsFalse)
}

// TestReport_MarksACleanupStepInEveryFormat keeps the distinction readable
// wherever a reader consumes the run.
func TestReport_MarksACleanupStepInEveryFormat(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{{
			Name:    "marked",
			Steps:   []dbtest.Step{{Name: "body", Exec: "SELECT 1"}},
			Cleanup: []dbtest.Step{{Name: "teardown", Exec: "SELECT 1"}},
		}},
	})
	c.Assert(err, qt.IsNil)

	text := report.Text()
	c.Assert(text, qt.Contains, `PASS    step "body"`)
	c.Assert(text, qt.Contains, `PASS    cleanup step "teardown"`)

	rendered, err := report.JSON()
	c.Assert(err, qt.IsNil)
	var document struct {
		Cases []struct {
			Steps []struct {
				Name string `json:"name"`
				Kind string `json:"kind"`
			} `json:"steps"`
		} `json:"cases"`
	}
	c.Assert(json.Unmarshal([]byte(rendered), &document), qt.IsNil)
	c.Assert(document.Cases[0].Steps[0].Kind, qt.Equals, "")
	c.Assert(document.Cases[0].Steps[1].Kind, qt.Equals, "cleanup")
}

// TestParseAtlasTestCases_CleanupBlock covers the translation.
//
// A `cleanup` block leaves the body and joins the case's teardown, so a
// document that writes one between two `exec` steps still runs both of them
// before it.
func TestParseAtlasTestCases_CleanupBlock(t *testing.T) {
	const document = `
test "schema" "translated" {
  exec { sql = "SELECT 1" }
  cleanup { sql = "DROP TABLE a" }
  exec { sql = "SELECT 2" }
  cleanup { sql = "DROP TABLE b" }
}
`

	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases([]byte(document), "s.test.hcl", dbtest.AtlasTestKindSchema)

	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 1)

	// The body keeps only the statements, in their own order.
	c.Assert(cases[0].Steps, qt.HasLen, 2)
	c.Assert(cases[0].Steps[0].Exec, qt.Equals, "SELECT 1")
	c.Assert(cases[0].Steps[1].Exec, qt.Equals, "SELECT 2")

	// The cleanup keeps written order in the model; the runner reverses it.
	c.Assert(cases[0].Cleanup, qt.HasLen, 2)
	c.Assert(cases[0].Cleanup[0].Exec, qt.Equals, "DROP TABLE a")
	c.Assert(cases[0].Cleanup[1].Exec, qt.Equals, "DROP TABLE b")
}

// TestParseAtlasTestCases_CleanupRefusals_FailurePath keeps the block failing
// closed.
func TestParseAtlasTestCases_CleanupRefusals_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     string
	}{
		{
			name:     "cleanup requires its statement",
			document: "test \"schema\" \"a\" {\n  exec { sql = \"SELECT 1\" }\n  cleanup {\n  }\n}\n",
			want:     ".*`cleanup` requires sql.*",
		},
		{
			name:     "cleanup takes nothing else",
			document: "test \"schema\" \"a\" {\n  cleanup {\n    sql   = \"SELECT 1\"\n    match = \"1\"\n  }\n}\n",
			want:     ".*`cleanup` does not take \\[match\\].*",
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

// TestReport_TheHTMLReportMarksACleanupStepStructurally closes the gap the
// end-to-end check found.
//
// The HTML report classified a step by outcome alone, so a failed cleanup and a
// failed check rendered identically. The word "cleanup" did appear in the page
// -- but only because the `.test.hcl` translation happens to NAME the step
// that, which is an accident of one authoring format rather than something the
// report says. A Go-authored cleanup with any other name showed nothing, which
// is the fixture here.
func TestReport_TheHTMLReportMarksACleanupStepStructurally(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{{
			Name:    "named anything",
			Steps:   []dbtest.Step{{Name: "body", Exec: "SELECT 1"}},
			Cleanup: []dbtest.Step{{Name: "tear it down", Exec: "SELECT 1"}},
		}},
	})
	c.Assert(err, qt.IsNil)

	page, err := report.HTML()
	c.Assert(err, qt.IsNil)

	// The marker is the report's own, not the step's name.
	c.Assert(page, qt.Contains, `<span class="noun">cleanup step</span>`)
	c.Assert(page, qt.Contains, `<span class="noun">step</span>`)
	c.Assert(strings.Count(page, `<span class="noun">cleanup step</span>`), qt.Equals, 1)
}
