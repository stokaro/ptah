package dbtest_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/dbtest"
)

// TestRunTest_AResultSetIsTheWholeResult is the defect this closes, not merely
// a feature it adds (stokaro/ptah#2866).
//
// An `exec` carrying `output` used to compare the first value of the first row.
// Measured against the previous binary, `output = "1"` PASSED for a query
// answering `(1,'x'),(2,'y')`: two rows and two columns, of which one value was
// checked and three were not. The expectation was satisfied by a result nobody
// would call equal to it.
func TestRunTest_AResultSetIsTheWholeResult(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{{
			Name: "two rows are not one value",
			Steps: []dbtest.Step{
				{Exec: "CREATE TABLE m (a INTEGER, b TEXT)"},
				{Exec: "INSERT INTO m VALUES (1, 'x'), (2, 'y')"},
				{Assert: &dbtest.Assertion{
					Query:     "SELECT a, b FROM m ORDER BY a",
					ResultSet: new("1"),
				}},
			},
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsTrue)
	c.Assert(report.Cases[0].Steps[2].Detail, qt.Contains, `got "1,x`)
}

// TestRunTest_ResultSetLayouts_HappyPath pins both renderings on a result with
// more than one row and more than one column.
//
// A single-value fixture would pass under either layout and under a
// first-value comparison too, so it would establish nothing about which of the
// three ran.
func TestRunTest_ResultSetLayouts_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		layout dbtest.ResultLayout
		want   string
	}{
		{
			name:   "csv is the layout an assertion gets by naming none",
			layout: "",
			want:   "1,x\n2,y",
		},
		{
			name:   "csv named explicitly",
			layout: dbtest.ResultLayoutCSV,
			want:   "1,x\n2,y",
		},
		{
			name:   "table carries a header and a rule",
			layout: dbtest.ResultLayoutTable,
			want:   " a | b \n---+---\n 1 | x \n 2 | y ",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
				Cases: []dbtest.Case{{
					Name: test.name,
					Steps: []dbtest.Step{
						{Exec: "CREATE TABLE m (a INTEGER, b TEXT)"},
						{Exec: "INSERT INTO m VALUES (1, 'x'), (2, 'y')"},
						{Assert: &dbtest.Assertion{
							Query:        "SELECT a, b FROM m ORDER BY a",
							ResultSet:    &test.want,
							ResultLayout: test.layout,
						}},
					},
				}},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(report.Failed(), qt.IsFalse,
				qt.Commentf("detail: %s", report.Cases[0].Steps[2].Detail))
		})
	}
}

// TestAssertion_RefusesALayoutWithNothingToLayOut keeps an instruction from
// being accepted and ignored.
//
// An author who writes `result_layout` expects it to have decided something.
// Beside any other condition it decides nothing, and accepting it silently is
// the shape the whole one-condition rule exists to prevent.
func TestAssertion_RefusesALayoutWithNothingToLayOut(t *testing.T) {
	tests := []struct {
		name      string
		assertion dbtest.Assertion
		wantErr   string
	}{
		{
			name:      "a layout without a result set",
			assertion: dbtest.Assertion{Query: "SELECT 1", Scalar: new("1"), ResultLayout: dbtest.ResultLayoutCSV},
			wantErr:   `(?s).*result_layout applies to result_set.*`,
		},
		{
			name:      "an unknown layout",
			assertion: dbtest.Assertion{Query: "SELECT 1", ResultSet: new("1"), ResultLayout: "yaml"},
			wantErr:   `(?s).*result_layout must be "csv" or "table", got "yaml".*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := dbtest.RunMigrationTest(context.Background(),
				dbtest.Options{Cases: []dbtest.Case{{
					Name:  "bad layout",
					Steps: []dbtest.Step{{Assert: &test.assertion}},
				}}})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestReport_DistinguishesALogAndACaughtFailureFromAPassingStep is the row the
// first slice left half done.
//
// `Passed` is true for all three, and a reader counting passing steps to see
// what a case proved would count a logged message among them and read an
// expected failure as an ordinary success. The distinction has to survive into
// each format a reader actually consumes.
func TestReport_DistinguishesALogAndACaughtFailureFromAPassingStep(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{{
			Name: "three outcomes",
			Steps: []dbtest.Step{
				{Name: "note", Log: "a message"},
				{Name: "plain", Exec: "SELECT 1"},
				{Name: "expected", Assert: &dbtest.Assertion{
					Query: "SELECT * FROM missing_table", ExpectAnyError: true,
				}},
			},
		}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse)

	// The model: three passing steps, and only two of them are checks.
	c.Assert(report.Cases[0].Steps[0].Kind, qt.Equals, dbtest.StepKindLog)
	c.Assert(report.Cases[0].Steps[1].Kind, qt.Equals, dbtest.StepKind(""))
	c.Assert(report.Cases[0].Steps[2].Kind, qt.Equals, dbtest.StepKindCaught)

	text := report.Text()
	c.Assert(text, qt.Contains, `LOG     step "note"`)
	c.Assert(text, qt.Contains, `PASS    step "plain"`)
	c.Assert(text, qt.Contains, `CAUGHT  step "expected"`)

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
	c.Assert(document.Cases[0].Steps[0].Kind, qt.Equals, "log")
	// Absent rather than empty-string-valued: a report of ordinary steps is
	// byte-identical to one from before the field existed.
	c.Assert(document.Cases[0].Steps[1].Kind, qt.Equals, "")
	c.Assert(rendered, qt.Not(qt.Contains), `"kind": ""`)
	c.Assert(document.Cases[0].Steps[2].Kind, qt.Equals, "caught")

	page, err := report.HTML()
	c.Assert(err, qt.IsNil)
	c.Assert(page, qt.Contains, `<span class="tag log">LOG</span>`)
	c.Assert(page, qt.Contains, `<span class="tag caught">CAUGHT</span>`)
	c.Assert(strings.Count(page, `<span class="tag pass">PASS</span>`), qt.Equals, 1)
}

// TestReport_AFailedExpectedFailureIsNotCaught puts the label where it is true.
//
// A `catch` whose statement succeeded did not catch anything, and labeling it
// caught would put the word on precisely the case it describes worst.
func TestReport_AFailedExpectedFailureIsNotCaught(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{{
			Name: "nothing failed",
			Steps: []dbtest.Step{
				{Name: "expected", Assert: &dbtest.Assertion{Query: "SELECT 1", ExpectAnyError: true}},
			},
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsTrue)
	c.Assert(report.Cases[0].Steps[0].Kind, qt.Equals, dbtest.StepKind(""))
	c.Assert(report.Text(), qt.Contains, `FAIL    step "expected"`)
}
