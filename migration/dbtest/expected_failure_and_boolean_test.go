package dbtest_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/dbtest"
)

// TestRunTest_ExpectedFailureAndBooleanAssertions_HappyPath covers the
// conditions this slice added to the shared model (stokaro/ptah#2866).
//
// Each row is the passing half of a pair; the failing half is the test below,
// and neither establishes the other. A condition that always passed would
// satisfy this table alone, and a condition that always failed would satisfy
// the next one alone.
func TestRunTest_ExpectedFailureAndBooleanAssertions_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		steps []dbtest.Step
	}{
		{
			name: "expect_any_error passes when the statement fails",
			steps: []dbtest.Step{
				{Assert: &dbtest.Assertion{Query: "SELECT * FROM missing_table", ExpectAnyError: true}},
			},
		},
		{
			name: "error_matches passes on a pattern the message matches",
			steps: []dbtest.Step{
				// A pattern rather than a substring: the dot has to match for
				// this to pass, so a substring implementation fails the row.
				{Assert: &dbtest.Assertion{Query: "SELECT * FROM missing_table", ErrorMatches: "missing.table"}},
			},
		},
		{
			name: "match passes on a pattern the value matches",
			steps: []dbtest.Step{
				{Exec: "CREATE TABLE m (name TEXT)"},
				{Exec: "INSERT INTO m (name) VALUES ('ada')"},
				{Assert: &dbtest.Assertion{Query: "SELECT name FROM m", Match: "^a.a$"}},
			},
		},
		{
			name: "true passes on a value the database reports as true",
			steps: []dbtest.Step{
				{Assert: &dbtest.Assertion{Query: "SELECT 1", True: true}},
			},
		},
		{
			name: "log passes and reaches no database",
			steps: []dbtest.Step{
				{Log: "nothing here touches the target"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report, err := dbtest.RunMigrationTest(context.Background(),
				dbtest.Options{Cases: []dbtest.Case{{Name: test.name, Steps: test.steps}}})

			c.Assert(err, qt.IsNil)
			c.Assert(report.Failed(), qt.IsFalse)
		})
	}
}

// TestRunTest_ExpectedFailureAndBooleanAssertions_FailurePath is the other
// half, and the detail is asserted rather than only the verdict.
//
// A failing case says nothing about whether it failed for the reason the author
// wrote, and every one of these conditions has a plausible implementation that
// fails for a different one -- a `true` assertion that errors on a false value
// rather than failing the case, an `expect_any_error` that reports the
// statement's own error as the failure.
func TestRunTest_ExpectedFailureAndBooleanAssertions_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		steps      []dbtest.Step
		wantDetail string
	}{
		{
			name: "expect_any_error fails when the statement succeeds",
			steps: []dbtest.Step{
				{Assert: &dbtest.Assertion{Query: "SELECT 1", ExpectAnyError: true}},
			},
			wantDetail: "expected an error, but the query succeeded",
		},
		{
			name: "error_matches fails when the message does not match",
			steps: []dbtest.Step{
				{Assert: &dbtest.Assertion{Query: "SELECT * FROM missing_table", ErrorMatches: "^nothing like it$"}},
			},
			wantDetail: `to match "^nothing like it$"`,
		},
		{
			name: "error_matches fails when the statement succeeds",
			steps: []dbtest.Step{
				{Assert: &dbtest.Assertion{Query: "SELECT 1", ErrorMatches: "anything"}},
			},
			wantDetail: `expected an error matching "anything", but the query succeeded`,
		},
		{
			name: "match fails when the value does not match",
			steps: []dbtest.Step{
				{Assert: &dbtest.Assertion{Query: "SELECT 'ada'", Match: "^zz"}},
			},
			wantDetail: `expected "ada" to match "^zz"`,
		},
		{
			name: "true fails on a false value",
			steps: []dbtest.Step{
				{Assert: &dbtest.Assertion{Query: "SELECT 0", True: true}},
			},
			wantDetail: `expected a true value, got "0"`,
		},
		{
			name: "true carries the author's message",
			steps: []dbtest.Step{
				{Assert: &dbtest.Assertion{Query: "SELECT 0", True: true, Message: "every order needs a customer"}},
			},
			wantDetail: "every order needs a customer",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report, err := dbtest.RunMigrationTest(context.Background(),
				dbtest.Options{Cases: []dbtest.Case{{Name: test.name, Steps: test.steps}}})

			c.Assert(err, qt.IsNil)
			c.Assert(report.Failed(), qt.IsTrue)
			c.Assert(report.Cases, qt.HasLen, 1)
			c.Assert(report.Cases[0].Steps, qt.HasLen, 1)
			c.Assert(report.Cases[0].Steps[0].Detail, qt.Contains, test.wantDetail)
		})
	}
}

// TestRunTest_ALogStepRunsWhereItStandsAndDecidesNothing is the control that
// keeps `log` from being either invisible or load-bearing.
//
// Two properties, and a single assertion would miss one of them. Its message
// has to reach the report at its own position, or an author cannot tell which
// statement it introduces; and it must not decide the case, or a message
// between two statements could turn a passing suite red.
func TestRunTest_ALogStepRunsWhereItStandsAndDecidesNothing(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{{
			Name: "logged",
			Steps: []dbtest.Step{
				{Log: "before the table"},
				{Exec: "CREATE TABLE logged (id INTEGER PRIMARY KEY)"},
				{Log: "after the table"},
			},
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse)
	c.Assert(report.Cases[0].Steps, qt.HasLen, 3)
	c.Assert(report.Cases[0].Steps[0].Detail, qt.Equals, "before the table")
	c.Assert(report.Cases[0].Steps[2].Detail, qt.Equals, "after the table")
}

// TestAssertion_RefusesTwoConditionsAtOnce keeps the one-condition rule as the
// new conditions join it.
//
// The rule is what stops a typo in one condition from leaving another
// unchecked, and it is the kind of rule that quietly stops covering the fields
// added after it was written.
func TestAssertion_RefusesTwoConditionsAtOnce(t *testing.T) {
	tests := []struct {
		name      string
		assertion dbtest.Assertion
	}{
		{
			name:      "match and scalar",
			assertion: dbtest.Assertion{Query: "SELECT 1", Match: "1", Scalar: new("1")},
		},
		{
			name:      "true and row_count",
			assertion: dbtest.Assertion{Query: "SELECT 1", True: true, RowCount: new(1)},
		},
		{
			name:      "error_matches and error_contains",
			assertion: dbtest.Assertion{Query: "SELECT 1", ErrorMatches: "x", ErrorContains: "x"},
		},
		{
			name:      "expect_any_error and error_matches",
			assertion: dbtest.Assertion{Query: "SELECT 1", ExpectAnyError: true, ErrorMatches: "x"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := dbtest.RunMigrationTest(context.Background(),
				dbtest.Options{Cases: []dbtest.Case{{
					Name:  "two conditions",
					Steps: []dbtest.Step{{Assert: &test.assertion}},
				}}})

			c.Assert(err, qt.ErrorMatches, `(?s).*must set exactly one of .*but 2 are set.*`)
		})
	}
}

// TestAssertion_RefusesAnInvalidPattern keeps a malformed regular expression an
// invalid case rather than a failing one.
//
// The distinction matters to an author reading a report: a pattern that cannot
// compile is a mistake in the test, and reporting it as "the database did not
// match" would send them to look at the database.
func TestAssertion_RefusesAnInvalidPattern(t *testing.T) {
	tests := []struct {
		name      string
		assertion dbtest.Assertion
	}{
		{name: "match", assertion: dbtest.Assertion{Query: "SELECT 1", Match: "([unclosed"}},
		{name: "error_matches", assertion: dbtest.Assertion{Query: "SELECT 1", ErrorMatches: "([unclosed"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := dbtest.RunMigrationTest(context.Background(),
				dbtest.Options{Cases: []dbtest.Case{{
					Name:  "bad pattern",
					Steps: []dbtest.Step{{Assert: &test.assertion}},
				}}})

			c.Assert(err, qt.ErrorMatches, `(?s).*not a valid regular expression.*`)
		})
	}
}
