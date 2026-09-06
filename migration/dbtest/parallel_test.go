package dbtest_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/dbtest"
)

// parallelCases builds n cases that each create the same table.
//
// Creating one name in every case is what makes the fixture measure isolation:
// run against one shared database, the second case fails with "table already
// exists". Every case getting its own is the only way they all pass.
func parallelCases(n int) []dbtest.Case {
	cases := make([]dbtest.Case, 0, n)
	for i := range n {
		cases = append(cases, dbtest.Case{
			Name:     fmt.Sprintf("case-%02d", i),
			Parallel: true,
			Steps: []dbtest.Step{
				{Exec: "CREATE TABLE contended (id INTEGER PRIMARY KEY)"},
				{Exec: "INSERT INTO contended (id) VALUES (1)"},
				{Assert: &dbtest.Assertion{Query: "SELECT COUNT(*) FROM contended", Scalar: new("1")}},
			},
		})
	}
	return cases
}

// TestRunTest_ParallelCasesKeepTheirOwnDatabase is the property the feature
// stands on.
//
// Every case creates a table of the SAME name and asserts it holds exactly one
// row. Sharing a database would fail the second case on "table already exists"
// and would make the row count depend on how many cases had inserted by then,
// so a passing run means each case really did get a database of its own.
//
// Enough cases to overlap: the bound is the machine's parallelism, and a table
// of two might be serialized by scheduling alone on a busy runner.
func TestRunTest_ParallelCasesKeepTheirOwnDatabase(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: parallelCases(12),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse, qt.Commentf("report: %s", report.Text()))
	c.Assert(report.Cases, qt.HasLen, 12)
}

// TestRunTest_AParallelReportKeepsDocumentOrder is what makes a parallel run
// comparable with a serial one.
//
// Results are placed by position rather than appended as they arrive, so two
// runs of the same file produce the same report whatever the machine's timing
// was. Appending would pass every other assertion here and produce a report
// whose order changes between runs.
func TestRunTest_AParallelReportKeepsDocumentOrder(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: parallelCases(12),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Cases, qt.HasLen, 12)
	for i := range report.Cases {
		c.Assert(report.Cases[i].Name, qt.Equals, fmt.Sprintf("case-%02d", i))
	}
}

// TestRunTest_EveryParallelCaseProducesAResult is the anti-goal made into an
// assertion.
//
// A suite that reported success while some of its cases never ran is the
// failure this mode hides most easily: the run is green, the count is smaller,
// and nothing says which case is missing. Every case here has to appear, named
// and passed.
func TestRunTest_EveryParallelCaseProducesAResult(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: parallelCases(12),
	})

	c.Assert(err, qt.IsNil)
	for i := range report.Cases {
		c.Assert(report.Cases[i].Passed, qt.IsTrue,
			qt.Commentf("case %d: %s", i, report.Cases[i].Name))
		c.Assert(report.Cases[i].Steps, qt.HasLen, 3)
	}
}

// TestRunTest_ParallelAndSerialCasesMix keeps an unmarked case running the way
// it always did.
//
// `parallel` is per case, so a file may hold both. The serial case here shares
// its position in the report with the parallel ones around it.
func TestRunTest_ParallelAndSerialCasesMix(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{
			{Name: "first", Parallel: true, Steps: []dbtest.Step{{Exec: "CREATE TABLE a (id INTEGER)"}}},
			{Name: "second", Steps: []dbtest.Step{{Exec: "CREATE TABLE a (id INTEGER)"}}},
			{Name: "third", Parallel: true, Steps: []dbtest.Step{{Exec: "CREATE TABLE a (id INTEGER)"}}},
			{Name: "fourth", Skip: true, Steps: []dbtest.Step{{Exec: "THIS WOULD FAIL"}}},
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse, qt.Commentf("report: %s", report.Text()))
	c.Assert(report.Cases, qt.HasLen, 4)
	c.Assert(report.Cases[0].Name, qt.Equals, "first")
	c.Assert(report.Cases[1].Name, qt.Equals, "second")
	c.Assert(report.Cases[2].Name, qt.Equals, "third")

	// A skipped case stays skipped in this mode too, and still runs nothing.
	c.Assert(report.Cases[3].Name, qt.Equals, "fourth")
	c.Assert(report.Cases[3].Skipped, qt.IsTrue)
	c.Assert(report.Cases[3].Steps, qt.HasLen, 0)
}

// TestRunTest_ParallelAgainstANamedServerIsolatesEachCase is what `parallel`
// buys against a throwaway server the caller named.
//
// Every case creates a table of the SAME name, so sharing the named database
// would fail the second case on "table already exists". Passing means each case
// really did get a database of its own on that server, created and removed
// around it.
//
// A file that never says `parallel` keeps the documented behavior of one shared
// database; that contract is pinned by
// TestRunSchemaTest_ExplicitDBURLPreservesStateBetweenCases and is deliberately
// not changed here.
func TestRunTest_ParallelAgainstANamedServerIsolatesEachCase(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		DBURL: "sqlite://" + filepath.Join(t.TempDir(), "server.db"),
		Cases: parallelCases(6),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse, qt.Commentf("report: %s", report.Text()))
	c.Assert(report.Cases, qt.HasLen, 6)
}

// TestRunTest_ParallelIsRefusedWhereItCannotBeIsolated_FailurePath keeps the
// refusal for a dialect that has no way to give a case its own database.
//
// It is a LOAD failure -- no report comes back -- so a suite whose third case
// cannot be isolated does not first create two databases and apply a schema to
// each. Sharing instead would make a run pass when two cases happened not to
// collide.
func TestRunTest_ParallelIsRefusedWhereItCannotBeIsolated_FailurePath(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		DBURL: "clickhouse://localhost:9000/dev",
		Cases: []dbtest.Case{
			{Name: "ordinary", Steps: []dbtest.Step{{Exec: "SELECT 1"}}},
			{Name: "wants concurrency", Parallel: true, Steps: []dbtest.Step{{Exec: "SELECT 1"}}},
		},
	})

	c.Assert(err, qt.ErrorIs, dbtest.ErrParallelNeedsIsolation)
	c.Assert(report, qt.IsNil)
	c.Assert(err.Error(), qt.Contains, "clickhouse")
}

// TestRunTest_ASharedDatabaseStillRunsSerialCases is the control on that
// refusal.
//
// Without it, refusing every run against a caller-owned database would satisfy
// the test above, and the refusal test alone cannot tell that apart from the
// rule being applied only where it belongs.
func TestRunTest_ASharedDatabaseStillRunsSerialCases(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		DBURL: "sqlite://" + t.TempDir() + "/shared.db",
		Cases: []dbtest.Case{
			{Name: "ordinary", Steps: []dbtest.Step{{Exec: "SELECT 1"}}},
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse)
	c.Assert(report.Cases, qt.HasLen, 1)
}

// TestParseAtlasTestCases_ParallelAttribute covers the translation, including
// that it is an expression rather than a literal.
func TestParseAtlasTestCases_ParallelAttribute(t *testing.T) {
	const document = `
variable "concurrent" {
  default = true
}

test "schema" "computed" {
  parallel = var.concurrent
  exec { sql = "SELECT 1" }
}

test "schema" "plain" {
  exec { sql = "SELECT 1" }
}
`

	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases([]byte(document), "s.test.hcl", dbtest.AtlasTestKindSchema)

	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 2)
	c.Assert(cases[0].Parallel, qt.IsTrue)
	// The control: an unmarked case is not parallel, so the attribute is read
	// rather than assumed.
	c.Assert(cases[1].Parallel, qt.IsFalse)
}

// TestParseAtlasTestCases_ParallelRefusesANonBoolean_FailurePath keeps the
// attribute failing closed, with the name of the attribute in the message.
//
// The name matters because `skip` and `parallel` share one reader: a message
// that always said `skip` would send an author to the wrong line.
func TestParseAtlasTestCases_ParallelRefusesANonBoolean_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     string
	}{
		{
			name:     "parallel names itself",
			document: "test \"schema\" \"a\" {\n  parallel = \"yes\"\n  exec { sql = \"SELECT 1\" }\n}\n",
			want:     ".*`parallel` must be a boolean, got string.*",
		},
		{
			name:     "skip still names itself",
			document: "test \"schema\" \"a\" {\n  skip = 7\n  exec { sql = \"SELECT 1\" }\n}\n",
			want:     ".*`skip` must be a boolean, got number.*",
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
