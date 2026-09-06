//go:build integration

package dbtest_test

import (
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
	"ptah.run/migration/dbtest"
)

// TestRunMigrationTest_ParallelCasesGetTheirOwnDatabaseOnAServer covers the
// half of per-case isolation SQLite cannot reach.
//
// A SQLite case is isolated by getting a file of its own, which exercises no
// server at all. On PostgreSQL the case's database is CREATEd on the server and
// DROPped afterwards, and nothing in the unit suite runs that path -- it is the
// one place a wrong URL, a name an engine refuses, or a drop that cannot run
// while a session holds the database would show up.
//
// Every case creates a table of the SAME name, so a shared database fails the
// second case on "already exists". Passing means each really did get its own.
func TestRunMigrationTest_ParallelCasesGetTheirOwnDatabaseOnAServer(t *testing.T) {
	c := qt.New(t)
	serverURL := dbtarget.URL(c, dbtarget.PostgreSQL)

	const cases = 6
	built := make([]dbtest.Case, 0, cases)
	for i := range cases {
		built = append(built, dbtest.Case{
			Name:     fmt.Sprintf("isolated-%02d", i),
			Parallel: true,
			Steps: []dbtest.Step{
				{Exec: "CREATE TABLE contended (id INTEGER PRIMARY KEY)"},
				{Exec: "INSERT INTO contended (id) VALUES (1)"},
				{Assert: &dbtest.Assertion{
					Query:    "SELECT COUNT(*) FROM contended",
					RowCount: new(1),
				}},
			},
		})
	}

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		DBURL:       serverURL,
		Parallelism: 3,
		Cases:       built,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse, qt.Commentf("report: %s", report.Text()))
	c.Assert(report.Cases, qt.HasLen, cases)
	for i := range report.Cases {
		c.Assert(report.Cases[i].Passed, qt.IsTrue,
			qt.Commentf("case %s", report.Cases[i].Name))
	}
}

// TestRunMigrationTest_AScratchDatabaseIsRemovedAfterTheRun is the other half:
// the databases a run creates do not accumulate on the server.
//
// A suite run repeatedly against one server would otherwise leave a database
// behind per case per run, and nothing in the report would say so. The count is
// compared before and after rather than asserted absolutely, because the server
// is shared with whatever else is using it.
func TestRunMigrationTest_AScratchDatabaseIsRemovedAfterTheRun(t *testing.T) {
	c := qt.New(t)
	serverURL := dbtarget.URL(c, dbtarget.PostgreSQL)

	before := scratchDatabaseCount(c, serverURL)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		DBURL: serverURL,
		Cases: []dbtest.Case{{
			Name:     "leaves nothing",
			Parallel: true,
			Steps:    []dbtest.Step{{Exec: "CREATE TABLE leftover (id INTEGER)"}},
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse)
	c.Assert(scratchDatabaseCount(c, serverURL), qt.Equals, before)
}
