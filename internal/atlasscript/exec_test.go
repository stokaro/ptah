package atlasscript_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasscript"
)

// countUsers is the state assertion every rollback test needs: the table, read
// after the script returned.
func countUsers(c *qt.C, db *sql.DB) int {
	c.Helper()

	var count int
	c.Assert(db.QueryRow("SELECT count(*) FROM users").Scan(&count), qt.IsNil)
	return count
}

// An exec script changes data and commits.
func TestRunExec_RunsAndCommits(t *testing.T) {
	c := qt.New(t)
	db := seeded(c)
	scripts := parse(c, `
script "exec" "purge" {
  exec "delete" {
    sql = "DELETE FROM users WHERE id = 1"
  }
}
`)

	outcomes, err := atlasscript.RunExec(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.IsNil)
	c.Assert(outcomes, qt.HasLen, 1)
	c.Assert(outcomes[0].Affected, qt.Equals, int64(1))
	c.Assert(countUsers(c, db), qt.Equals, 1)
}

// A step that fails leaves the steps before it undone.
//
// This is the property the transaction exists for, and it is asserted on the
// TABLE rather than on the error: a script that reported a failure and left the
// first delete committed would pass an error-only test while leaving the
// database in a state no script describes.
func TestRunExec_AFailedStepUndoesTheOnesBeforeIt(t *testing.T) {
	c := qt.New(t)
	db := seeded(c)
	scripts := parse(c, `
script "exec" "half" {
  exec "first" {
    sql = "DELETE FROM users WHERE id = 1"
  }
  exec "broken" {
    sql = "DELETE FROM nosuchtable"
  }
}
`)

	_, err := atlasscript.RunExec(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.ErrorMatches, `exec "broken" \(script\.hcl:6\): .*`)
	c.Assert(countUsers(c, db), qt.Equals, 2)
}

// expect_rows that is not met rolls the script back.
//
// The assertion is the point of the field: a purge that expected to remove
// three rows and removed three hundred is a script that did the wrong thing,
// and committing it because no statement errored would be the worst outcome.
func TestRunExec_AnUnmetExpectRowsRollsBack(t *testing.T) {
	tests := []struct {
		name        string
		expectRows  string
		wantMessage string
	}{
		{name: "too few", expectRows: "5", wantMessage: "expected 5 rows, changed 2"},
		{name: "too many", expectRows: "1", wantMessage: "expected 1 rows, changed 2"},
		{name: "zero when there is work", expectRows: "0", wantMessage: "expected 0 rows, changed 2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := seeded(c)
			scripts := parse(c, `
script "exec" "purge" {
  exec "delete" {
    sql         = "DELETE FROM users"
    expect_rows = `+test.expectRows+`
  }
}
`)

			_, err := atlasscript.RunExec(context.Background(), db, scripts[0],
				atlasscript.RunOptions{Now: fixedClock()})

			c.Assert(err, qt.ErrorMatches, ".*"+test.wantMessage+".*")
			c.Assert(countUsers(c, db), qt.Equals, 2)
		})
	}
}

// expect_rows that is met commits, which is the control.
func TestRunExec_AMetExpectRowsCommits(t *testing.T) {
	c := qt.New(t)
	db := seeded(c)
	scripts := parse(c, `
script "exec" "purge" {
  exec "delete" {
    sql         = "DELETE FROM users"
    expect_rows = 2
  }
}
`)

	_, err := atlasscript.RunExec(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.IsNil)
	c.Assert(countUsers(c, db), qt.Equals, 0)
}

// A condition that does not hold stops the script, undoes nothing, and is not
// a fault.
//
// A purge guarded by "only if there is something to purge" that finds nothing
// is a successful run with no work. ErrConditionFalse is its own sentinel so a
// caller can tell that from a failure rather than paging somebody.
func TestRunExec_AFalseConditionStopsWithoutFaulting(t *testing.T) {
	c := qt.New(t)
	db := seeded(c)
	scripts := parse(c, `
script "exec" "guarded" {
  condition "any_inactive" {
    sql = "SELECT 0"
  }
  exec "delete" {
    sql = "DELETE FROM users"
  }
}
`)

	_, err := atlasscript.RunExec(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.ErrorIs, atlasscript.ErrConditionFalse)
	c.Assert(countUsers(c, db), qt.Equals, 2)
}

// A condition that holds lets the script through, which is the control the
// refusal above needs.
func TestRunExec_ASatisfiedConditionLetsTheScriptRun(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "a positive count", sql: "SELECT count(*) FROM users"},
		{name: "a literal one", sql: "SELECT 1"},
		{name: "a row that exists", sql: "SELECT id FROM users WHERE id = 1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := seeded(c)
			scripts := parse(c, `
script "exec" "guarded" {
  condition "guard" {
    sql = "`+test.sql+`"
  }
  exec "delete" {
    sql = "DELETE FROM users"
  }
}
`)

			_, err := atlasscript.RunExec(context.Background(), db, scripts[0],
				atlasscript.RunOptions{Now: fixedClock()})

			c.Assert(err, qt.IsNil)
			c.Assert(countUsers(c, db), qt.Equals, 0)
		})
	}
}

// An empty result set is a false condition, not an error.
//
// "SELECT id FROM users WHERE active = 0 LIMIT 1" returning nothing is exactly
// the guard working.
func TestRunExec_AnEmptyResultIsAFalseCondition(t *testing.T) {
	c := qt.New(t)
	db := seeded(c)
	scripts := parse(c, `
script "exec" "guarded" {
  condition "none" {
    sql = "SELECT id FROM users WHERE id = 999"
  }
  exec "delete" {
    sql = "DELETE FROM users"
  }
}
`)

	_, err := atlasscript.RunExec(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.ErrorIs, atlasscript.ErrConditionFalse)
	c.Assert(countUsers(c, db), qt.Equals, 2)
}

// The report carries the transaction boundaries and each step.
func TestRunExec_ReportsTheTransactionAndEachStep(t *testing.T) {
	c := qt.New(t)
	db := seeded(c)
	scripts := parse(c, `
script "exec" "purge" {
  condition "guard" {
    sql = "SELECT 1"
  }
  exec "delete" {
    sql = "DELETE FROM users WHERE id = 1"
  }
  output {
    message = "purged"
  }
}
`)
	var report strings.Builder

	_, err := atlasscript.RunExec(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Report: &report, Now: fixedClock()})

	c.Assert(err, qt.IsNil)
	text := report.String()
	c.Assert(text, qt.Contains, "-- tx open")
	c.Assert(text, qt.Contains, `-- condition "guard" ok (script.hcl:3)`)
	c.Assert(text, qt.Contains, `-- exec "delete" (script.hcl:6)`)
	c.Assert(text, qt.Contains, "1 rows affected")
	c.Assert(text, qt.Contains, "-- output (script.hcl:9): purged")
	c.Assert(text, qt.Contains, "-- tx commit")
	c.Assert(text, qt.Not(qt.Contains), "-- tx rollback")
}

// A rollback is reported, so a reader is not left to infer it from the absence
// of a commit.
func TestRunExec_ARollbackIsReported(t *testing.T) {
	c := qt.New(t)
	db := seeded(c)
	scripts := parse(c, `
script "exec" "broken" {
  exec "e" {
    sql = "DELETE FROM nosuchtable"
  }
}
`)
	var report strings.Builder

	_, err := atlasscript.RunExec(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Report: &report, Now: fixedClock()})

	c.Assert(err, qt.IsNotNil)
	c.Assert(report.String(), qt.Contains, "-- tx rollback")
	c.Assert(report.String(), qt.Not(qt.Contains), "-- tx commit")
}

// A script of another kind is refused by name.
func TestRunExec_RefusesAScriptOfAnotherKind(t *testing.T) {
	c := qt.New(t)
	db := seeded(c)
	scripts := parse(c, `
script "query" "report" {
  query "rows" {
    sql = "SELECT id FROM users"
  }
}
`)

	_, err := atlasscript.RunExec(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.ErrorMatches, ".*only exec scripts run here.*")
}
