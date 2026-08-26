package atlasscript_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"

	"go.5x5.cz/ptah/internal/atlasscript"
)

// seeded opens an in-memory database holding two rows worth masking.
//
// A real database rather than a fake: the executor's job is to scan whatever a
// driver hands back, and a fake that returned strings would never exercise the
// NULL and the integer, which are the two shapes that reach renderValue's
// branches.
func seeded(c *qt.C) *sql.DB {
	c.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(`CREATE TABLE users (id INTEGER, email TEXT, note TEXT)`)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(`INSERT INTO users VALUES (1, 'ada@example.com', NULL), (2, 'grace@example.com', 'x')`)
	c.Assert(err, qt.IsNil)
	return db
}

// fixedClock advances by a known step per call, so a report's timings are the
// same on every run and can be asserted on.
func fixedClock() func() time.Time {
	base := time.Unix(0, 0).UTC()
	calls := 0
	return func() time.Time {
		calls++
		return base.Add(time.Duration(calls) * time.Millisecond)
	}
}

// A query script reads its rows and masks them on the way out.
func TestRunQuery_ReadsAndMasks(t *testing.T) {
	c := qt.New(t)
	db := seeded(c)
	scripts := parse(c, `
mask "email" {
  method  = "REDACT"
  columns = ["email"]
}

script "query" "report" {
  query "rows" {
    sql = "SELECT id, email FROM users ORDER BY id"
    use = [mask.email]
  }
}
`)
	var out strings.Builder

	results, err := atlasscript.RunQuery(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Out: &out, Now: fixedClock()})

	c.Assert(err, qt.IsNil)
	c.Assert(results, qt.HasLen, 1)
	c.Assert(results[0].Columns, qt.DeepEquals, []string{"id", "email"})
	c.Assert(results[0].Rows, qt.DeepEquals, [][]string{{"1", "***"}, {"2", "***"}})
	// The unmasked column is carried as it was, so the mask is measured as
	// scoped rather than as blanking the result set.
	c.Assert(out.String(), qt.Contains, "id,email")
	c.Assert(out.String(), qt.Not(qt.Contains), "ada@example.com")
}

// A NULL becomes the empty string, not the four characters.
//
// The product is CSV, where a literal `NULL` is indistinguishable from a column
// whose value is that text.
func TestRunQuery_ANullIsEmptyRatherThanTheWord(t *testing.T) {
	c := qt.New(t)
	db := seeded(c)
	scripts := parse(c, `
script "query" "notes" {
  query "rows" {
    sql = "SELECT id, note FROM users ORDER BY id"
  }
}
`)

	results, err := atlasscript.RunQuery(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.IsNil)
	c.Assert(results[0].Rows, qt.DeepEquals, [][]string{{"1", ""}, {"2", "x"}})
}

// The report names the script, the step and where each was written.
func TestRunQuery_ReportsEachStepAndWhereItCameFrom(t *testing.T) {
	c := qt.New(t)
	db := seeded(c)
	scripts := parse(c, `
script "query" "report" {
  query "rows" {
    sql = "SELECT id FROM users"
  }
  output {
    message = "done"
  }
}
`)
	var report strings.Builder

	_, err := atlasscript.RunQuery(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Report: &report, Now: fixedClock()})

	c.Assert(err, qt.IsNil)
	text := report.String()
	c.Assert(text, qt.Contains, `Executing script "report" (script.hcl:2)`)
	c.Assert(text, qt.Contains, `-- query "rows" (script.hcl:3)`)
	c.Assert(text, qt.Contains, "SELECT id FROM users")
	c.Assert(text, qt.Contains, "2 rows")
	c.Assert(text, qt.Contains, "-- output (script.hcl:6): done")
	c.Assert(text, qt.Contains, "-- 1 statements")
}

// --quiet drops the report and keeps the product.
//
// The two are separate writers rather than one with a flag, so "quiet" cannot
// accidentally suppress the rows the script was run for.
func TestRunQuery_QuietDropsTheReportAndKeepsTheRows(t *testing.T) {
	c := qt.New(t)
	db := seeded(c)
	scripts := parse(c, `
script "query" "report" {
  query "rows" {
    sql = "SELECT id FROM users ORDER BY id"
  }
}
`)
	var out strings.Builder

	_, err := atlasscript.RunQuery(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Out: &out, Now: fixedClock()})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "id\n1\n2\n")
}

// A script that is not a query script is refused by name.
//
// Running one through the read-only path would either fail confusingly at the
// driver or, worse, succeed for a statement the driver happens to accept
// through a query call.
func TestRunQuery_RefusesAScriptThatChangesData(t *testing.T) {
	tests := []struct {
		name     string
		document string
		says     string
	}{
		{
			name: "an exec script",
			document: `
script "exec" "purge" {
  exec "e" { sql = "DELETE FROM users" }
}`,
			says: "only query scripts run here",
		},
		{
			name: "a loop script",
			document: `
script "loop" "purge" {
  iterator "keyset" {
    cursor { id = int }
    init {
      sql = "SELECT id FROM users ORDER BY id LIMIT 2"
    }
    next {
      sql  = "SELECT id FROM users WHERE id > ? ORDER BY id LIMIT 2"
      args = [cursor.id]
    }
  }
  do {
    exec "e" { sql = "DELETE FROM users" }
  }
}`,
			says: "only query scripts run here",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := seeded(c)
			scripts := parse(c, test.document)

			_, err := atlasscript.RunQuery(context.Background(), db, scripts[0],
				atlasscript.RunOptions{Now: fixedClock()})

			c.Assert(err, qt.ErrorMatches, ".*"+test.says+".*")
			// Nothing ran: the table still holds both rows.
			var count int
			c.Assert(db.QueryRow("SELECT count(*) FROM users").Scan(&count), qt.IsNil)
			c.Assert(count, qt.Equals, 2)
		})
	}
}

// A step the query path does not run stops the script rather than being
// skipped.
//
// Skipping would run a partial script and report it as a whole one, which is
// the failure a report exists to prevent.
func TestRunQuery_RefusesAStepItCannotRun(t *testing.T) {
	c := qt.New(t)
	db := seeded(c)
	scripts := parse(c, `
script "query" "mixed" {
  query "rows" { sql = "SELECT id FROM users" }
  condition "guard" { sql = "SELECT 1" }
}
`)

	_, err := atlasscript.RunQuery(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.ErrorMatches, `.*condition step at script\.hcl:4.*`)
}

// A failing statement is reported with the step and its position.
func TestRunQuery_AFailedStatementNamesTheStep(t *testing.T) {
	c := qt.New(t)
	db := seeded(c)
	scripts := parse(c, `
script "query" "broken" {
  query "rows" {
    sql = "SELECT nosuch FROM users"
  }
}
`)

	_, err := atlasscript.RunQuery(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.ErrorMatches, `query "rows" \(script\.hcl:3\): .*`)
}
