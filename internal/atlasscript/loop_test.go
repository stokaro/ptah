package atlasscript_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite" // registers the SQLite driver for database/sql

	"go.5x5.cz/ptah/internal/atlasscript"
)

// manyRows opens a database with enough rows to need several batches.
func manyRows(c *qt.C, count int) *sql.DB {
	c.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, done INTEGER NOT NULL)`)
	c.Assert(err, qt.IsNil)
	for index := 1; index <= count; index++ {
		_, err = db.Exec(`INSERT INTO items VALUES (?, 0)`, index)
		c.Assert(err, qt.IsNil)
	}
	return db
}

// purgeScript walks items in batches and deletes each batch.
const purgeScript = `
script "loop" "purge" {
  iterator "keyset" {
    cursor { id = int }
    init {
      sql = "SELECT id FROM items ORDER BY id LIMIT 2"
    }
    next {
      sql  = "SELECT id FROM items WHERE id > ? ORDER BY id LIMIT 2"
      args = [cursor.id]
    }
  }
  do {
    exec "delete" {
      sql = "DELETE FROM items WHERE id IN (SELECT id FROM items ORDER BY id LIMIT 2)"
    }
  }
}
`

// A loop walks the whole table in batches and its body runs once per batch.
func TestRunLoop_WalksInBatchesUntilTheWalkEnds(t *testing.T) {
	c := qt.New(t)
	db := manyRows(c, 7)
	scripts := parse(c, purgeScript)

	outcome, err := atlasscript.RunLoop(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.IsNil)
	// Seven rows in batches of two: four batches, the last holding one.
	c.Assert(outcome.Batches, qt.Equals, 4)
	c.Assert(outcome.Rows, qt.Equals, 7)
	var remaining int
	c.Assert(db.QueryRow("SELECT count(*) FROM items").Scan(&remaining), qt.IsNil)
	c.Assert(remaining, qt.Equals, 0)
}

// An empty table produces no batch and no work.
func TestRunLoop_AnEmptyWalkRunsTheBodyNotAtAll(t *testing.T) {
	c := qt.New(t)
	db := manyRows(c, 0)
	scripts := parse(c, purgeScript)

	outcome, err := atlasscript.RunLoop(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.IsNil)
	c.Assert(outcome.Batches, qt.Equals, 0)
	c.Assert(outcome.Steps, qt.HasLen, 0)
}

// A body that does not consume the rows the walk selects is stopped rather
// than run forever.
//
// This is the failure the bound exists for: a keyset loop ends when a batch
// comes back empty, and that depends on the body removing what the walk
// selects. A body that does not walks the same batch until something else
// gives out, holding transactions against a live database. The error names the
// script and says what is wrong rather than timing out.
func TestRunLoop_ABodyThatConsumesNothingIsStopped(t *testing.T) {
	c := qt.New(t)
	db := manyRows(c, 4)
	scripts := parse(c, `
script "loop" "spins" {
  iterator "keyset" {
    cursor { id = int }
    init {
      sql = "SELECT id FROM items ORDER BY id LIMIT 2"
    }
    next {
      sql  = "SELECT id FROM items ORDER BY id LIMIT 2"
      args = [cursor.id]
    }
  }
  do {
    exec "nothing" {
      sql = "UPDATE items SET done = done"
    }
  }
}
`)

	outcome, err := atlasscript.RunLoop(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock(), MaxBatches: 5})

	c.Assert(err, qt.ErrorMatches, `.*without the walk ending.*not consuming.*`)
	c.Assert(outcome.Batches, qt.Equals, 5)
}

// A failing batch undoes itself and leaves the batches before it committed.
//
// That is the point of one transaction per batch rather than one per script: a
// purge over a million rows must not roll back an hour of work on the last
// statement, and a rerun has to know where it stopped.
func TestRunLoop_AFailingBatchLeavesTheEarlierOnesCommitted(t *testing.T) {
	c := qt.New(t)
	db := manyRows(c, 6)
	scripts := parse(c, `
script "loop" "breaks" {
  iterator "keyset" {
    cursor { id = int }
    init {
      sql = "SELECT id FROM items ORDER BY id LIMIT 2"
    }
    next {
      sql  = "SELECT id FROM items WHERE id > ? ORDER BY id LIMIT 2"
      args = [cursor.id]
    }
  }
  do {
    exec "delete" {
      sql = "DELETE FROM items WHERE id IN (SELECT id FROM items ORDER BY id LIMIT 2)"
    }
    exec "fails_on_the_third" {
      sql         = "UPDATE items SET done = 1"
      expect_rows = 4
    }
  }
}
`)

	outcome, err := atlasscript.RunLoop(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.ErrorMatches, `loop "breaks" batch \d+: .*expected 4 rows.*`)
	// The batch that failed is undone; the ones before it are not.
	var remaining int
	c.Assert(db.QueryRow("SELECT count(*) FROM items").Scan(&remaining), qt.IsNil)
	c.Assert(remaining > 0, qt.IsTrue)
	c.Assert(remaining < 6, qt.IsTrue,
		qt.Commentf("nothing was committed, so the batches are not independent"))
	c.Assert(outcome.Batches > 0, qt.IsTrue)
}

// The report names each batch, so a rerun knows where the walk stopped.
func TestRunLoop_ReportsEachBatch(t *testing.T) {
	c := qt.New(t)
	db := manyRows(c, 3)
	scripts := parse(c, purgeScript)
	var report strings.Builder

	_, err := atlasscript.RunLoop(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Report: &report, Now: fixedClock()})

	c.Assert(err, qt.IsNil)
	text := report.String()
	c.Assert(text, qt.Contains, "-- batch 1 | 2 rows")
	c.Assert(text, qt.Contains, "-- batch 2 | 1 rows")
	c.Assert(text, qt.Contains, "-- tx commit")
	c.Assert(text, qt.Contains, "2 batches, 3 rows")
}

// A script of another kind is refused by name.
func TestRunLoop_RefusesAScriptOfAnotherKind(t *testing.T) {
	c := qt.New(t)
	db := manyRows(c, 1)
	scripts := parse(c, `
script "exec" "purge" {
  exec "e" { sql = "DELETE FROM items" }
}
`)

	_, err := atlasscript.RunLoop(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.ErrorMatches, ".*only loop scripts run here.*")
}
