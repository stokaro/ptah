//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
)

// scriptArgsDatabase writes a SQLite database with two items priced 0 and
// returns its path.
func scriptArgsDatabase(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "app.db")
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	for _, statement := range []string{
		"CREATE TABLE items (id INTEGER PRIMARY KEY, price INTEGER NOT NULL)",
		"INSERT INTO items VALUES (1, 0), (2, 0)",
	} {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	return path
}

// itemPrices reads the prices back in key order.
func itemPrices(c *qt.C, path string) []int64 {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), "SELECT price FROM items ORDER BY id")
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	prices := make([]int64, 0)
	for rows.Next() {
		var price int64
		c.Assert(rows.Scan(&price), qt.IsNil)
		prices = append(prices, price)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return prices
}

// scriptArgsLoop is the reproduction from stokaro/ptah#4127 with the do body
// reading the cursor as iterator.keyset.cursor, the spelling it has there.
const scriptArgsLoop = `script "loop" "touch" {
  iterator "keyset" {
    cursor {
      id = int
    }
    init {
      sql = "SELECT id FROM items ORDER BY id LIMIT 1"
    }
    next {
      sql  = "SELECT id FROM items WHERE id > ? ORDER BY id LIMIT 1"
      args = [cursor.id]
    }
  }
  do {
    exec "touch" {
      sql  = "UPDATE items SET price = price + 100 WHERE id = ?"
      args = [iterator.keyset.cursor.id]
    }
  }
}
`

// TestCompatScriptLoopBindsTheCursorE2E runs the loop through ptah-compat.
// Each batch is one row, the body updates the row its cursor names, and both
// prices end at 100. Bound as an empty string, the cursor matched no row and
// each batch reported 0 rows affected.
func TestCompatScriptLoopBindsTheCursorE2E(t *testing.T) {
	c := qt.New(t)
	path := scriptArgsDatabase(c)
	script := filepath.Join(c.TempDir(), "script.hcl")
	c.Assert(os.WriteFile(script, []byte(scriptArgsLoop), 0o600), qt.IsNil)

	out, err := runCompatVerb("script", "loop", "--url", atlasurl.SQLiteURLFromPath(path), "--file", script)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "-- ok (")
	c.Assert(out, qt.Contains, "| 1 rows affected")
	c.Assert(out, qt.Contains, "-- 2 batches, 2 rows")
	c.Assert(itemPrices(c, path), qt.DeepEquals, []int64{100, 100})
}

// TestCompatScriptLoopRefusesTheBareCursorInABodyE2E is the spelling the
// issue wrote: in a do body the bare cursor names nothing. The script is
// refused before it connects, and the prices stay where they were.
func TestCompatScriptLoopRefusesTheBareCursorInABodyE2E(t *testing.T) {
	c := qt.New(t)
	path := scriptArgsDatabase(c)
	script := filepath.Join(c.TempDir(), "script.hcl")
	c.Assert(os.WriteFile(script, []byte(`script "loop" "touch" {
  iterator "keyset" {
    cursor {
      id = int
    }
    init {
      sql = "SELECT id FROM items ORDER BY id LIMIT 1"
    }
    next {
      sql  = "SELECT id FROM items WHERE id > ? ORDER BY id LIMIT 1"
      args = [cursor.id]
    }
  }
  do {
    exec "touch" {
      sql  = "UPDATE items SET price = price + 100 WHERE id = ?"
      args = [cursor.id]
    }
  }
}
`), 0o600), qt.IsNil)

	out, err := runCompatVerb("script", "loop", "--url", atlasurl.SQLiteURLFromPath(path), "--file", script)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "args element cursor.id names the cursor the way only the iterator's next query does: "+
		"inside do it is iterator.keyset.cursor.id, the last row of the page")
	c.Assert(itemPrices(c, path), qt.DeepEquals, []int64{0, 0})
}
