package atlasscript_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasscript"
)

// openWith opens an in-memory SQLite database and runs setup in it.
func openWith(c *qt.C, setup ...string) *sql.DB {
	c.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	c.Assert(err, qt.IsNil)
	// One connection: an in-memory database is per connection, and the
	// script's transaction must see the rows setup wrote.
	db.SetMaxOpenConns(1)
	c.Cleanup(func() { _ = db.Close() })
	for _, statement := range setup {
		_, err := db.Exec(statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return db
}

// column reads one column of a query as int64s, in order.
func column(c *qt.C, db *sql.DB, query string) []int64 {
	c.Helper()
	rows, err := db.Query(query)
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	values := make([]int64, 0)
	for rows.Next() {
		var value int64
		c.Assert(rows.Scan(&value), qt.IsNil)
		values = append(values, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return values
}

// texts reads one row of a query as strings.
func texts(c *qt.C, db *sql.DB, query string, count int) []string {
	c.Helper()
	values := make([]string, count)
	holders := make([]any, count)
	for index := range values {
		holders[index] = &values[index]
	}
	c.Assert(db.QueryRow(query).Scan(holders...), qt.IsNil)
	return values
}

// The reproduction from stokaro/ptah#4127, in the spelling a do body reads the
// cursor with. Each batch is one row, so the cursor is that row, and each
// UPDATE changes it.
func TestRunLoop_ABodyReadsTheCursorOfItsPage(t *testing.T) {
	c := qt.New(t)
	db := openWith(c,
		"CREATE TABLE items (id INTEGER PRIMARY KEY, price INTEGER NOT NULL)",
		"INSERT INTO items VALUES (1, 0), (2, 0)",
	)
	scripts := parse(c, `
script "loop" "touch" {
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
      sql         = "UPDATE items SET price = price + 100 WHERE id = ?"
      args        = [iterator.keyset.cursor.id]
      expect_rows = 1
    }
  }
}
`)

	outcome, err := atlasscript.RunLoop(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.IsNil)
	c.Assert(outcome.Batches, qt.Equals, 2)
	c.Assert(column(c, db, "SELECT price FROM items ORDER BY id"), qt.DeepEquals, []int64{100, 100})
}

// A do body reads the whole page through iterator.keyset.batch, counts it,
// and knows which iteration it is on.
func TestRunLoop_ABodyReadsItsPage(t *testing.T) {
	c := qt.New(t)
	db := openWith(c,
		"CREATE TABLE items (id INTEGER PRIMARY KEY, done INTEGER NOT NULL)",
		"INSERT INTO items VALUES (1, 0), (2, 0), (3, 0), (4, 0), (5, 0)",
		"CREATE TABLE audit (batch INTEGER, size INTEGER)",
	)
	scripts := parse(c, `
script "loop" "mark" {
  iterator "keyset" {
    cursor {
      id = int
    }
    batch {
      id = int
    }
    init {
      sql = "SELECT id FROM items ORDER BY id LIMIT 2"
    }
    next {
      sql  = "SELECT id FROM items WHERE id > ? ORDER BY id LIMIT 2"
      args = [cursor.id]
    }
  }
  do {
    exec "mark" {
      sql  = "UPDATE items SET done = 1 WHERE id IN (SELECT value FROM json_each(?))"
      args = [jsonencode(iterator.keyset.batch[*].id)]
    }
    exec "audit" {
      sql  = "INSERT INTO audit (batch, size) VALUES (?, ?)"
      args = [self.index, length(iterator.keyset.batch)]
    }
  }
}
`)

	outcome, err := atlasscript.RunLoop(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.IsNil)
	c.Assert(outcome.Batches, qt.Equals, 3)
	c.Assert(column(c, db, "SELECT done FROM items ORDER BY id"), qt.DeepEquals, []int64{1, 1, 1, 1, 1})
	c.Assert(column(c, db, "SELECT batch FROM audit ORDER BY batch"), qt.DeepEquals, []int64{0, 1, 2})
	c.Assert(column(c, db, "SELECT size FROM audit ORDER BY batch"), qt.DeepEquals, []int64{2, 2, 1})
}

// The next query binds its args in the order written, each naming its column.
// The cursor is declared a, b and the seek is on (b, a): binding the cursor row
// in declaration order would seek past (5, 2) rather than (2, 5) and walk the
// first row again.
func TestRunLoop_TheNextQueryBindsItsArgsInTheOrderWritten(t *testing.T) {
	c := qt.New(t)
	db := openWith(c,
		"CREATE TABLE pairs (a INTEGER NOT NULL, b INTEGER NOT NULL)",
		"INSERT INTO pairs VALUES (1, 10), (2, 5), (3, 7)",
		"CREATE TABLE seen (step INTEGER PRIMARY KEY AUTOINCREMENT, a INTEGER)",
	)
	scripts := parse(c, `
script "loop" "walk" {
  iterator "keyset" {
    cursor {
      a = int
      b = int
    }
    init {
      sql = "SELECT a, b FROM pairs ORDER BY b, a LIMIT 1"
    }
    next {
      sql  = "SELECT a, b FROM pairs WHERE (b, a) > (?, ?) ORDER BY b, a LIMIT 1"
      args = [cursor.b, cursor.a]
    }
  }
  do {
    exec "record" {
      sql  = "INSERT INTO seen (a) VALUES (?)"
      args = [iterator.keyset.cursor.a]
    }
  }
}
`)

	outcome, err := atlasscript.RunLoop(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock(), MaxBatches: 10})

	c.Assert(err, qt.IsNil)
	c.Assert(outcome.Batches, qt.Equals, 3)
	c.Assert(column(c, db, "SELECT a FROM seen ORDER BY step"), qt.DeepEquals, []int64{2, 3, 1})
}

// The cursor takes its column by name. The query selects name before id, and
// the cursor declares id alone: the seek has to read id, wherever it sits.
func TestRunLoop_TheCursorReadsItsColumnByName(t *testing.T) {
	c := qt.New(t)
	db := openWith(c,
		"CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
		"INSERT INTO items VALUES (1, 'c'), (2, 'b'), (3, 'a')",
		"CREATE TABLE seen (step INTEGER PRIMARY KEY AUTOINCREMENT, id INTEGER)",
	)
	scripts := parse(c, `
script "loop" "walk" {
  iterator "keyset" {
    cursor {
      id = int
    }
    init {
      sql = "SELECT name, id FROM items ORDER BY id LIMIT 1"
    }
    next {
      sql  = "SELECT name, id FROM items WHERE id > ? ORDER BY id LIMIT 1"
      args = [cursor.id]
    }
  }
  do {
    exec "record" {
      sql  = "INSERT INTO seen (id) VALUES (?)"
      args = [iterator.keyset.cursor.id]
    }
  }
}
`)

	outcome, err := atlasscript.RunLoop(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock(), MaxBatches: 10})

	c.Assert(err, qt.IsNil)
	c.Assert(outcome.Batches, qt.Equals, 3)
	c.Assert(column(c, db, "SELECT id FROM seen ORDER BY step"), qt.DeepEquals, []int64{1, 2, 3})
}

// A constant binds as its own type. Columns with no declared type keep what
// was bound, so typeof reads back what the driver received: the text "1" is
// the defect an engine that types its parameters refuses.
func TestRunExec_AConstantBindsAsItsOwnType(t *testing.T) {
	c := qt.New(t)
	db := openWith(c, "CREATE TABLE typed (a, b, c, d)")
	scripts := parse(c, `
script "exec" "typed" {
  exec "insert" {
    sql  = "INSERT INTO typed (a, b, c, d) VALUES (?, ?, ?, ?)"
    args = [1, 1.5, "x", true]
  }
}
`)

	_, err := atlasscript.RunExec(context.Background(), db, scripts[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.IsNil)
	c.Assert(texts(c, db, "SELECT typeof(a), typeof(b), typeof(c), typeof(d) FROM typed", 4),
		qt.DeepEquals, []string{"integer", "real", "text", "integer"})
}

// guardedScript updates row 2 when a condition finds the row id names.
func guardedScript(id string) string {
	return `
script "exec" "guarded" {
  condition "has" {
    sql  = "SELECT count(*) > 0 FROM items WHERE id = ?"
    args = [` + id + `]
  }
  exec "set" {
    sql = "UPDATE items SET price = 7 WHERE id = 2"
  }
}
`
}

// itemsAtZero is two items priced 0.
func itemsAtZero(c *qt.C) *sql.DB {
	c.Helper()
	return openWith(c,
		"CREATE TABLE items (id INTEGER PRIMARY KEY, price INTEGER NOT NULL)",
		"INSERT INTO items VALUES (1, 0), (2, 0)",
	)
}

// A condition binds its args too: the guard holds for a row that exists.
func TestRunExec_AConditionBindsItsArgs_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := itemsAtZero(c)

	_, err := atlasscript.RunExec(context.Background(), db, parse(c, guardedScript("2"))[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.IsNil)
	c.Assert(column(c, db, "SELECT price FROM items ORDER BY id"), qt.DeepEquals, []int64{0, 7})
}

// The same guard stops the script for a row that does not exist.
func TestRunExec_AConditionBindsItsArgs_FailurePath(t *testing.T) {
	c := qt.New(t)
	db := itemsAtZero(c)

	_, err := atlasscript.RunExec(context.Background(), db, parse(c, guardedScript("9"))[0],
		atlasscript.RunOptions{Now: fixedClock()})

	c.Assert(err, qt.ErrorIs, atlasscript.ErrConditionFalse)
	c.Assert(column(c, db, "SELECT price FROM items ORDER BY id"), qt.DeepEquals, []int64{0, 0})
}

// A page that does not carry a declared column, or carries it as another
// type, stops the loop naming the column rather than binding a guess.
func TestRunLoop_APageThatDoesNotMatchTheCursor_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "the column is missing",
			query: "SELECT name FROM items ORDER BY id LIMIT 1",
			want:  `loop "walk" iterator cursor: column "id" not found in query result`,
		},
		{
			name:  "the column is not a number",
			query: "SELECT name AS id FROM items ORDER BY name LIMIT 1",
			want:  `loop "walk" iterator cursor: column "id" is declared int: "a" is not a number`,
		},
		{
			name:  "the column is not a whole number",
			query: "SELECT 1.5 AS id",
			want:  `loop "walk" iterator cursor: column "id" is declared int: 1\.5 is not a whole number`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := openWith(c,
				"CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
				"INSERT INTO items VALUES (1, 'a')",
			)
			scripts := parse(c, `
script "loop" "walk" {
  iterator "keyset" {
    cursor {
      id = int
    }
    init {
      sql = "`+test.query+`"
    }
    next {
      sql  = "SELECT id FROM items WHERE id > ? ORDER BY id LIMIT 1"
      args = [cursor.id]
    }
  }
  do {
    exec "touch" {
      sql  = "UPDATE items SET name = name WHERE id = ?"
      args = [iterator.keyset.cursor.id]
    }
  }
}
`)

			outcome, err := atlasscript.RunLoop(context.Background(), db, scripts[0],
				atlasscript.RunOptions{Now: fixedClock()})

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(outcome.Steps, qt.HasLen, 0)
		})
	}
}
