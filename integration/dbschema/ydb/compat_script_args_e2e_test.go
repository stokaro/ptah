//go:build integration

package ydb_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// scriptArgsTable is the table the script walks, at the database root.
const scriptArgsTable = "ptah_ydb_script_args"

// scriptArgsLoop is the loop from stokaro/ptah#4127 in the spelling a do body
// reads the cursor with. YDB types its parameters, so it measures what a
// permissive engine cannot: the literal 100 has to reach the server as an
// Int64 to add to an Int64 column, and the cursor's id has to reach it as one
// to match a key. A script writes its engine's own placeholder, and on YDB the
// connection binds the first argument to $p1.
const scriptArgsLoop = `script "loop" "touch" {
  iterator "keyset" {
    cursor {
      id = int
    }
    init {
      sql = "SELECT id FROM ` + "`" + scriptArgsTable + "`" + ` ORDER BY id LIMIT 1"
    }
    next {
      sql  = "SELECT id FROM ` + "`" + scriptArgsTable + "`" + ` WHERE id > $p1 ORDER BY id LIMIT 1"
      args = [cursor.id]
    }
  }
  do {
    exec "touch" {
      sql  = "UPDATE ` + "`" + scriptArgsTable + "`" + ` SET price = price + $p1 WHERE id = $p2"
      args = [100, iterator.keyset.cursor.id]
    }
  }
}
`

// prices reads the table's prices in key order.
func prices(c *qt.C, conn *dbschema.DatabaseConnection) []int64 {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), "SELECT price FROM `"+scriptArgsTable+"` ORDER BY id")
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

// TestYDBCompatBinary_ScriptBindsTypedArgs runs a loop script through
// ptah-compat on each certified line. Every batch adds 100 to the row its
// cursor names, so both rows end at 100; bound as text, the constant is a type
// error on the server and the cursor matches no key.
func TestYDBCompatBinary_ScriptBindsTypedArgs(t *testing.T) {
	c := qt.New(t)
	binary := buildCompatBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			drop := func() {
				c.Assert(conn.Writer().ExecuteSQL(context.Background(),
					"DROP TABLE IF EXISTS `"+scriptArgsTable+"`"), qt.IsNil)
			}
			drop()
			c.Cleanup(drop)
			c.Assert(conn.Writer().ExecuteSQL(ctx, "CREATE TABLE `"+scriptArgsTable+
				"` (id Int64 NOT NULL, price Int64 NOT NULL, PRIMARY KEY (id))"), qt.IsNil)
			c.Assert(conn.Writer().ExecuteSQL(ctx, "UPSERT INTO `"+scriptArgsTable+
				"` (id, price) VALUES (1l, 0l), (2l, 0l)"), qt.IsNil)
			script := writeCompatFile(c, c.TempDir(), "script.hcl", scriptArgsLoop)

			ran, report, err := runCompat(ctx, binary, "script", "loop", "--url", url, "--file", script)

			c.Assert(err, qt.IsNil, qt.Commentf("script loop:\n%s\n%s", ran, report))
			c.Assert(report, qt.Contains, "-- 2 batches, 2 rows")
			c.Assert(prices(c, conn), qt.DeepEquals, []int64{100, 100})
		})
	}
}
