//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// viewOrderEngine is a server whose CREATE VIEW checks the relations the
// query reads, so a view created before a view it reads is refused.
type viewOrderEngine struct {
	name string
	// url creates an empty database, or an account on Oracle, and returns
	// the URL ptah connects to it with.
	url func(c *qt.C) string
	// number is the column type the schema files declare.
	number string
	// viewCount counts the views the database holds.
	viewCount string
}

var viewOrderEngines = []viewOrderEngine{
	{
		name: "MySQL",
		url: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin).database(c, "view_order")
			return url
		},
		number:    "int",
		viewCount: "SELECT COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA = DATABASE()",
	},
	{
		name: "MariaDB",
		url: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin).database(c, "view_order")
			return url
		},
		number:    "int",
		viewCount: "SELECT COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA = DATABASE()",
	},
	{
		name: "SQL Server",
		url: func(c *qt.C) string {
			return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
				"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
		},
		number:    "int",
		viewCount: "SELECT COUNT(*) FROM sys.views",
	},
	{
		name:      "Oracle",
		url:       oracleScratchURL,
		number:    "NUMBER(10)",
		viewCount: "SELECT COUNT(*) FROM user_views",
	},
}

// writeViewOrderSchema writes schema to a file and returns its path.
func writeViewOrderSchema(c *qt.C, schema string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(schema), 0o600), qt.IsNil)
	return path
}

// TestViewReadingAViewNamedAfterItAppliesE2E applies three views, each
// reading the one whose name sorts after it, to an empty database. Each view
// is created after the view it reads, the server accepts all three, the next
// run plans nothing, and the first view reads the table through the other
// two. Created in name order, the first CREATE VIEW names a relation that
// does not exist yet.
func TestViewReadingAViewNamedAfterItAppliesE2E(t *testing.T) {
	for _, engine := range viewOrderEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			database := engine.url(c)
			schema := writeViewOrderSchema(c, fmt.Sprintf("CREATE TABLE t (n %s);\n"+
				"CREATE VIEW a_v AS SELECT n FROM b_v;\n"+
				"CREATE VIEW b_v AS SELECT n FROM c_v;\n"+
				"CREATE VIEW c_v AS SELECT n FROM t;\n", engine.number))

			applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")
			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))

			planned := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")
			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Contains, "Schema is synced, no changes to be made.", historyComment(planned))

			conn := connectDevDialect(c, database)
			_, err := conn.ExecContext(c.Context(), "INSERT INTO t (n) VALUES (7)")
			c.Assert(err, qt.IsNil)
			var read int
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT n FROM a_v").Scan(&read), qt.IsNil)
			c.Assert(read, qt.Equals, 7)
		})
	}
}

// TestViewCycleIsRefusedByNameE2E declares two views that read each other.
// No order creates them, so the run names both and changes nothing.
func TestViewCycleIsRefusedByNameE2E(t *testing.T) {
	for _, engine := range viewOrderEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			database := engine.url(c)
			schema := writeViewOrderSchema(c, fmt.Sprintf("CREATE TABLE t (n %s);\n"+
				"CREATE VIEW a_v AS SELECT n FROM b_v;\n"+
				"CREATE VIEW b_v AS SELECT n FROM a_v;\n", engine.number))

			applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains,
				"the views a_v, b_v read one another in a cycle, so none of them can be created first: a_v reads b_v reads a_v",
				historyComment(applied))
			var views int
			c.Assert(connectDevDialect(c, database).QueryRowContext(c.Context(), engine.viewCount).Scan(&views), qt.IsNil)
			c.Assert(views, qt.Equals, 0)
		})
	}
}

// TestViewsWhoseNamesReadNoViewApplyE2E declares two views whose aliases, or
// whose common table expressions, carry the name of the other view. Neither
// view reads the other, so the server creates both in any order, and the run
// applies them. Read as references, the names close a ring, and the run
// refuses the schema as a cycle. The next run is not planned here: MariaDB
// stores a common table expression in a form its comparison does not read
// back, which has nothing to do with the order.
func TestViewsWhoseNamesReadNoViewApplyE2E(t *testing.T) {
	shapes := []struct {
		name string
		a, b string
	}{
		{name: "column aliases", a: "SELECT n AS b_v FROM t", b: "SELECT n AS a_v FROM t"},
		{name: "table aliases", a: "SELECT b_v.n FROM t b_v", b: "SELECT a_v.n FROM t a_v"},
		{
			name: "common table expressions",
			a:    "WITH b_v AS (SELECT n FROM t) SELECT n FROM b_v",
			b:    "WITH a_v AS (SELECT n FROM t) SELECT n FROM a_v",
		},
	}
	for _, engine := range viewOrderEngines {
		for _, shape := range shapes {
			t.Run(engine.name+"/"+shape.name, func(t *testing.T) {
				c := qt.New(t)
				database := engine.url(c)
				schema := writeViewOrderSchema(c, fmt.Sprintf("CREATE TABLE t (n %s);\n"+
					"CREATE VIEW a_v AS %s;\n"+
					"CREATE VIEW b_v AS %s;\n", engine.number, shape.a, shape.b))

				applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")
				c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))

				var views int
				c.Assert(connectDevDialect(c, database).QueryRowContext(c.Context(), engine.viewCount).Scan(&views), qt.IsNil)
				c.Assert(views, qt.Equals, 2)
			})
		}
	}
}

// TestViewReadingAViewAfterADoubleQuotedStringAppliesE2E puts a double-quoted
// string holding an escaped quote before the view the body reads. MySQL and
// MariaDB read "x\"y" as the string x"y, so the name after it is a relation
// the view reads, and the view is created after it. Read as a quoted name,
// which takes no backslash, the string closes early, the rest of the body is
// a name that never closes, and the view is created first.
func TestViewReadingAViewAfterADoubleQuotedStringAppliesE2E(t *testing.T) {
	for _, engine := range viewOrderEngines[:2] {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			database := engine.url(c)
			schema := writeViewOrderSchema(c, "CREATE TABLE t (n int);\n"+
				`CREATE VIEW a_v AS SELECT "x\"y" AS s, n FROM b_v;`+"\n"+
				"CREATE VIEW b_v AS SELECT n FROM t;\n")

			applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")
			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))

			conn := connectDevDialect(c, database)
			_, err := conn.ExecContext(c.Context(), "INSERT INTO t (n) VALUES (7)")
			c.Assert(err, qt.IsNil)
			var read string
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT s FROM a_v").Scan(&read), qt.IsNil)
			c.Assert(read, qt.Equals, `x"y`)
		})
	}
}
