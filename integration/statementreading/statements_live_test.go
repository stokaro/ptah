//go:build integration

// Package statementreading_test holds the shared SQL parser and the schema reader to
// what a real server answers: a statement they read is one a server takes, a statement
// they refuse is one it refuses, and a schema read of a script ends where the server's
// catalog ends.
package statementreading_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqllint"
	"ptah.run/internal/sqlschema"
)

// engine is one server, with what it takes and refuses. {s} in a statement is a suffix
// no other run uses.
type engine struct {
	dialect string
	// target names the server, and is unused for SQLite, whose engine is compiled in.
	target dbtarget.Engine
	// table declares a table, with {t} for its name.
	table string
	// views is the query that lists the views whose names end in {s}.
	views string
	// fixture creates what the statements act on, in order.
	fixture []string
	// reads are the statements the server takes, in order, as one session.
	reads []string
	// inTransaction are the statements it takes in a transaction, in order.
	inTransaction []string
	// refuses are statements it answers with a syntax error, or as unimplemented.
	refuses []string
	// cleanup removes what a run left, in order, each a statement that may find
	// nothing.
	cleanup []string
}

const standardViews = "SELECT table_name FROM information_schema.views WHERE table_name LIKE '%_{s}'"

func engines() []engine {
	postgresFixture := []string{
		"CREATE TABLE t_{s} (id INT PRIMARY KEY, a INT)",
		"CREATE TABLE u_{s} (id INT PRIMARY KEY)",
		"CREATE VIEW v1_{s} AS SELECT id FROM t_{s}",
		"CREATE VIEW v2_{s} AS SELECT id FROM t_{s}",
		"CREATE MATERIALIZED VIEW m_{s} AS SELECT id FROM t_{s}",
		"CREATE SEQUENCE q_{s}",
		"CREATE TYPE e_{s} AS ENUM ('a')",
		"CREATE FUNCTION f_{s}() RETURNS INT LANGUAGE SQL AS 'SELECT 1'",
		"CREATE PROCEDURE p_{s}() LANGUAGE SQL AS 'SELECT 1'",
		"CREATE FUNCTION tf_{s}() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
		"CREATE TRIGGER g_{s} BEFORE INSERT ON t_{s} FOR EACH ROW EXECUTE FUNCTION tf_{s}()",
	}
	postgresCleanup := []string{
		"DROP VIEW IF EXISTS v1_{s}", "DROP VIEW IF EXISTS v2_{s}", "DROP MATERIALIZED VIEW IF EXISTS m_{s}",
		"DROP TRIGGER IF EXISTS g_{s} ON t_{s}", "DROP TABLE IF EXISTS t_{s}", "DROP TABLE IF EXISTS u_{s}",
		"DROP SEQUENCE IF EXISTS q_{s}", "DROP TYPE IF EXISTS e_{s}", "DROP FUNCTION IF EXISTS f_{s}()",
		"DROP PROCEDURE IF EXISTS p_{s}()", "DROP FUNCTION IF EXISTS tf_{s}()",
	}
	postgresDrops := []string{
		"DROP VIEW IF EXISTS nope_{s}",
		"DROP MATERIALIZED VIEW m_{s}",
		"DROP TRIGGER IF EXISTS g_{s} ON t_{s}",
		"DROP FUNCTION f_{s}()",
		"DROP PROCEDURE IF EXISTS p_{s}",
		"DROP SEQUENCE q_{s}",
		"DROP TYPE IF EXISTS e_{s}",
		"TRUNCATE t_{s}",
		"TRUNCATE TABLE t_{s}, u_{s}",
	}
	postgresRefuses := []string{
		"DROP VIEW v_{s} SYNC", "DROP PROC p_{s}", "DROP TRIGGER g_{s}", "RENAME TABLE t_{s} TO z_{s}",
		"OPTIMIZE TABLE t_{s}", "REPLACE INTO t_{s} VALUES (1, 2)", "TRUNCATE TABLE t_{s} SYNC",
		"CREATE NONCLUSTERED INDEX i_{s} ON t_{s} (a)", "DROP TABEL t_{s}",
	}
	mysqlFixture := []string{
		"CREATE TABLE t_{s} (id INT PRIMARY KEY, a INT)",
		"CREATE TABLE u_{s} (id INT PRIMARY KEY)",
		"CREATE TABLE w_{s} (id INT PRIMARY KEY)",
		"CREATE VIEW v1_{s} AS SELECT id FROM t_{s}",
		"CREATE VIEW v2_{s} AS SELECT id FROM t_{s}",
		"CREATE TRIGGER g_{s} BEFORE INSERT ON t_{s} FOR EACH ROW SET NEW.a = 1",
		"CREATE FUNCTION f_{s}() RETURNS INT DETERMINISTIC RETURN 1",
		"CREATE PROCEDURE p_{s}() SELECT 1",
	}
	mysqlCleanup := []string{
		"UNLOCK TABLES", "DROP VIEW IF EXISTS v1_{s}, v2_{s}", "DROP TRIGGER IF EXISTS g_{s}",
		"DROP TABLE IF EXISTS t_{s}, u_{s}, w_{s}, z_{s}", "DROP FUNCTION IF EXISTS f_{s}", "DROP PROCEDURE IF EXISTS p_{s}",
	}
	mysqlReads := []string{
		"DROP VIEW v1_{s}, v2_{s}",
		"DROP VIEW IF EXISTS nope_{s}",
		"DROP VIEW IF EXISTS t_{s}",
		"DROP TRIGGER IF EXISTS g_{s}",
		"DROP FUNCTION f_{s}",
		"DROP PROCEDURE IF EXISTS p_{s}",
		"TRUNCATE t_{s}",
		"TRUNCATE TABLE u_{s}",
		"OPTIMIZE TABLE t_{s}, u_{s}",
		"RENAME TABLE w_{s} TO z_{s}",
		"REPLACE INTO t_{s} (id, a) VALUES (1, 2)",
		"LOCK TABLES t_{s} READ, u_{s} AS x_{s} WRITE",
		"UNLOCK TABLES",
	}
	mysqlRefuses := []string{
		"DROP MATERIALIZED VIEW m_{s}", "DROP TRIGGER g_{s} ON t_{s}", "DROP FUNCTION f_{s}()", "DROP TYPE e_{s}",
		"TRUNCATE t_{s}, u_{s}", "TRUNCATE t_{s} CASCADE", "UPSERT INTO t_{s} VALUES (1, 2)", "SAVE TRANSACTION s_{s}",
		"DROP TABEL t_{s}",
	}
	mysqlSavepoints := []string{"SAVEPOINT a_{s}", "RELEASE SAVEPOINT a_{s}"}
	return []engine{
		{
			dialect: platform.Postgres, target: dbtarget.PostgreSQL,
			table:         "CREATE TABLE {t} (id INT PRIMARY KEY)",
			views:         standardViews,
			fixture:       postgresFixture,
			reads:         append([]string{"DROP VIEW v1_{s}, v2_{s} CASCADE"}, postgresDrops...),
			inTransaction: []string{"SAVEPOINT a_{s}", "RELEASE a_{s}", "LOCK TABLE t_{s} IN ACCESS SHARE MODE NOWAIT", "LOCK u_{s}"},
			refuses:       postgresRefuses,
			cleanup:       postgresCleanup,
		},
		{
			dialect: platform.YugabyteDB, target: dbtarget.YugabyteDB,
			table:         "CREATE TABLE {t} (id INT PRIMARY KEY)",
			views:         standardViews,
			fixture:       postgresFixture,
			reads:         append([]string{"DROP VIEW v1_{s} RESTRICT"}, postgresDrops...),
			inTransaction: []string{"SAVEPOINT a_{s}", "RELEASE SAVEPOINT a_{s}", "LOCK TABLE t_{s} IN ACCESS SHARE MODE"},
			refuses:       append([]string{"DROP VIEW v1_{s}, v2_{s}"}, postgresRefuses...),
			cleanup:       postgresCleanup,
		},
		{
			dialect: platform.CockroachDB, target: dbtarget.CockroachDB,
			table:   "CREATE TABLE {t} (id INT PRIMARY KEY)",
			views:   standardViews,
			fixture: postgresFixture,
			reads: []string{
				"DROP VIEW v1_{s}, v2_{s}",
				"DROP VIEW IF EXISTS nope_{s}",
				"DROP MATERIALIZED VIEW m_{s}",
				"DROP TRIGGER g_{s} ON t_{s}",
				"DROP FUNCTION f_{s}()",
				"DROP PROCEDURE IF EXISTS p_{s}",
				"DROP SEQUENCE q_{s}",
				"DROP TYPE e_{s}",
				"TRUNCATE t_{s}, u_{s}",
				"UPSERT INTO u_{s} (id) VALUES (1)",
				"CREATE INDEX i_{s} ON t_{s} (a) STORING (id)",
			},
			inTransaction: []string{"SAVEPOINT a_{s}", "RELEASE a_{s}"},
			refuses:       []string{"LOCK TABLE t_{s}", "TRUNCATE t_{s} RESTART IDENTITY", "DROP FUNCTION f_{s}() CASCADE", "DROP TABEL t_{s}"},
			cleanup:       postgresCleanup,
		},
		{
			dialect: platform.MySQL, target: dbtarget.MySQL,
			table:         "CREATE TABLE {t} (id INT PRIMARY KEY)",
			views:         standardViews,
			fixture:       mysqlFixture,
			reads:         mysqlReads,
			inTransaction: mysqlSavepoints,
			refuses:       append([]string{"LOCK TABLES t_{s} LOW_PRIORITY WRITE", "RELEASE a_{s}"}, mysqlRefuses...),
			cleanup:       mysqlCleanup,
		},
		{
			dialect: platform.MariaDB, target: dbtarget.MariaDB,
			table:   "CREATE TABLE {t} (id INT PRIMARY KEY)",
			views:   standardViews,
			fixture: append([]string{"CREATE SEQUENCE q_{s}"}, mysqlFixture...),
			reads: append([]string{
				"DROP SEQUENCE q_{s}",
				"LOCK TABLES u_{s} LOW_PRIORITY WRITE",
				"UNLOCK TABLES",
			}, mysqlReads...),
			inTransaction: mysqlSavepoints,
			refuses:       append([]string{"RELEASE a_{s}"}, mysqlRefuses...),
			cleanup:       append([]string{"DROP SEQUENCE IF EXISTS q_{s}"}, mysqlCleanup...),
		},
		{
			dialect: platform.ClickHouse, target: dbtarget.ClickHouse,
			table: "CREATE TABLE {t} (id Int32) ENGINE = MergeTree ORDER BY id",
			views: standardViews,
			fixture: []string{
				"CREATE TABLE t_{s} (id Int32) ENGINE = MergeTree ORDER BY id",
				"CREATE TABLE u_{s} (id Int32) ENGINE = MergeTree ORDER BY id",
				"CREATE TABLE w_{s} (id Int32) ENGINE = MergeTree ORDER BY id",
				"CREATE VIEW v1_{s} AS SELECT id FROM t_{s}",
				"CREATE VIEW v2_{s} AS SELECT id FROM t_{s}",
			},
			reads: []string{
				"DROP VIEW v1_{s}, v2_{s}",
				"DROP VIEW IF EXISTS nope_{s} SYNC",
				"TRUNCATE t_{s}",
				"TRUNCATE TABLE IF EXISTS u_{s} SYNC",
				"OPTIMIZE TABLE t_{s} FINAL",
				"RENAME TABLE w_{s} TO z_{s}",
				"DROP TABLE z_{s} SYNC",
			},
			refuses: []string{"DROP MATERIALIZED VIEW m_{s}", "DROP TRIGGER g_{s}", "SAVEPOINT a_{s}", "TRUNCATE t_{s}, u_{s}", "DROP TABEL t_{s}"},
			cleanup: []string{
				"DROP VIEW IF EXISTS v1_{s}", "DROP VIEW IF EXISTS v2_{s}",
				"DROP TABLE IF EXISTS t_{s}", "DROP TABLE IF EXISTS u_{s}", "DROP TABLE IF EXISTS w_{s}", "DROP TABLE IF EXISTS z_{s}",
			},
		},
		{
			dialect: platform.SQLServer, target: dbtarget.SQLServer,
			table: "CREATE TABLE {t} (id INT PRIMARY KEY)",
			views: standardViews,
			fixture: []string{
				"CREATE TABLE t_{s} (id INT PRIMARY KEY, a INT)",
				"CREATE VIEW v1_{s} AS SELECT id FROM t_{s}",
				"CREATE VIEW v2_{s} AS SELECT id FROM t_{s}",
				"CREATE SEQUENCE q_{s}",
				"CREATE TYPE e_{s} FROM INT",
				"CREATE FUNCTION f_{s}() RETURNS INT AS BEGIN RETURN 1; END",
				"CREATE PROCEDURE p_{s} AS SELECT 1",
				"CREATE PROCEDURE r_{s} AS SELECT 1",
				"CREATE TRIGGER g_{s} ON t_{s} AFTER INSERT AS SELECT 1",
			},
			reads: []string{
				"DROP VIEW v1_{s}, v2_{s}",
				"DROP VIEW IF EXISTS nope_{s}",
				"DROP TRIGGER IF EXISTS g_{s}",
				"DROP FUNCTION f_{s}",
				"DROP PROCEDURE p_{s}",
				"DROP PROC r_{s}",
				"DROP SEQUENCE q_{s}",
				"DROP TYPE IF EXISTS e_{s}",
				"TRUNCATE TABLE t_{s}",
				"CREATE NONCLUSTERED INDEX i_{s} ON t_{s} (a)",
				"CREATE UNIQUE NONCLUSTERED INDEX j_{s} ON t_{s} (a)",
			},
			inTransaction: []string{"SAVE TRANSACTION a_{s}", "SAVE TRAN b_{s}"},
			refuses:       []string{"TRUNCATE t_{s}", "DROP VIEW v_{s} CASCADE", "DROP MATERIALIZED VIEW m_{s}", "SAVEPOINT a_{s}", "DROP TABEL t_{s}"},
			cleanup: []string{
				"DROP VIEW IF EXISTS v1_{s}", "DROP VIEW IF EXISTS v2_{s}", "DROP TRIGGER IF EXISTS g_{s}", "DROP TABLE IF EXISTS t_{s}",
				"DROP SEQUENCE IF EXISTS q_{s}", "DROP TYPE IF EXISTS e_{s}", "DROP FUNCTION IF EXISTS f_{s}",
				"DROP PROCEDURE IF EXISTS p_{s}", "DROP PROCEDURE IF EXISTS r_{s}",
			},
		},
		{
			dialect: platform.SQLite,
			table:   "CREATE TABLE {t} (id INT PRIMARY KEY)",
			views:   "SELECT name FROM sqlite_master WHERE type = 'view' AND name LIKE '%_{s}'",
			fixture: []string{
				"CREATE TABLE t_{s} (id INT PRIMARY KEY, a INT)",
				"CREATE VIEW v1_{s} AS SELECT id FROM t_{s}",
				"CREATE VIEW v2_{s} AS SELECT id FROM t_{s}",
				"CREATE TRIGGER g_{s} BEFORE INSERT ON t_{s} BEGIN SELECT 1; END",
			},
			reads: []string{
				"DROP VIEW v1_{s}",
				"DROP VIEW IF EXISTS v2_{s}",
				"DROP TRIGGER g_{s}",
				"REPLACE INTO t_{s} (id, a) VALUES (1, 2)",
			},
			inTransaction: []string{"SAVEPOINT a_{s}", "RELEASE a_{s}", "SAVEPOINT b_{s}", "RELEASE SAVEPOINT b_{s}"},
			refuses:       []string{"TRUNCATE t_{s}", "DROP VIEW v1_{s}, v2_{s}", "DROP VIEW v1_{s} CASCADE", "DROP MATERIALIZED VIEW m_{s}", "DROP TABEL t_{s}"},
		},
		{
			dialect: platform.Spanner, target: dbtarget.Spanner,
			table: "CREATE TABLE {t} (id BIGINT PRIMARY KEY)",
			views: standardViews,
			fixture: []string{
				"CREATE TABLE t_{s} (id BIGINT PRIMARY KEY)",
				"CREATE VIEW v1_{s} AS SELECT id FROM t_{s}",
				"CREATE VIEW v2_{s} AS SELECT id FROM t_{s}",
				"CREATE SEQUENCE q_{s} BIT_REVERSED_POSITIVE",
			},
			reads:   []string{"DROP VIEW v1_{s}", "DROP VIEW IF EXISTS v2_{s}", "DROP SEQUENCE IF EXISTS q_{s}", "TRUNCATE t_{s}"},
			refuses: []string{"DROP VIEW v1_{s}, v2_{s}", "DROP TRIGGER g_{s}", "DROP MATERIALIZED VIEW m_{s}", "DROP TABEL t_{s}"},
			cleanup: []string{"DROP VIEW IF EXISTS v1_{s}", "DROP VIEW IF EXISTS v2_{s}", "DROP SEQUENCE IF EXISTS q_{s}", "DROP TABLE IF EXISTS t_{s}"},
		},
	}
}

// connect opens the server of e. SQLite's is a file of the test's own.
func connect(c *qt.C, e engine) *dbschema.DatabaseConnection {
	c.Helper()
	address := atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "statements.db"))
	if e.dialect != platform.SQLite {
		address = dbtarget.URL(c, e.target)
	}
	conn, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", e.dialect))
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	return conn
}

// suffix is what keeps a run's objects apart from any other run's.
func suffix() string {
	return fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000_000_000)
}

func runCleanup(c *qt.C, conn *dbschema.DatabaseConnection, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := conn.ExecContext(context.Background(), statement)
		c.Check(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

func filled(statements []string, stamp string) []string {
	out := make([]string, 0, len(statements))
	for _, statement := range statements {
		out = append(out, strings.ReplaceAll(statement, "{s}", stamp))
	}
	return out
}

// assertRead holds what a linter says about a statement the parser reads.
func assertRead(c *qt.C, dialect, statement string) {
	c.Helper()
	_, err := parser.NewParser(statement+";", parser.WithDialect(dialect)).Parse()
	c.Assert(err, qt.IsNil, qt.Commentf("%s: %s", dialect, statement))
}

// TestAServerTakesWhatTheParserReads runs each statement on a real server, which takes
// it, and holds the parser to reading it. These are the statements the parser refused
// in the same words as text that is no SQL.
func TestAServerTakesWhatTheParserReads(t *testing.T) {
	for _, e := range engines() {
		t.Run(e.dialect, func(t *testing.T) {
			c := qt.New(t)
			conn := connect(c, e)
			stamp := suffix()
			c.Cleanup(func() { runCleanup(c, conn, filled(e.cleanup, stamp)) })
			for _, statement := range filled(e.fixture, stamp) {
				_, err := conn.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}

			runAsOneSession(c, conn, e.dialect, filled(e.reads, stamp))
			runInATransaction(c, conn, e.dialect, filled(e.inTransaction, stamp))
		})
	}
}

// runAsOneSession holds the parser to reading each statement and the server to taking
// it, on one session, since a lock lasts as long as its session.
func runAsOneSession(c *qt.C, conn *dbschema.DatabaseConnection, dialect string, statements []string) {
	c.Helper()
	err := conn.WithSession(c.Context(), func(session *dbschema.DatabaseConnection) error {
		for _, statement := range statements {
			assertRead(c, dialect, statement)
			if _, err := session.ExecContext(c.Context(), statement); err != nil {
				return fmt.Errorf("%s: %w", statement, err)
			}
		}
		return nil
	})
	c.Assert(err, qt.IsNil)
}

// runInATransaction is [runAsOneSession] for statements a server runs in a transaction.
func runInATransaction(c *qt.C, conn *dbschema.DatabaseConnection, dialect string, statements []string) {
	c.Helper()
	if len(statements) == 0 {
		return
	}
	transaction, err := conn.BeginTx(c.Context(), nil)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(transaction.Rollback(), qt.ErrorIs, sql.ErrTxDone) }()
	for _, statement := range statements {
		assertRead(c, dialect, statement)
		_, err := transaction.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	c.Assert(transaction.Commit(), qt.IsNil)
}

// TestAServerRefusesWhatTheParserRefuses is the direction that keeps a real syntax
// error from passing: each statement draws an answer from the server that no schema
// could change, and the linter reports an error for it.
func TestAServerRefusesWhatTheParserRefuses(t *testing.T) {
	for _, e := range engines() {
		t.Run(e.dialect, func(t *testing.T) {
			c := qt.New(t)
			conn := connect(c, e)
			stamp := suffix()
			runtime := builtintest.Runtime()

			for _, statement := range filled(e.refuses, stamp) {
				comment := qt.Commentf("%s: %s", e.dialect, statement)
				_, serverErr := conn.ExecContext(c.Context(), statement)
				c.Assert(serverErr, qt.IsNotNil, comment)

				_, parseErr := parser.NewParser(statement+";", parser.WithDialect(e.dialect)).Parse()
				c.Assert(parseErr, qt.IsNotNil, comment)

				findings, err := sqllint.LintSource(c.Context(), sqllint.Source{Name: "x.sql", SQL: statement + ";"}, sqllint.Options{
					SQL: runtime.SQL(), Bodies: runtime, Dialect: e.dialect,
				})
				c.Assert(err, qt.IsNil, comment)
				c.Assert(hasError(findings), qt.IsTrue, comment)
			}
		})
	}
}

func hasError(findings []sqllint.Finding) bool {
	return slices.ContainsFunc(findings, func(finding sqllint.Finding) bool { return finding.Severity == sqllint.SeverityError })
}

// viewScripts are what a schema file may do with a view, each ending where the server
// says it ends. {t} is a table, {a} and {b} views, and {c} a name nothing holds.
var viewScripts = []struct {
	name       string
	statements []string
}{
	{name: "a dropped view", statements: []string{"DROP VIEW {a}"}},
	{name: "a view created again", statements: []string{"DROP VIEW {a}", "CREATE VIEW {a} AS SELECT id FROM {t}"}},
	{name: "IF EXISTS of nothing", statements: []string{"DROP VIEW IF EXISTS {c}"}},
	{name: "a name nothing holds", statements: []string{"DROP VIEW {c}"}},
	{name: "a name a table holds", statements: []string{"DROP VIEW {t}"}},
	{name: "a name a table holds, IF EXISTS", statements: []string{"DROP VIEW IF EXISTS {t}"}},
	{name: "a view another view reads", statements: []string{"CREATE VIEW {c} AS SELECT id FROM {a}", "DROP VIEW {a}"}},
}

// TestASchemaReadOfADroppedViewEndsWhereTheServerDoes runs one script on a real server
// and through the schema reader. The server decides: where it refuses a statement the
// reader refuses the script, and where it takes them all the reader ends with the views
// the server's catalog lists, read back.
func TestASchemaReadOfADroppedViewEndsWhereTheServerDoes(t *testing.T) {
	for _, e := range engines() {
		for _, script := range viewScripts {
			t.Run(e.dialect+"/"+script.name, func(t *testing.T) {
				c := qt.New(t)
				conn := connect(c, e)
				stamp := suffix()
				names := strings.NewReplacer("{t}", "t_"+stamp, "{a}", "a_"+stamp, "{b}", "b_"+stamp, "{c}", "c_"+stamp)
				declare := []string{
					names.Replace(e.table),
					names.Replace("CREATE VIEW {a} AS SELECT id FROM {t}"),
					names.Replace("CREATE VIEW {b} AS SELECT id FROM {t}"),
				}
				c.Cleanup(func() {
					runCleanup(c, conn, []string{
						names.Replace("DROP VIEW IF EXISTS {c}"), names.Replace("DROP VIEW IF EXISTS {a}"),
						names.Replace("DROP VIEW IF EXISTS {b}"), names.Replace("DROP TABLE IF EXISTS {t}"),
					})
				})
				for _, statement := range declare {
					_, err := conn.ExecContext(c.Context(), statement)
					c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
				}
				written := make([]string, 0, len(script.statements))
				for _, statement := range script.statements {
					written = append(written, names.Replace(statement))
				}

				want := viewsTheServerEndsWith(c, conn, e, written, stamp)

				got := viewsTheReaderEndsWith(c, e, append(slices.Clone(declare), written...))
				c.Assert(got, qt.DeepEquals, want)
			})
		}
	}
}

// viewsTheServerEndsWith runs the statements and reads the catalog back. It returns nil
// where the server refuses one.
func viewsTheServerEndsWith(c *qt.C, conn *dbschema.DatabaseConnection, e engine, statements []string, stamp string) []string {
	c.Helper()
	for _, statement := range statements {
		if _, err := conn.ExecContext(c.Context(), statement); err != nil {
			return nil
		}
	}
	rows, err := conn.QueryContext(c.Context(), strings.ReplaceAll(e.views, "{s}", stamp))
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	names := make([]string, 0)
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	slices.Sort(names)
	return names
}

// viewsTheReaderEndsWith reads the statements as one schema file. It returns nil where
// the read refuses them.
func viewsTheReaderEndsWith(c *qt.C, e engine, statements []string) []string {
	c.Helper()
	database, _, err := sqlschema.ReadWith(c.Context(), []byte(strings.Join(statements, ";\n")+";"), e.dialect, nil, sqlschema.Options{
		Owners: builtintest.Runtime().SQL(),
	})
	if err != nil {
		return nil
	}
	return viewNames(database)
}

func viewNames(database schemamodel.Database) []string {
	names := make([]string, 0)
	for _, view := range database.Views {
		names = append(names, view.Name)
	}
	slices.Sort(names)
	return names
}
