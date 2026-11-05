//go:build integration

package integration_test

import (
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/dbtarget"
)

// engineGuardsCase is one engine: a target to apply the files to, a table and a
// view the second step removes and an index it changes, and the query that
// counts the removed objects.
type engineGuardsCase struct {
	name    string
	dialect string
	target  func(c *qt.C) string
	before  string
	after   string
	kept    string
}

var engineGuardsCases = []engineGuardsCase{
	{
		name:    "postgres",
		dialect: platform.Postgres,
		target:  func(c *qt.C) string { return pinnedDevURL(c, postgresScratchDevURL(c, "ptah_guards"), "public") },
		before: "CREATE TABLE gone (id bigint PRIMARY KEY);\nCREATE TABLE t (id bigint PRIMARY KEY, a int, b int, CONSTRAINT uq UNIQUE (a, b));\n" +
			"CREATE VIEW v AS SELECT id FROM t;\n",
		after: "CREATE TABLE t (id bigint PRIMARY KEY, a int);\n",
		kept:  "SELECT count(*) FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relname IN ('gone', 'v', 'uq')",
	},
	{
		name:    "mysql",
		dialect: platform.MySQL,
		target: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin).database(c, "guards")
			return url
		},
		before: "CREATE TABLE gone (id bigint PRIMARY KEY);\nCREATE TABLE t (id bigint PRIMARY KEY, a int);\n" +
			"CREATE INDEX i ON t (a);\nCREATE VIEW v AS SELECT id FROM t;\n",
		after: "CREATE TABLE t (id bigint PRIMARY KEY, a int);\n",
		kept:  "SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN ('gone', 'v')",
	},
	{
		name:    "mariadb",
		dialect: platform.MariaDB,
		target: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin).database(c, "guards")
			return url
		},
		before: "CREATE TABLE gone (id bigint PRIMARY KEY);\nCREATE TABLE t (id bigint PRIMARY KEY, a int);\n" +
			"CREATE INDEX i ON t (a);\nCREATE VIEW v AS SELECT id FROM t;\n",
		after: "CREATE TABLE t (id bigint PRIMARY KEY, a int);\n",
		kept:  "SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN ('gone', 'v')",
	},
	{
		name:    "sqlite",
		dialect: platform.SQLite,
		target:  sqliteScratchURL,
		before: "CREATE TABLE gone (id integer PRIMARY KEY);\nCREATE TABLE t (id integer PRIMARY KEY, a int);\n" +
			"CREATE INDEX i ON t (a);\nCREATE VIEW v AS SELECT id FROM t;\n",
		after: "CREATE TABLE t (id integer PRIMARY KEY, a int);\n",
		kept:  "SELECT count(*) FROM sqlite_master WHERE name IN ('gone', 'v', 'i')",
	},
	{
		name:    "sqlserver",
		dialect: platform.SQLServer,
		target: func(c *qt.C) string {
			return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
				"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
		},
		before: "CREATE TABLE gone (id bigint PRIMARY KEY);\nCREATE TABLE t (id bigint PRIMARY KEY, a int);\n" +
			"CREATE INDEX i ON t (a);\n",
		after: "CREATE TABLE t (id bigint PRIMARY KEY, a int);\n",
		kept:  "SELECT count(*) FROM sys.objects WHERE name = 'gone'",
	},
	{
		name:    "clickhouse",
		dialect: platform.ClickHouse,
		target:  clickHouseScratchURL,
		before: "CREATE TABLE gone (id UInt64) ENGINE = MergeTree ORDER BY id;\nCREATE TABLE t (id UInt64, a UInt32) ENGINE = MergeTree ORDER BY id;\n" +
			"CREATE VIEW v AS SELECT id FROM t;\n",
		after: "CREATE TABLE t (id UInt64, a UInt32) ENGINE = MergeTree ORDER BY id;\n",
		kept:  "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN ('gone', 'v')",
	},
}

// existenceGuard finds a guard the plan writes on a statement.
var existenceGuard = regexp.MustCompile(`IF (NOT )?EXISTS`)

// TestCompatMigrateDiffEngineGuardsE2E_ADropStopsOnAnotherState diffs a removal
// on each engine and holds both of its files to no existence guard. Applied to
// the database it was planned for, the file removes the objects and the rollback
// puts them back. Applied once more, the second run stops on the first drop,
// where a guard would make the statement do nothing and the file report success.
func TestCompatMigrateDiffEngineGuardsE2E_ADropStopsOnAnotherState(t *testing.T) {
	for _, test := range engineGuardsCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			pairs := readMigrationPairs(c, diffEngineSteps(c, "", test.target(c), []string{test.before, test.after}))
			target := test.target(c)

			c.Assert(pairs, qt.HasLen, 2)
			c.Assert(existenceGuard.FindString(pairs[1].Up), qt.Equals, "", qt.Commentf("%s", pairs[1].Up))
			c.Assert(existenceGuard.FindString(pairs[0].Down), qt.Equals, "", qt.Commentf("%s", pairs[0].Down))
			applyEngineFile(c, target, test.dialect, pairs[0].Up)
			applyEngineFile(c, target, test.dialect, pairs[1].Up)
			c.Assert(heldColumns(c, target, test.kept), qt.Equals, "0")
			c.Assert(firstStatementError(c, target, test.dialect, pairs[1].Up), qt.IsNotNil)
		})
	}
}

// firstStatementError runs the statements of a file on target and returns the
// first error.
func firstStatementError(c *qt.C, targetURL, dialect, file string) error {
	c.Helper()
	conn := connectDevDialect(c, targetURL)
	for _, statement := range sqlutil.SplitSQLStatementsForDialect(file, dialect) {
		if strings.TrimSpace(sqlutil.StripCommentsForDialect(statement, dialect)) == "" {
			continue
		}
		if _, err := conn.ExecContext(c.Context(), statement); err != nil {
			return err
		}
	}
	return nil
}
