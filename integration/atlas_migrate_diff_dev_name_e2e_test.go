//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/dbtarget"
)

// devNameCase is one engine whose reader records the database a view lives in.
type devNameCase struct {
	name    string
	dialect string
	scratch func(c *qt.C) string
	schemas []string
	views   string
}

var devNameCases = []devNameCase{
	{
		name:    "mysql",
		dialect: platform.MySQL,
		scratch: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin).database(c, "devname")
			return url
		},
		schemas: []string{
			"CREATE TABLE t (id bigint PRIMARY KEY, a int);\nCREATE VIEW v AS SELECT id, a FROM t;\n",
			"CREATE TABLE t (id bigint PRIMARY KEY, a int, b int);\nCREATE VIEW v AS SELECT id, a, b FROM t;\n",
			"CREATE TABLE t (id bigint PRIMARY KEY, a int, b int);\n",
		},
		views: "SELECT count(*) FROM information_schema.views WHERE table_schema = DATABASE()",
	},
	{
		name:    "mariadb",
		dialect: platform.MariaDB,
		scratch: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin).database(c, "devname")
			return url
		},
		schemas: []string{
			"CREATE TABLE t (id bigint PRIMARY KEY, a int);\nCREATE VIEW v AS SELECT id, a FROM t;\n",
			"CREATE TABLE t (id bigint PRIMARY KEY, a int, b int);\nCREATE VIEW v AS SELECT id, a, b FROM t;\n",
			"CREATE TABLE t (id bigint PRIMARY KEY, a int, b int);\n",
		},
		views: "SELECT count(*) FROM information_schema.views WHERE table_schema = DATABASE()",
	},
	{
		name:    "clickhouse",
		dialect: platform.ClickHouse,
		scratch: clickHouseScratchURL,
		schemas: []string{
			"CREATE TABLE t (id UInt64, a UInt32) ENGINE = MergeTree ORDER BY id;\nCREATE VIEW v AS SELECT id, a FROM t;\n",
			"CREATE TABLE t (id UInt64, a UInt32, b UInt32) ENGINE = MergeTree ORDER BY id;\nCREATE VIEW v AS SELECT id, a, b FROM t;\n",
			"CREATE TABLE t (id UInt64, a UInt32, b UInt32) ENGINE = MergeTree ORDER BY id;\n",
		},
		views: "SELECT count() FROM system.tables WHERE database = currentDatabase() AND engine = 'View'",
	},
}

// TestCompatMigrateDiffE2E_AFileNamesNoDevDatabase diffs a view through its life
// against one database and runs both files of each step on another. A file that
// names the dev database stops there, at DROP VIEW `dev`.`v`, or with a guard
// does nothing and leaves the view behind. The rollback reads a body the server
// printed with the dev database in front of every relation, so it is held to the
// same.
func TestCompatMigrateDiffE2E_AFileNamesNoDevDatabase(t *testing.T) {
	for _, test := range devNameCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := test.scratch(c)
			database := connectDevDialect(c, dev).Info().Schema
			c.Assert(database, qt.Not(qt.Equals), "")

			pairs := readMigrationPairs(c, diffDevNameSteps(c, dev, test.schemas))
			target := test.scratch(c)

			c.Assert(pairs, qt.HasLen, len(test.schemas))
			for _, pair := range pairs {
				c.Assert(pair.Up, qt.Not(qt.Contains), database)
				c.Assert(pair.Down, qt.Not(qt.Contains), database)
				applyDevNameFile(c, target, test.dialect, pair.Up)
			}
			c.Assert(devNameCount(c, target, test.views), qt.Equals, "0")
			applyDevNameFile(c, target, test.dialect, pairs[2].Down)
			c.Assert(devNameCount(c, target, test.views), qt.Equals, "1")
			applyDevNameFile(c, target, test.dialect, pairs[1].Down)
			c.Assert(devNameCount(c, target, test.views), qt.Equals, "1")
		})
	}
}

func diffDevNameSteps(c *qt.C, dev string, schemas []string) string {
	c.Helper()
	work := c.TempDir()
	dir := filepath.Join(work, "migrations")
	for i, schema := range schemas {
		name := "s" + string(rune('1'+i))
		path := filepath.Join(work, name+".sql")
		c.Assert(os.WriteFile(path, []byte(schema), 0o600), qt.IsNil)
		output, err := runCompatVerb("migrate", "diff", name,
			"--dir", "file://"+filepath.ToSlash(dir)+"?format=golang-migrate",
			"--to", "file://"+filepath.ToSlash(path),
			"--dev-url", dev,
			"--format", `{{ sql . "  " }}`)
		c.Assert(err, qt.IsNil, qt.Commentf("step %s:\n%s", name, output))
	}
	return dir
}

func applyDevNameFile(c *qt.C, targetURL, dialect, file string) {
	c.Helper()
	conn := connectDevDialect(c, targetURL)
	for _, statement := range sqlutil.SplitSQLStatementsForDialect(file, dialect) {
		if strings.TrimSpace(sqlutil.StripCommentsForDialect(statement, dialect)) == "" {
			continue
		}
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

func devNameCount(c *qt.C, targetURL, query string) string {
	c.Helper()
	conn := connectDevDialect(c, targetURL)
	var count string
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&count), qt.IsNil)
	return count
}
