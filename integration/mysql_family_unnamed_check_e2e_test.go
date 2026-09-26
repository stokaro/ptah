//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// mysqlFamilyEngine is one engine of the MySQL family, reached with an account
// that may create databases, with the query that reads a database's CHECKs
// back as `<table> <name>: <clause>`. MySQL keeps CHECK names per database and
// reports the table through TABLE_CONSTRAINTS; MariaDB keeps them per table and
// reports it in CHECK_CONSTRAINTS itself.
type mysqlFamilyEngine struct {
	name   string
	admin  dbtarget.Engine
	checks string
}

var (
	mysqlCheckEngine = mysqlFamilyEngine{
		name:  "mysql",
		admin: dbtarget.MySQLAdmin,
		checks: `SELECT CONCAT(tc.TABLE_NAME, ' ', tc.CONSTRAINT_NAME, ': ', cc.CHECK_CLAUSE)
			FROM information_schema.TABLE_CONSTRAINTS tc
			JOIN information_schema.CHECK_CONSTRAINTS cc
			  ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
			WHERE tc.CONSTRAINT_SCHEMA = ? AND tc.CONSTRAINT_TYPE = 'CHECK'
			ORDER BY 1`,
	}
	mariaDBCheckEngine = mysqlFamilyEngine{
		name:  "mariadb",
		admin: dbtarget.MariaDBAdmin,
		checks: `SELECT CONCAT(TABLE_NAME, ' ', CONSTRAINT_NAME, ': ', CHECK_CLAUSE)
			FROM information_schema.CHECK_CONSTRAINTS
			WHERE CONSTRAINT_SCHEMA = ?
			ORDER BY 1`,
	}
)

// mysqlFamilyUnnamedChecks are schema files whose CHECKs carry no name. MySQL
// 8.4.11 and 26.7.0 name them `<table>_chk_<n>` in the order the file writes
// them; MariaDB 11.8.9 names a column's CHECK after the column and a table's
// `CONSTRAINT_<n>`. Left unnamed in the model, each compares unequal to the
// database the same file built (stokaro/ptah#3741). Atlas CE v1.3.0 reports
// each synced.
var mysqlFamilyUnnamedChecks = []struct {
	name   string
	engine mysqlFamilyEngine
	schema string
}{
	{
		name:   "mysql: on columns and on tables, in the order written",
		engine: mysqlCheckEngine,
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int CHECK (a > 0), b int, CHECK (b > 0), CHECK (a < b));
CREATE TABLE c2 (CHECK (b > 0), a int CHECK (a > 0), b int);
CREATE TABLE c4 (a int CHECK (a > 0) CHECK (a < 10));
CREATE TABLE c5 (a int, CONSTRAINT CHECK (a > 0), CHECK (a < 9));`,
	},
	{
		name:   "mysql: ALTER TABLE",
		engine: mysqlCheckEngine,
		schema: `CREATE TABLE al (a int, b int);
ALTER TABLE al ADD CHECK (a > 0);
ALTER TABLE al ADD CONSTRAINT al_chk_5 CHECK (a > 1);
ALTER TABLE al ADD CHECK (a > 2);
ALTER TABLE al ADD COLUMN x int CHECK (x > 0), ADD CHECK (b > 0);
ALTER TABLE al ADD CONSTRAINT al_chk_20 CHECK (a > 3), ADD CHECK (a > 4);
ALTER TABLE al DROP CONSTRAINT al_chk_20, ADD CHECK (a > 5);`,
	},
	{
		name:   "mariadb: on columns and on tables",
		engine: mariaDBCheckEngine,
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int CHECK (a > 0), b int, CHECK (b > 0), CHECK (a < b));
CREATE TABLE e2 (a int, CHECK (a < 9), CONSTRAINT CONSTRAINT_1 CHECK (a > 0));
CREATE TABLE c5 (a int, CONSTRAINT CHECK (a > 0), CHECK (a < 9));`,
	},
	{
		name:   "mariadb: ALTER TABLE",
		engine: mariaDBCheckEngine,
		schema: `CREATE TABLE al (a int, b int);
ALTER TABLE al ADD CHECK (a > 0);
ALTER TABLE al ADD CONSTRAINT CONSTRAINT_5 CHECK (a > 1);
ALTER TABLE al ADD CHECK (a > 2);
ALTER TABLE al ADD COLUMN x int CHECK (x > 0), ADD CHECK (b > 0);
ALTER TABLE al ADD CHECK (a > 3), ADD CONSTRAINT CONSTRAINT_4 CHECK (a > 4);
ALTER TABLE al DROP CONSTRAINT CONSTRAINT_1, ADD CHECK (a > 5);`,
	},
}

// checkScratch creates databases on one engine and reads them back.
type checkScratch struct {
	engine   mysqlFamilyEngine
	admin    *sql.DB
	adminDSN string
	adminURL string
}

func newCheckScratch(c *qt.C, engine mysqlFamilyEngine) checkScratch {
	c.Helper()
	adminDSN := dbtarget.DriverDSN(c, engine.admin)
	admin, err := sql.Open("mysql", adminDSN)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	c.Assert(admin.PingContext(c.Context()), qt.IsNil)
	return checkScratch{engine: engine, admin: admin, adminDSN: adminDSN, adminURL: dbtarget.URL(c, engine.admin)}
}

// database creates an empty database, dropped when the test ends, and returns
// its name and the URL ptah connects to it with, in the engine's own scheme.
func (s checkScratch) database(c *qt.C, prefix string) (name, url string) {
	c.Helper()
	name = fmt.Sprintf("ptah_%s_%d", prefix, time.Now().UnixNano())
	createMySQLDatabase(c, c.Context(), s.admin, name)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), s.admin, name) })
	return name, replaceMySQLDatabaseName(c, s.adminURL, name)
}

// builtFrom creates a database and runs the SQL on it as the server receives
// it, with nothing of Ptah's in between.
func (s checkScratch) builtFrom(c *qt.C, prefix, schema string) (name, url string) {
	c.Helper()
	name, url = s.database(c, prefix)
	conn, err := sql.Open("mysql", mySQLDSNForDatabase(c, s.adminDSN, name)+"?multiStatements=true")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	_, err = conn.ExecContext(c.Context(), schema)
	c.Assert(err, qt.IsNil)
	return name, url
}

// checks reads back the CHECKs a database holds.
func (s checkScratch) checks(c *qt.C, database string) []string {
	c.Helper()
	rows, err := s.admin.QueryContext(c.Context(), s.engine.checks, database)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var checks []string
	for rows.Next() {
		var check string
		c.Assert(rows.Scan(&check), qt.IsNil)
		checks = append(checks, check)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return checks
}

// TestMigrateDiffFindsAMySQLFamilyFileWithUnnamedChecksSyncedE2E replays a
// directory whose one migration is the schema file itself and diffs: the
// directory is synced.
func TestMigrateDiffFindsAMySQLFamilyFileWithUnnamedChecksSyncedE2E(t *testing.T) {
	for _, test := range mysqlFamilyUnnamedChecks {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newCheckScratch(c, test.engine)
			dir, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
			_, dev := scratch.database(c, "chk_dev")

			plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", dev, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
			c.Assert(plan, qt.Contains, "The migration directory is synced with the desired state")
		})
	}
}

// TestSchemaApplyFindsAMySQLFamilyFileWithUnnamedChecksSyncedE2E plans the
// native apply of the schema file against the database the same file built:
// nothing is planned.
func TestSchemaApplyFindsAMySQLFamilyFileWithUnnamedChecksSyncedE2E(t *testing.T) {
	for _, test := range mysqlFamilyUnnamedChecks {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newCheckScratch(c, test.engine)
			_, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
			_, target := scratch.builtFrom(c, "chk_built", test.schema)

			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(plan, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyCreatesTheMySQLFamilyChecksTheFileCreatesE2E applies a schema
// file with unnamed CHECKs to a database holding its tables, and reads the
// CHECKs back: they are the ones the file's own SQL creates, name and clause
// alike. The expected set is read from a second database the file's SQL was run
// on, so the assertion is the server's answer. A plan that drops a CHECK the
// file declares fails it.
func TestSchemaApplyCreatesTheMySQLFamilyChecksTheFileCreatesE2E(t *testing.T) {
	tests := []struct {
		name     string
		engine   mysqlFamilyEngine
		existing string
		schema   string
	}{
		{
			name:     "mysql: a table without its CHECKs",
			engine:   mysqlCheckEngine,
			existing: `CREATE TABLE c (id int PRIMARY KEY, b int);`,
			schema:   `CREATE TABLE c (id int PRIMARY KEY, a int CHECK (a > 0), b int, CHECK (b > 0), CHECK (a < b));`,
		},
		{
			name:   "mysql: a table holding the CHECKs under other names",
			engine: mysqlCheckEngine,
			existing: `CREATE TABLE c (id int PRIMARY KEY, a int CONSTRAINT old_a CHECK (a > 0), b int,
  CONSTRAINT old_b CHECK (b > 0), CONSTRAINT old_ab CHECK (a < b));`,
			schema: `CREATE TABLE c (id int PRIMARY KEY, a int CHECK (a > 0), b int, CHECK (b > 0), CHECK (a < b));`,
		},
		{
			name:     "mysql: two CHECKs on one column",
			engine:   mysqlCheckEngine,
			existing: `CREATE TABLE c4 (a int);`,
			schema:   `CREATE TABLE c4 (a int CHECK (a > 0) CHECK (a < 10));`,
		},
		{
			name:     "mariadb: a table without its CHECKs",
			engine:   mariaDBCheckEngine,
			existing: `CREATE TABLE c (id int PRIMARY KEY, b int);`,
			schema:   `CREATE TABLE c (id int PRIMARY KEY, a int CHECK (a > 0), b int, CHECK (b > 0), CHECK (a < b));`,
		},
		{
			name:   "mariadb: a table holding the CHECKs under other names",
			engine: mariaDBCheckEngine,
			existing: `CREATE TABLE c (id int PRIMARY KEY, a int, b int,
  CONSTRAINT old_a CHECK (a > 0), CONSTRAINT old_b CHECK (b > 0), CONSTRAINT old_ab CHECK (a < b));`,
			schema: `CREATE TABLE c (id int PRIMARY KEY, a int CHECK (a > 0), b int, CHECK (b > 0), CHECK (a < b));`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newCheckScratch(c, test.engine)
			builtName, _ := scratch.builtFrom(c, "chk_want", test.schema)
			want := scratch.checks(c, builtName)
			targetName, target := scratch.builtFrom(c, "chk_target", test.existing)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.schema+"\n"), 0o600), qt.IsNil)

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(scratch.checks(c, targetName), qt.DeepEquals, want)
			out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
			c.Assert(out, qt.Contains, "Schema is synced")
		})
	}
}
