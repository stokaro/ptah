//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// mysqlUnnamedForeignKeys are MySQL schema files whose foreign keys the author
// left unnamed, with the keys and indexes of `c` MySQL 8.4.11 built from each,
// read back from information_schema. The server names such a key
// `<table>_ibfk_<n>` and its index after the key's first column. Read with
// Ptah's own `fk_<table>_<columns>` instead, the file plans a rename and a
// DROP INDEX against the database it built, and applying that plan fails
// halfway (stokaro/ptah#3725). Atlas CE v1.3.0 reports each row synced.
//
// A key written `CONSTRAINT FOREIGN KEY`, with the keyword and no symbol, is
// unnamed too, and the server numbers it in the same sequence as the keys
// written without the keyword (stokaro/ptah#3730).
var mysqlUnnamedForeignKeys = []struct {
	name     string
	schema   string
	wantKeys string
	wantIdx  string
}{
	{
		name: "one unnamed key",
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int NOT NULL, FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE);`,
		wantKeys: "c_ibfk_1(p_id)",
		wantIdx:  "p_id(p_id)",
	},
	{
		name: "two unnamed keys",
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, x int, y int, FOREIGN KEY (y) REFERENCES p(id), FOREIGN KEY (x) REFERENCES p(id));`,
		wantKeys: "c_ibfk_1(y) c_ibfk_2(x)",
		wantIdx:  "x(x) y(y)",
	},
	{
		name: "a named key and an unnamed one added by ALTER TABLE",
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, x int, y int, CONSTRAINT c_ibfk_5 FOREIGN KEY (x) REFERENCES p(id));
ALTER TABLE c ADD FOREIGN KEY (y) REFERENCES p(id);`,
		wantKeys: "c_ibfk_5(x) c_ibfk_6(y)",
		wantIdx:  "c_ibfk_5(x) y(y)",
	},
	{
		name: "a composite key",
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE q (a int, b int, UNIQUE KEY (a, b));
CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a, b) REFERENCES q(a, b));`,
		wantKeys: "c_ibfk_1(a,b)",
		wantIdx:  "a(a,b)",
	},
	{
		name: "an index already holds the column's name",
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int, o int, KEY p_id (o), FOREIGN KEY (p_id) REFERENCES p(id));`,
		wantKeys: "c_ibfk_1(p_id)",
		wantIdx:  "p_id(o) p_id_2(p_id)",
	},
	{
		name: "a CONSTRAINT without a symbol",
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT FOREIGN KEY (p_id) REFERENCES p(id));`,
		wantKeys: "c_ibfk_1(p_id)",
		wantIdx:  "p_id(p_id)",
	},
	{
		name: "CONSTRAINT without a symbol numbered with the other unnamed keys",
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, x int, y int, z int, w int, FOREIGN KEY (x) REFERENCES p(id),
  CONSTRAINT FOREIGN KEY (y) REFERENCES p(id), CONSTRAINT c_ibfk_5 FOREIGN KEY (z) REFERENCES p(id),
  CONSTRAINT FOREIGN KEY (w) REFERENCES p(id));`,
		wantKeys: "c_ibfk_1(x) c_ibfk_2(y) c_ibfk_3(w) c_ibfk_5(z)",
		wantIdx:  "c_ibfk_5(z) w(w) x(x) y(y)",
	},
	{
		name: "ALTER TABLE adds a CONSTRAINT without a symbol",
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, x int, y int, CONSTRAINT c_ibfk_5 FOREIGN KEY (x) REFERENCES p(id));
ALTER TABLE c ADD CONSTRAINT FOREIGN KEY (y) REFERENCES p(id);`,
		wantKeys: "c_ibfk_5(x) c_ibfk_6(y)",
		wantIdx:  "c_ibfk_5(x) y(y)",
	},
	{
		name: "an unnamed key before an unnamed descending index",
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY (a) REFERENCES p(id), KEY (a DESC));`,
		wantKeys: "c_ibfk_1(a)",
		wantIdx:  "a(a) a_2(a DESC)",
	},
}

// mysqlScratch is the administrative connection the scratch databases of one
// test are created through.
type mysqlScratch struct {
	admin    *sql.DB
	adminDSN string
	adminURL string
}

func newMySQLScratch(c *qt.C) mysqlScratch {
	c.Helper()
	adminDSN := dbtarget.DriverDSN(c, dbtarget.MySQLAdmin)
	admin, err := sql.Open("mysql", adminDSN)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	c.Assert(admin.PingContext(c.Context()), qt.IsNil)
	return mysqlScratch{admin: admin, adminDSN: adminDSN, adminURL: dbtarget.URL(c, dbtarget.MySQLAdmin)}
}

// builtFrom creates a database and runs the SQL on it as the server receives
// it, with nothing of Ptah's in between.
func (s mysqlScratch) builtFrom(c *qt.C, schema string) (name, url string) {
	c.Helper()
	name, url = s.database(c, "fk_built")
	conn, err := sql.Open("mysql", mySQLDSNForDatabase(c, s.adminDSN, name)+"?multiStatements=true")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	_, err = conn.ExecContext(c.Context(), schema)
	c.Assert(err, qt.IsNil)
	return name, url
}

// database creates an empty database, dropped when the test ends, and
// returns its name and the URL ptah connects to it with.
func (s mysqlScratch) database(c *qt.C, prefix string) (name, url string) {
	c.Helper()
	name = fmt.Sprintf("ptah_%s_%d", prefix, time.Now().UnixNano())
	createMySQLDatabase(c, c.Context(), s.admin, name)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), s.admin, name) })
	return name, asMySQLURL(replaceMySQLDatabaseName(c, s.adminURL, name))
}

// keysOfC answers the foreign keys of `c` as `name(columns)` and its secondary
// indexes as `name(columns)`, each in name order.
func (s mysqlScratch) keysOfC(c *qt.C, database string) (keys, indexes string) {
	c.Helper()
	err := s.admin.QueryRowContext(c.Context(), `
		SELECT
			(SELECT COALESCE(GROUP_CONCAT(CONCAT(k.name, '(', k.cols, ')') ORDER BY k.name SEPARATOR ' '), '')
			 FROM (SELECT CONSTRAINT_NAME AS name, GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) AS cols
			       FROM information_schema.KEY_COLUMN_USAGE
			       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'c' AND REFERENCED_TABLE_NAME IS NOT NULL
			       GROUP BY CONSTRAINT_NAME) k),
			(SELECT COALESCE(GROUP_CONCAT(CONCAT(i.name, '(', i.cols, ')') ORDER BY i.name SEPARATOR ' '), '')
			 FROM (SELECT INDEX_NAME AS name,
			              GROUP_CONCAT(CONCAT(COLUMN_NAME, IF(COLLATION = 'D', ' DESC', '')) ORDER BY SEQ_IN_INDEX) AS cols
			       FROM information_schema.STATISTICS
			       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'c' AND INDEX_NAME <> 'PRIMARY'
			       GROUP BY INDEX_NAME) i)`,
		database, database,
	).Scan(&keys, &indexes)
	c.Assert(err, qt.IsNil)
	return keys, indexes
}

// TestMigrateDiffFindsAMySQLFileWithUnnamedForeignKeysSyncedE2E replays a
// directory whose one migration is the schema file itself and diffs: the
// directory is synced. Applied to an empty database, the directory builds the
// keys and indexes the row names, which is what the file is compared with.
func TestMigrateDiffFindsAMySQLFileWithUnnamedForeignKeysSyncedE2E(t *testing.T) {
	for _, test := range mysqlUnnamedForeignKeys {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLScratch(c)
			dir, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
			_, dev := scratch.database(c, "fk_dev")
			targetName, target := scratch.database(c, "fk_target")

			plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", dev, "--dry-run")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
			out, err := runCompatVerb("migrate", "apply", "--url", target, "--dir", "file://"+dir)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			keys, indexes := scratch.keysOfC(c, targetName)

			c.Assert(plan, qt.Contains, "The migration directory is synced with the desired state")
			c.Assert(keys, qt.Equals, test.wantKeys)
			c.Assert(indexes, qt.Equals, test.wantIdx)
		})
	}
}

// TestSchemaApplyFindsAMySQLFileWithUnnamedForeignKeysSyncedE2E plans the
// native apply of the schema file against the database the same file built:
// nothing is planned.
func TestSchemaApplyFindsAMySQLFileWithUnnamedForeignKeysSyncedE2E(t *testing.T) {
	for _, test := range mysqlUnnamedForeignKeys {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLScratch(c)
			_, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
			targetName, target := scratch.builtFrom(c, test.schema)

			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
			keys, indexes := scratch.keysOfC(c, targetName)

			c.Assert(keys, qt.Equals, test.wantKeys)
			c.Assert(indexes, qt.Equals, test.wantIdx)
			c.Assert(plan, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyOfAMySQLFileWithUnnamedForeignKeysConvergesE2E applies the
// schema file natively to an empty database and plans again. Ptah writes each
// key under the name it derived, so the database holds the keys the file names
// and the second plan is empty. The indexes differ from the rows above and are
// not compared: a key created with a name gets an index under that name.
func TestSchemaApplyOfAMySQLFileWithUnnamedForeignKeysConvergesE2E(t *testing.T) {
	for _, test := range mysqlUnnamedForeignKeys {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLScratch(c)
			_, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
			targetName, target := scratch.database(c, "fk_target")

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
			again := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
			keys, _ := scratch.keysOfC(c, targetName)

			c.Assert(keys, qt.Equals, test.wantKeys)
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

// TestMigrateDiffRenamesAMySQLKeyTheFileLeavesUnnamedE2E is the control: a
// migration that names the key `c_p_fk` and a schema file that leaves it
// unnamed differ in the key's name, so the plan drops `c_p_fk` and adds
// `c_ibfk_1`, as Atlas CE v1.3.0 does, and the directory then converges.
func TestMigrateDiffRenamesAMySQLKeyTheFileLeavesUnnamedE2E(t *testing.T) {
	c := qt.New(t)
	scratch := newMySQLScratch(c)
	dir, schema := writeRewrittenMigrationProject(c,
		`CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int NOT NULL, CONSTRAINT c_p_fk FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE);`,
		`CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int NOT NULL, FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE);`)
	_, dev := scratch.database(c, "fk_dev")
	targetName, target := scratch.database(c, "fk_target")

	plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir, "--to", "file://"+schema, "--dev-url", dev, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
	out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir, "--to", "file://"+schema, "--dev-url", dev)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runCompatVerb("migrate", "apply", "--url", target, "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	again, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir, "--to", "file://"+schema, "--dev-url", dev, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", again))
	keys, _ := scratch.keysOfC(c, targetName)

	c.Assert(plan, qt.Contains, "DROP FOREIGN KEY `c_p_fk`")
	c.Assert(plan, qt.Contains, "ADD CONSTRAINT `c_ibfk_1`")
	c.Assert(keys, qt.Equals, "c_ibfk_1(p_id)")
	c.Assert(again, qt.Contains, "The migration directory is synced with the desired state")
}
