//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// bothMySQLEngines and mariaDBOnly are the engines a row of
// [mysqlFamilyUnnamedNames] holds on.
var (
	bothMySQLEngines = []dbtarget.Engine{dbtarget.MySQLAdmin, dbtarget.MariaDBAdmin}
	mariaDBOnly      = []dbtarget.Engine{dbtarget.MariaDBAdmin}
)

// mysqlFamilyUnnamedNames are schema files that leave a key unnamed, with the
// secondary indexes of `c` the server built from each and the columns of its
// foreign keys, read back from information_schema.
//
// The server names what the file leaves unnamed, and a file compared with the
// database it built has to arrive at the same names or plan to rename them.
// An unnamed UNIQUE or index an ALTER TABLE adds takes its first column's name
// (stokaro/ptah#3742). MariaDB names an unnamed foreign key as MySQL does, and
// a name written inside the FOREIGN KEY clause names the key there
// (stokaro/ptah#3743). Every row was measured on MySQL 8.4.11 and 26.7.0 and
// on MariaDB 11.8.9 and 12.3.3, and Atlas CE v1.3.0 reports each one synced.
// The foreign keys' names are left out: MariaDB 12.1 and later name an unnamed
// key `<n>` where earlier lines name it `<table>_ibfk_<n>`, and the unit rows
// of internal/sqlschema and internal/mysqlname pin both.
var mysqlFamilyUnnamedNames = []struct {
	name     string
	engines  []dbtarget.Engine
	schema   string
	wantIdx  string
	wantKeys string
}{
	{
		name:    "ALTER TABLE adds an unnamed UNIQUE",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int);
ALTER TABLE c ADD UNIQUE (a);`,
		wantIdx: "a(a)",
	},
	{
		name:    "an index holds the column's name",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int, b int, KEY a (b));
ALTER TABLE c ADD UNIQUE (a), ADD INDEX (b);`,
		wantIdx: "a(b) a_2(a) b(b)",
	},
	{
		name:    "indexes and a UNIQUE in one statement",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE c (id int PRIMARY KEY, a int);
ALTER TABLE c ADD INDEX (a), ADD KEY (a), ADD UNIQUE (a);`,
		wantIdx: "a(a) a_2(a) a_3(a)",
	},
	{
		name:    "a UNIQUE that covers a key takes the name of the key's index",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY (a) REFERENCES p(id));
ALTER TABLE c ADD UNIQUE (a);`,
		wantIdx:  "a(a)",
		wantKeys: "(a)",
	},
	{
		name:    "a named key's index holds the key's name",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT b FOREIGN KEY (a) REFERENCES p(id));
ALTER TABLE c ADD UNIQUE (b);`,
		wantIdx:  "b(a) b_2(b)",
		wantKeys: "(a)",
	},
	{
		name:    "an unnamed composite key's index holds its first column's name",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY, x int, y int, UNIQUE KEY (x, y));
CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a, b) REFERENCES p(x, y));
ALTER TABLE c ADD UNIQUE (a);`,
		wantIdx:  "a(a,b) a_2(a)",
		wantKeys: "(a,b)",
	},
	{
		name:    "an index name in the FOREIGN KEY clause",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int, FOREIGN KEY idx_c_p (p_id) REFERENCES p(id));`,
		wantIdx:  "idx_c_p(p_id)",
		wantKeys: "(p_id)",
	},
	{
		name:    "ALTER TABLE adds a key with an index name in the clause",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int);
ALTER TABLE c ADD FOREIGN KEY idx_c_p (p_id) REFERENCES p(id);`,
		wantIdx:  "idx_c_p(p_id)",
		wantKeys: "(p_id)",
	},
	{
		name:    "ALTER TABLE does not count a key it names",
		engines: bothMySQLEngines,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a) REFERENCES p(id));
ALTER TABLE c ADD CONSTRAINT c_ibfk_9 FOREIGN KEY (a) REFERENCES p(id), ADD FOREIGN KEY (b) REFERENCES p(id);`,
		wantIdx:  "b(b) c_ibfk_9(a)",
		wantKeys: "(a) (a) (b)",
	},
	{
		name:    "an unnamed key on MariaDB",
		engines: mariaDBOnly,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int, FOREIGN KEY (p_id) REFERENCES p(id));`,
		wantIdx:  "p_id(p_id)",
		wantKeys: "(p_id)",
	},
	{
		name:    "an unnamed key numbered after a named one on MariaDB",
		engines: mariaDBOnly,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, x int, y int, CONSTRAINT c_ibfk_5 FOREIGN KEY (x) REFERENCES p(id));
ALTER TABLE c ADD FOREIGN KEY (y) REFERENCES p(id);`,
		wantIdx:  "c_ibfk_5(x) y(y)",
		wantKeys: "(x) (y)",
	},
	{
		name:    "an unnamed key's index numbered after an index on MariaDB",
		engines: mariaDBOnly,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, p_id int, o int, KEY p_id (o), FOREIGN KEY (p_id) REFERENCES p(id));`,
		wantIdx:  "p_id(o) p_id_2(p_id)",
		wantKeys: "(p_id)",
	},
	{
		name:    "a column's REFERENCES on MariaDB",
		engines: mariaDBOnly,
		schema: `CREATE TABLE p (id int PRIMARY KEY);
CREATE TABLE c (id int PRIMARY KEY, a int REFERENCES p(id), b int, FOREIGN KEY (b) REFERENCES p(id));`,
		wantIdx:  "a(a) b(b)",
		wantKeys: "(a) (b)",
	},
}

// foreignKeyColumnsOfC answers the column lists of the foreign keys of `c`,
// each as `(columns)`, in order.
func (s mysqlScratch) foreignKeyColumnsOfC(c *qt.C, database string) string {
	c.Helper()
	var columns string
	err := s.admin.QueryRowContext(c.Context(), `
		SELECT COALESCE(GROUP_CONCAT(CONCAT('(', k.cols, ')') ORDER BY k.cols SEPARATOR ' '), '')
		FROM (SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) AS cols
		      FROM information_schema.KEY_COLUMN_USAGE
		      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'c' AND REFERENCED_TABLE_NAME IS NOT NULL
		      GROUP BY CONSTRAINT_NAME) k`,
		database,
	).Scan(&columns)
	c.Assert(err, qt.IsNil)
	return columns
}

// TestMigrateDiffFindsAFileWithUnnamedMySQLFamilyNamesSyncedE2E replays a
// directory whose one migration is the schema file itself and diffs: the
// directory is synced. Applied to an empty database, the directory builds the
// indexes and keys the row names, which is what the file is compared with.
func TestMigrateDiffFindsAFileWithUnnamedMySQLFamilyNamesSyncedE2E(t *testing.T) {
	for _, test := range mysqlFamilyUnnamedNames {
		for _, engine := range test.engines {
			t.Run(engine.String()+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine)
				dir, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
				_, dev := scratch.database(c, "names_dev")
				targetName, target := scratch.database(c, "names_target")

				plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
					"--to", "file://"+schema, "--dev-url", dev, "--dry-run")
				c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
				out, err := runCompatVerb("migrate", "apply", "--url", target, "--dir", "file://"+dir)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				_, indexes := scratch.keysOfC(c, targetName)

				c.Assert(plan, qt.Contains, "The migration directory is synced with the desired state")
				c.Assert(indexes, qt.Equals, test.wantIdx)
				c.Assert(scratch.foreignKeyColumnsOfC(c, targetName), qt.Equals, test.wantKeys)
			})
		}
	}
}

// TestSchemaApplyFindsAFileWithUnnamedMySQLFamilyNamesSyncedE2E plans the
// native apply of the schema file against the database the same file built,
// with nothing of Ptah's in between: nothing is planned.
func TestSchemaApplyFindsAFileWithUnnamedMySQLFamilyNamesSyncedE2E(t *testing.T) {
	for _, test := range mysqlFamilyUnnamedNames {
		for _, engine := range test.engines {
			t.Run(engine.String()+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine)
				_, schema := writeRewrittenMigrationProject(c, test.schema, test.schema)
				targetName, target := scratch.builtFrom(c, test.schema)

				plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
				_, indexes := scratch.keysOfC(c, targetName)

				c.Assert(indexes, qt.Equals, test.wantIdx)
				c.Assert(scratch.foreignKeyColumnsOfC(c, targetName), qt.Equals, test.wantKeys)
				c.Assert(plan, qt.Contains, "Schema is synced")
			})
		}
	}
}
