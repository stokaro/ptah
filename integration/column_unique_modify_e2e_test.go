//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// columnUniqueModified are a migration and a schema file that change a column
// of a table on MySQL and MariaDB. A UNIQUE in MODIFY COLUMN asks for a unique
// key whatever keys the column already has. Measured on MySQL 8.4.11 and
// MariaDB 11.8.9, a column that keeps its UNIQUE while its type, nullability
// or default changes, planned with the clause, ends with a second key, `x_2`,
// which the next comparison drops. Atlas CE v1.3.0 restates the column without
// the clause and its next comparison is synced (stokaro/ptah#3857). A column
// that gains its UNIQUE is the control: the clause is what adds its key. want is the statement each engine is planned;
// keys are the keys of c other than the primary key once the plan is applied.
var columnUniqueModified = []struct {
	name      string
	migration string
	schema    string
	want      string
	keys      string
}{
	{
		name:      "the type of a UNIQUE column changes",
		migration: `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, x bigint UNIQUE);`,
		want:      "MODIFY COLUMN `x` bigint;",
		keys:      "x(x)",
	},
	{
		name:      "a UNIQUE column becomes NOT NULL",
		migration: `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, x int NOT NULL UNIQUE);`,
		want:      "MODIFY COLUMN `x` int NOT NULL;",
		keys:      "x(x)",
	},
	{
		name:      "a UNIQUE column gains a default",
		migration: `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, x int DEFAULT 5 UNIQUE);`,
		want:      "MODIFY COLUMN `x` int DEFAULT 5;",
		keys:      "x(x)",
	},
	{
		name: "a UNIQUE column whose key holds the next name",
		migration: `CREATE TABLE c (id int PRIMARY KEY, y int, KEY x (y));
ALTER TABLE c ADD COLUMN x int UNIQUE;`,
		schema: `CREATE TABLE c (id int PRIMARY KEY, y int, KEY x (y));
ALTER TABLE c ADD COLUMN x bigint UNIQUE;`,
		want: "MODIFY COLUMN `x` bigint;",
		keys: "x(y) x_2(x)",
	},
	{
		name:      "a column gains its UNIQUE as its type changes",
		migration: `CREATE TABLE c (id int PRIMARY KEY, x int);`,
		schema:    `CREATE TABLE c (id int PRIMARY KEY, x bigint UNIQUE);`,
		want:      "MODIFY COLUMN `x` bigint UNIQUE;",
		keys:      "x(x)",
	},
}

// keysOfC answers the keys of table c other than its primary key, as
// `name(columns)` in the byte order of their names.
func keysOfC(c *qt.C, url string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var keys string
	err = conn.QueryRowContext(c.Context(), `
		SELECT COALESCE(GROUP_CONCAT(CONCAT(i.name, '(', i.cols, ')') ORDER BY CAST(i.name AS BINARY) SEPARATOR ' '), '')
		FROM (SELECT INDEX_NAME AS name, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS cols
		      FROM information_schema.STATISTICS
		      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'c' AND INDEX_NAME <> 'PRIMARY'
		      GROUP BY INDEX_NAME) i`,
	).Scan(&keys)
	c.Assert(err, qt.IsNil)
	return keys
}

// TestMigrateDiffModifiesAUniqueColumnWithOneKeyE2E plans the directory
// against the schema file, writes the plan into the directory, applies the
// directory to an empty database and diffs again: the directory is synced, and
// the column has one key.
func TestMigrateDiffModifiesAUniqueColumnWithOneKeyE2E(t *testing.T) {
	for _, engine := range mysqlFamilyUniqueEngines {
		for _, test := range columnUniqueModified {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				dir, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)

				plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
					"--to", "file://"+schema, "--dev-url", engine.emptyDB(c), "--dry-run")
				c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
				written, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
					"--to", "file://"+schema, "--dev-url", engine.emptyDB(c))
				c.Assert(err, qt.IsNil, qt.Commentf("%s", written))
				target := engine.emptyDB(c)
				out, err := runCompatVerb("migrate", "apply", "--url", target, "--dir", "file://"+dir)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				again, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
					"--to", "file://"+schema, "--dev-url", engine.emptyDB(c), "--dry-run")
				c.Assert(err, qt.IsNil, qt.Commentf("%s", again))

				c.Assert(plan, qt.Contains, test.want)
				c.Assert(again, qt.Contains, "The migration directory is synced with the desired state")
				c.Assert(keysOfC(c, target), qt.Equals, test.keys)
			})
		}
	}
}

// TestSchemaApplyModifiesAUniqueColumnWithOneKeyE2E applies the schema file
// natively to the database the migration built, and plans it again: nothing is
// left to do, and the column has one key.
func TestSchemaApplyModifiesAUniqueColumnWithOneKeyE2E(t *testing.T) {
	for _, engine := range mysqlFamilyUniqueEngines {
		for _, test := range columnUniqueModified {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				_, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
				target := engine.builtDB(c, test.migration)

				applied := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
				again := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

				c.Assert(applied, qt.Contains, test.want)
				c.Assert(again, qt.Contains, "Schema is synced")
				c.Assert(keysOfC(c, target), qt.Equals, test.keys)
			})
		}
	}
}
