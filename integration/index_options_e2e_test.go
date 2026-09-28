//go:build integration

package integration_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// indexOptionEngine is one MySQL-family server the rows below run on, the word
// it hides an index with -- MySQL's INVISIBLE, MariaDB's IGNORED, each
// answering ERROR 1064 to the other's -- and the query that describes each
// index of ic as the server records it: name, comment, and whether the
// optimizer uses it (stokaro/ptah#3853).
type indexOptionEngine struct {
	name     string
	engine   dbtarget.Engine
	hidden   string
	readBack string
}

var indexOptionEngines = []indexOptionEngine{
	{
		name: "MySQL", engine: dbtarget.MySQLAdmin, hidden: " INVISIBLE",
		readBack: `SELECT CONCAT(INDEX_NAME, ' comment=', INDEX_COMMENT, ' visible=', IS_VISIBLE)
FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ic' ORDER BY 1`,
	},
	{
		name: "MariaDB", engine: dbtarget.MariaDBAdmin, hidden: " IGNORED",
		readBack: `SELECT CONCAT(INDEX_NAME, ' comment=', INDEX_COMMENT, ' visible=',
  CASE IGNORED WHEN 'YES' THEN 'NO' ELSE 'YES' END)
FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ic' ORDER BY 1`,
	},
}

// indexOptionSchema is the table the rows declare: ip, and ic whose q
// references it, with k_q the index that foreign key needs, k_a and k_b two
// more. qComment and aComment are the comments of k_q and k_a, and bHidden
// follows k_b's key parts.
func indexOptionSchema(qComment, aComment, bHidden string) string {
	return `CREATE TABLE ip (id int PRIMARY KEY);
CREATE TABLE ic (id int PRIMARY KEY, a int, b int, q int,
  KEY k_q (q) COMMENT '` + qComment + `', KEY k_a (a) COMMENT '` + aComment + `', KEY k_b (b)` + bHidden + `,
  CONSTRAINT ic_q_fk FOREIGN KEY (q) REFERENCES ip (id));`
}

// indexOptionDatabase builds a database from ddl and returns its URL and a
// read of the indexes of ic as the server records them.
func indexOptionDatabase(c *qt.C, engine indexOptionEngine, ddl string) (string, func() []string) {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine.engine)
	name, url := scratch.builtFrom(c, ddl)
	return url, func() []string {
		return indexOptionsReadBack(c, engine, mySQLDSNForDatabase(c, scratch.adminDSN, name))
	}
}

// indexOptionsReadBack describes each index of ic in the database dsn names.
func indexOptionsReadBack(c *qt.C, engine indexOptionEngine, dsn string) []string {
	c.Helper()
	conn, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	rows, err := conn.QueryContext(c.Context(), engine.readBack)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var described []string
	for rows.Next() {
		var line string
		c.Assert(rows.Scan(&line), qt.IsNil)
		described = append(described, line)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return described
}

// TestSchemaApplyKeepsIndexCommentsAndVisibilityE2E applies a schema file to a
// database whose indexes carry other comments and visibility, or that lacks
// the table, and reads them back: they are the ones the file's own SQL builds.
// A comment change on k_q, the index a foreign key needs, is one statement,
// which the server takes where it refuses the DROP INDEX alone with ERROR
// 1553. The next apply is synced.
func TestSchemaApplyKeepsIndexCommentsAndVisibilityE2E(t *testing.T) {
	for _, engine := range indexOptionEngines {
		for _, direction := range []struct {
			name     string
			from, to string
			planned  string
		}{
			{
				name: "a table created with them",
				from: `CREATE TABLE ip (id int PRIMARY KEY);`, to: indexOptionSchema("q", "a", engine.hidden),
				planned: "CREATE TABLE `ic`",
			},
			{
				name: "comments change and an index hides",
				from: indexOptionSchema("old", "", ""), to: indexOptionSchema("new", "a", engine.hidden),
				planned: "DROP INDEX `k_q`, ADD INDEX `k_q`",
			},
			{
				name: "comments go and an index shows",
				from: indexOptionSchema("new", "a", engine.hidden), to: indexOptionSchema("", "", ""),
				planned: "DROP INDEX `k_q`, ADD INDEX `k_q`",
			},
		} {
			t.Run(engine.name+"/"+direction.name, func(t *testing.T) {
				c := qt.New(t)
				_, built := indexOptionDatabase(c, engine, direction.to)
				want := built()
				target, readBack := indexOptionDatabase(c, engine, direction.from)
				_, schema := writeRewrittenMigrationProject(c, direction.to, direction.to)

				plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
				c.Assert(plan, qt.Contains, direction.planned)
				runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

				c.Assert(readBack(), qt.DeepEquals, want)
				plan = runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
				c.Assert(plan, qt.Contains, "Schema is synced")
			})
		}
	}
}

// TestSchemaApplyShowsAnIndexInPlaceE2E changes only whether the optimizer
// uses an index, which the plan does with ALTER INDEX rather than a rebuild.
func TestSchemaApplyShowsAnIndexInPlaceE2E(t *testing.T) {
	for _, engine := range indexOptionEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			file := indexOptionSchema("q", "a", "")
			target := mysqlFamilyBuiltDatabase(c, engine.engine, indexOptionSchema("q", "a", engine.hidden))
			_, schema := writeRewrittenMigrationProject(c, file, file)

			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(plan, qt.Contains, "ALTER INDEX `k_b`")
			c.Assert(plan, qt.Not(qt.Contains), "DROP INDEX")
		})
	}
}

// TestIndexOptionsFindTheirOwnDatabaseSyncedE2E compares a database holding
// commented and hidden indexes with itself, and a directory whose one
// migration is the schema file with the file: both are synced.
func TestIndexOptionsFindTheirOwnDatabaseSyncedE2E(t *testing.T) {
	for _, engine := range indexOptionEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			file := indexOptionSchema("q", "a", engine.hidden)
			source := mysqlFamilyBuiltDatabase(c, engine.engine, file)
			dir, schema := writeRewrittenMigrationProject(c, file, file)

			selfDiff := runPtahNative(c, "schema", "diff", "--from", source, "--to", source)
			migrateDiff, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", mysqlFamilyEmptyDatabase(c, engine.engine), "--dry-run")

			c.Assert(selfDiff, qt.Contains, "Schemas are synced")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", migrateDiff))
			c.Assert(migrateDiff, qt.Contains, "The migration directory is synced with the desired state")
		})
	}
}
