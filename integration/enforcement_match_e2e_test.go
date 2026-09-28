//go:build integration

package integration_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// clauseEngine is one server the rows below run on: how to get an empty
// database there, a database built from SQL with nothing of Ptah's in between,
// the catalog query that reads back what each constraint of table ec says
// about enforcement and its MATCH type, and the arguments `schema diff` needs
// to plan for that server.
type clauseEngine struct {
	name     string
	emptyDB  func(c *qt.C) string
	builtDB  func(c *qt.C, sql string) string
	readBack string
	diffArgs []string
}

// clauseEngines are MySQL and PostgreSQL, the engines that keep NOT ENFORCED
// and a MATCH type. MariaDB records neither, and the reader refuses both
// there.
var clauseEngines = []clauseEngine{
	{
		name:    "MySQL",
		emptyDB: func(c *qt.C) string { return mysqlFamilyEmptyDatabase(c, dbtarget.MySQLAdmin) },
		builtDB: func(c *qt.C, sql string) string { return mysqlFamilyBuiltDatabase(c, dbtarget.MySQLAdmin, sql) },
		readBack: `SELECT CONCAT(tc.CONSTRAINT_NAME, ' ', tc.CONSTRAINT_TYPE, ' enforced=', tc.ENFORCED,
  ' match=', COALESCE(rc.MATCH_OPTION, ''))
FROM information_schema.TABLE_CONSTRAINTS tc
LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
  ON rc.CONSTRAINT_SCHEMA = tc.TABLE_SCHEMA AND rc.TABLE_NAME = tc.TABLE_NAME AND rc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
WHERE tc.TABLE_SCHEMA = DATABASE() AND tc.TABLE_NAME = 'ec' AND tc.CONSTRAINT_TYPE IN ('CHECK', 'FOREIGN KEY')
ORDER BY 1`,
	},
	{
		name: "PostgreSQL",
		emptyDB: func(c *qt.C) string {
			url, _ := scratchReplayDatabase(c)
			return url
		},
		builtDB:  databaseBuiltFrom,
		readBack: `SELECT conname || ' ' || pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'ec'::regclass AND contype IN ('c', 'f') ORDER BY 1`,
		// Without a server version, `schema diff` plans for the dialect's
		// default preset, PostgreSQL 17, which has no NOT ENFORCED, even when
		// it compares on a PostgreSQL 18 connection (stokaro/ptah#3910).
		diffArgs: []string{"--server-version", "18"},
	},
}

// clauseSQL is, per engine, a table whose constraints carry every clause the
// engine keeps, and the same table without them. Measured on MySQL 8.4.11,
// 9.7.2 and 26.7.0 and on PostgreSQL 18.6, each server builds the clauses and
// reads them back (stokaro/ptah#3853).
var clauseSQL = map[string]struct{ with, without string }{
	"MySQL": {
		with: `CREATE TABLE ep (id int PRIMARY KEY);
CREATE TABLE ec (id int PRIMARY KEY, a int CHECK (a > 0) NOT ENFORCED, b int, p int, q int,
  CONSTRAINT ec_b CHECK (b > 0) NOT ENFORCED,
  CONSTRAINT ec_p FOREIGN KEY (p) REFERENCES ep (id) MATCH FULL,
  CONSTRAINT ec_q FOREIGN KEY (q) REFERENCES ep (id) MATCH PARTIAL);`,
		without: `CREATE TABLE ep (id int PRIMARY KEY);
CREATE TABLE ec (id int PRIMARY KEY, a int CHECK (a > 0), b int, p int, q int,
  CONSTRAINT ec_b CHECK (b > 0),
  CONSTRAINT ec_p FOREIGN KEY (p) REFERENCES ep (id),
  CONSTRAINT ec_q FOREIGN KEY (q) REFERENCES ep (id));`,
	},
	"PostgreSQL": {
		with: `CREATE TABLE ep (id int PRIMARY KEY);
CREATE TABLE ec (id int PRIMARY KEY, a int CHECK (a > 0) NOT ENFORCED, b int,
  p int REFERENCES ep (id) MATCH FULL NOT ENFORCED, q int,
  CONSTRAINT ec_b CHECK (b > 0) NOT ENFORCED,
  CONSTRAINT ec_q FOREIGN KEY (q) REFERENCES ep (id) MATCH FULL);`,
		without: `CREATE TABLE ep (id int PRIMARY KEY);
CREATE TABLE ec (id int PRIMARY KEY, a int CHECK (a > 0), b int,
  p int REFERENCES ep (id), q int,
  CONSTRAINT ec_b CHECK (b > 0),
  CONSTRAINT ec_q FOREIGN KEY (q) REFERENCES ep (id));`,
	},
}

// clauseReadBack answers the engine's read-back over the database at url.
func clauseReadBack(c *qt.C, url, query string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), query)
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

// clauseParentOnly is the referenced table alone, the database the first
// direction below starts from.
const clauseParentOnly = `CREATE TABLE ep (id int PRIMARY KEY);`

// TestSchemaApplyBuildsEnforcementAndMatchE2E applies a schema file to a
// database that holds its constraints with other clauses, or none of them, and
// reads the constraints back: they are the ones the file's own SQL builds. The
// next apply is synced. A change of enforcement or MATCH type is a drop and an
// add, because PostgreSQL 18.6 cannot change a CHECK's enforcement in place
// and MySQL none of these.
func TestSchemaApplyBuildsEnforcementAndMatchE2E(t *testing.T) {
	for _, engine := range clauseEngines {
		sql := clauseSQL[engine.name]
		for _, direction := range []struct{ name, from, to string }{
			{name: "beside the referenced table alone", from: clauseParentOnly, to: sql.with},
			{name: "onto the plain constraints", from: sql.without, to: sql.with},
			{name: "back to the plain constraints", from: sql.with, to: sql.without},
		} {
			t.Run(engine.name+"/"+direction.name, func(t *testing.T) {
				c := qt.New(t)
				want := clauseReadBack(c, engine.builtDB(c, direction.to), engine.readBack)
				target := engine.builtDB(c, direction.from)
				_, schema := writeRewrittenMigrationProject(c, direction.to, direction.to)

				runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

				c.Assert(clauseReadBack(c, target, engine.readBack), qt.DeepEquals, want)
				plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
				c.Assert(plan, qt.Contains, "Schema is synced")
			})
		}
	}
}

// TestMigrateDiffFindsEnforcementAndMatchSyncedE2E replays a directory whose
// one migration is the schema file itself and diffs: the constraints the
// replay builds pair with the file's, clauses and all.
func TestMigrateDiffFindsEnforcementAndMatchSyncedE2E(t *testing.T) {
	for _, engine := range clauseEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			sql := clauseSQL[engine.name].with
			dir, schema := writeRewrittenMigrationProject(c, sql, sql)

			plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", engine.emptyDB(c), "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
			c.Assert(plan, qt.Contains, "The migration directory is synced with the desired state")
		})
	}
}

// TestSchemaDiffFromADatabaseFindsEnforcementAndMatchSyncedE2E diffs the
// database the SQL built against the same SQL as a file: the constraints the
// server built pair with the file's, clauses and all.
func TestSchemaDiffFromADatabaseFindsEnforcementAndMatchSyncedE2E(t *testing.T) {
	for _, engine := range clauseEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			sql := clauseSQL[engine.name].with
			_, schema := writeRewrittenMigrationProject(c, sql, sql)
			source := engine.builtDB(c, sql)

			out := runPtahNative(c, append([]string{"schema", "diff", "--from", source, "--to", "file://" + schema},
				engine.diffArgs...)...)

			c.Assert(out, qt.Contains, "Schemas are synced")
		})
	}
}

// TestMigrationsGenerateReplayFindsEnforcementAndMatchSyncedE2E replays a
// directory whose one migration is the schema file on the dev database and
// compares it with the file: no migration is written.
func TestMigrationsGenerateReplayFindsEnforcementAndMatchSyncedE2E(t *testing.T) {
	for _, engine := range clauseEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			sql := clauseSQL[engine.name].with
			dir, schema := writeRewrittenMigrationProject(c, sql, sql)

			out := runPtahNative(c, "migrations", "generate", "--schema-file", schema,
				"--migrations-dir", dir, "--replay", "--dev-url", engine.emptyDB(c), "--dir-format", "atlas")

			c.Assert(out, qt.Contains, "no migration files generated")
			entries, err := os.ReadDir(dir)
			c.Assert(err, qt.IsNil)
			c.Assert(entries, qt.HasLen, 2)
		})
	}
}
