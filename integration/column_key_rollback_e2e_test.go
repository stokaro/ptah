//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// columnKeyRollbackEngine is a server the rows below run on: a database built
// from SQL with nothing of Ptah's in between, with the URL ptah reads it by and
// a connection that runs a migration file, and how to read its keys back.
type columnKeyRollbackEngine struct {
	name  string
	built func(c *qt.C, sql string) (url string, db *sql.DB)
	read  func(c *qt.C, db *sql.DB) []string
}

var (
	postgresColumnKeyRollback = columnKeyRollbackEngine{
		name: "PostgreSQL",
		built: func(c *qt.C, schema string) (string, *sql.DB) {
			c.Helper()
			url := databaseBuiltFrom(c, schema)
			db, err := sql.Open("pgx", url)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			return url, db
		},
		read: func(c *qt.C, db *sql.DB) []string {
			c.Helper()
			return queryRows(c, db, `SELECT 'column ' || cl.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod)
FROM pg_attribute a
JOIN pg_class cl ON cl.oid = a.attrelid
JOIN pg_namespace n ON n.oid = cl.relnamespace
WHERE n.nspname = 'public' AND cl.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
UNION ALL
SELECT 'constraint ' || conrelid::regclass::text || '.' || conname || ' ' || pg_get_constraintdef(oid)
FROM pg_constraint WHERE connamespace = 'public'::regnamespace
UNION ALL
SELECT 'index ' || indexname || ' ' || indexdef FROM pg_indexes WHERE schemaname = 'public'
ORDER BY 1`)
		},
	}
	mysqlColumnKeyRollback = columnKeyRollbackEngine{
		name: "MySQL",
		built: func(c *qt.C, schema string) (string, *sql.DB) {
			c.Helper()
			scratch := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin)
			name, url := scratch.builtFrom(c, schema)
			db, err := sql.Open("mysql", mySQLDSNForDatabase(c, scratch.adminDSN, name)+"?multiStatements=true")
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			return url, db
		},
		read: func(c *qt.C, db *sql.DB) []string {
			c.Helper()
			return queryRows(c, db, `SELECT CONCAT('column ', TABLE_NAME, '.', COLUMN_NAME, ' ', COLUMN_TYPE)
FROM information_schema.columns WHERE TABLE_SCHEMA = DATABASE()
UNION ALL
SELECT CONCAT('index ', TABLE_NAME, '.', INDEX_NAME, ' unique=', 1 - NON_UNIQUE, ' (', GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX), ')')
FROM information_schema.statistics WHERE TABLE_SCHEMA = DATABASE()
GROUP BY TABLE_NAME, INDEX_NAME, NON_UNIQUE
ORDER BY 1`)
		},
	}
)

// columnKeyRollbacks are a database and a schema file that changes a column's
// own UNIQUE, or changes a key beside it that takes or frees the name of the
// column's key. The first four are the shapes stokaro/ptah#4982 measured on
// PostgreSQL 18.6, where the down file stopped with `relation "..." already
// exists`.
var columnKeyRollbacks = []struct {
	name   string
	engine columnKeyRollbackEngine
	before string
	after  string
}{
	{
		name:   "a UNIQUE column a foreign key references is dropped, with its table",
		engine: postgresColumnKeyRollback,
		before: `CREATE TABLE t (id bigint PRIMARY KEY, code text UNIQUE);
CREATE TABLE u (id bigint PRIMARY KEY, t_code text REFERENCES t (code));`,
		after: `CREATE TABLE t (id bigint PRIMARY KEY);`,
	},
	{
		name:   "a column takes UNIQUE from a key that has MySQL's name",
		engine: postgresColumnKeyRollback,
		before: `CREATE TABLE c (id int PRIMARY KEY, x int, CONSTRAINT x UNIQUE (x));`,
		after:  `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
	},
	{
		name:   "a column takes UNIQUE whose name a key over another column holds",
		engine: postgresColumnKeyRollback,
		before: `CREATE TABLE c (id int PRIMARY KEY, x int, y int, CONSTRAINT c_x_uq UNIQUE (x), CONSTRAINT c_x_key UNIQUE (y));`,
		after:  `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE, y int);`,
	},
	{
		name:   "a UNIQUE column is added whose name a key over another column holds",
		engine: postgresColumnKeyRollback,
		before: `CREATE TABLE c (id int PRIMARY KEY, y int, CONSTRAINT c_x_key UNIQUE (y));`,
		after:  `CREATE TABLE c (id int PRIMARY KEY, y int, x int UNIQUE);`,
	},
	{
		name:   "a UNIQUE column is dropped",
		engine: postgresColumnKeyRollback,
		before: `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
		after:  `CREATE TABLE c (id int PRIMARY KEY);`,
	},
	{
		name:   "a column loses UNIQUE",
		engine: postgresColumnKeyRollback,
		before: `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
		after:  `CREATE TABLE c (id int PRIMARY KEY, x int);`,
	},
	{
		name:   "a column takes UNIQUE",
		engine: postgresColumnKeyRollback,
		before: `CREATE TABLE c (id int PRIMARY KEY, x int);`,
		after:  `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
	},
	{
		name:   "a UNIQUE column is added",
		engine: postgresColumnKeyRollback,
		before: `CREATE TABLE c (id int PRIMARY KEY);`,
		after:  `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
	},
	{
		name:   "a UNIQUE column is dropped",
		engine: mysqlColumnKeyRollback,
		before: `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
		after:  `CREATE TABLE c (id int PRIMARY KEY);`,
	},
	{
		name:   "a column loses UNIQUE",
		engine: mysqlColumnKeyRollback,
		before: `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
		after:  `CREATE TABLE c (id int PRIMARY KEY, x int);`,
	},
	{
		name:   "a column takes UNIQUE",
		engine: mysqlColumnKeyRollback,
		before: `CREATE TABLE c (id int PRIMARY KEY, x int);`,
		after:  `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
	},
	{
		name:   "a UNIQUE column is added whose name a key over another column holds",
		engine: mysqlColumnKeyRollback,
		before: `CREATE TABLE c (id int PRIMARY KEY, y int, CONSTRAINT x UNIQUE (y));`,
		after:  `CREATE TABLE c (id int PRIMARY KEY, y int, x int UNIQUE);`,
	},
	{
		name:   "a UNIQUE column is added",
		engine: mysqlColumnKeyRollback,
		before: `CREATE TABLE c (id int PRIMARY KEY);`,
		after:  `CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);`,
	},
}

// TestColumnKeyRollbackE2E_DownRestoresTheKeys builds a database from before,
// writes the migration to after with `ptah migrations generate`, and runs its
// up file and then its down file, reading the columns and keys back after
// each. The up file changes what the database holds, and the down file gives
// back what it held before.
func TestColumnKeyRollbackE2E_DownRestoresTheKeys(t *testing.T) {
	for _, test := range columnKeyRollbacks {
		t.Run(test.engine.name+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)
			url, db := test.engine.built(c, test.before)
			before := test.engine.read(c, db)
			work := c.TempDir()
			dir := filepath.Join(work, "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			schema := filepath.Join(work, "after.sql")
			c.Assert(os.WriteFile(schema, []byte(test.after+"\n"), 0o600), qt.IsNil)
			runPtahNative(c, "migrations", "generate", "--db-url", url,
				"--schema-file", schema, "--migrations-dir", dir, "--name", "after")
			pairs := readMigrationPairs(c, dir)
			c.Assert(pairs, qt.HasLen, 1)

			_, err := db.ExecContext(c.Context(), pairs[0].Up)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", pairs[0].Up))
			c.Assert(test.engine.read(c, db), qt.Not(qt.DeepEquals), before, qt.Commentf("%s", pairs[0].Up))
			_, err = db.ExecContext(c.Context(), pairs[0].Down)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", pairs[0].Down))

			c.Assert(test.engine.read(c, db), qt.DeepEquals, before, qt.Commentf("%s", pairs[0].Down))
		})
	}
}
