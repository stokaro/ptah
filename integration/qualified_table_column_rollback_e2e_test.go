//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// qualifiedTableBefore and qualifiedTableAfter name a table with its default
// schema and change one of its columns. The database read names the table
// bare, so the rollback has to find `public.items` under `items`.
const (
	qualifiedTableBefore = "CREATE TABLE public.items (id integer PRIMARY KEY, sku varchar(64));\n"
	qualifiedTableAfter  = "CREATE TABLE public.items (id integer PRIMARY KEY, sku varchar(128));\n"
)

// qualifiedTableWriters each plan qualifiedTableAfter against a database
// holding qualifiedTableBefore and return the up and down file of that step.
var qualifiedTableWriters = []struct {
	name  string
	write func(c *qt.C, before, after string) migrationPair
}{
	{name: "migrations generate", write: qualifiedTableNative},
	{name: "migrations generate --replay", write: qualifiedTableSteps(writeNative)},
	{name: "migrate diff golang-migrate", write: qualifiedTableSteps(writeGolangMigrate)},
}

// qualifiedTableNative reads a database built from before and writes the
// step to after with `ptah migrations generate`.
func qualifiedTableNative(c *qt.C, before, after string) migrationPair {
	c.Helper()
	source := databaseBuiltFrom(c, before)
	work := c.TempDir()
	dir := filepath.Join(work, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	schema := filepath.Join(work, "after.sql")
	c.Assert(os.WriteFile(schema, []byte(after), 0o600), qt.IsNil)
	runPtahNative(c, "migrations", "generate", "--db-url", source,
		"--schema-file", schema, "--migrations-dir", dir, "--name", "after")
	pairs := readMigrationPairs(c, dir)
	c.Assert(pairs, qt.HasLen, 1)
	return pairs[0]
}

// qualifiedTableSteps writes before and after as two steps with write, on a
// dev database of their own, and returns the second step.
func qualifiedTableSteps(write func(c *qt.C, dir, dev, name, schema string)) func(c *qt.C, before, after string) migrationPair {
	return func(c *qt.C, before, after string) migrationPair {
		c.Helper()
		envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
		dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_qualified_rollback_dev"), "public")
		work := c.TempDir()
		dir := filepath.Join(work, "migrations")
		c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
		for _, step := range []struct{ name, schema string }{{"before", before}, {"after", after}} {
			path := filepath.Join(work, step.name+".sql")
			c.Assert(os.WriteFile(path, []byte(step.schema), 0o600), qt.IsNil)
			write(c, dir, dev, step.name, path)
		}
		pairs := readMigrationPairs(c, dir)
		c.Assert(pairs, qt.HasLen, 2)
		return pairs[1]
	}
}

// readPublicColumns reads every column of the tables of the public schema,
// with its type and nullability, as the catalog reports them.
func readPublicColumns(c *qt.C, db *sql.DB) []string {
	c.Helper()
	return queryRows(c, db, `SELECT cl.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod)
  || CASE WHEN a.attnotnull THEN ' not null' ELSE '' END
FROM pg_attribute a
JOIN pg_class cl ON cl.oid = a.attrelid
JOIN pg_namespace n ON n.oid = cl.relnamespace
WHERE n.nspname = 'public' AND cl.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY 1`)
}

// TestQualifiedTableColumnChangeE2E_DownRestoresTheColumn plans a column change
// on a table named with its schema, applies the up file and then the down file
// to a database built from the first schema, and reads the columns back after
// each: the down file puts the column back as the database held it.
func TestQualifiedTableColumnChangeE2E_DownRestoresTheColumn(t *testing.T) {
	for _, writer := range qualifiedTableWriters {
		t.Run(writer.name, func(t *testing.T) {
			c := qt.New(t)
			pair := writer.write(c, qualifiedTableBefore, qualifiedTableAfter)
			target, err := sql.Open("pgx", databaseBuiltFrom(c, qualifiedTableBefore))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(target.Close(), qt.IsNil) })
			before := []string{"items.id integer not null", "items.sku character varying(64)"}
			c.Assert(readPublicColumns(c, target), qt.DeepEquals, before)

			_, err = target.ExecContext(c.Context(), pair.Up)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", pair.Up))
			c.Assert(readPublicColumns(c, target), qt.DeepEquals,
				[]string{"items.id integer not null", "items.sku character varying(128)"}, qt.Commentf("%s", pair.Up))

			_, err = target.ExecContext(c.Context(), pair.Down)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", pair.Down))
			c.Assert(readPublicColumns(c, target), qt.DeepEquals, before, qt.Commentf("%s", pair.Down))
		})
	}
}

// TestQualifiedTableColumnChangeE2E_SQLiteDownRestoresTheColumn is the same
// plan on SQLite, where the table is named with `main` and a type change is a
// rebuild: the rollback rebuilds the table as the database held it.
func TestQualifiedTableColumnChangeE2E_SQLiteDownRestoresTheColumn(t *testing.T) {
	c := qt.New(t)
	work := c.TempDir()
	path := filepath.Join(work, "live.db")
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.ExecContext(c.Context(), "CREATE TABLE main.items (id integer PRIMARY KEY, sku varchar(64))")
	c.Assert(err, qt.IsNil)
	before := readSQLiteColumns(c, db)
	c.Assert(before, qt.DeepEquals, []string{"id integer", "sku varchar(64)"})

	dir := filepath.Join(work, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	schema := filepath.Join(work, "after.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE main.items (id integer PRIMARY KEY, sku bigint);\n"), 0o600), qt.IsNil)
	runPtahNative(c, "migrations", "generate", "--db-url", "sqlite://"+filepath.ToSlash(path),
		"--schema-file", schema, "--migrations-dir", dir, "--name", "after")
	pairs := readMigrationPairs(c, dir)
	c.Assert(pairs, qt.HasLen, 1)

	_, err = db.ExecContext(c.Context(), pairs[0].Up)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", pairs[0].Up))
	c.Assert(readSQLiteColumns(c, db), qt.DeepEquals, []string{"id integer", "sku bigint"}, qt.Commentf("%s", pairs[0].Up))

	_, err = db.ExecContext(c.Context(), pairs[0].Down)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", pairs[0].Down))
	c.Assert(readSQLiteColumns(c, db), qt.DeepEquals, before, qt.Commentf("%s", pairs[0].Down))
}

// readSQLiteColumns reads the columns of items with their declared types,
// folded to lower case as SQLite compares them.
func readSQLiteColumns(c *qt.C, db *sql.DB) []string {
	c.Helper()
	return queryRows(c, db, `SELECT name || ' ' || lower(type) FROM pragma_table_info('items') ORDER BY cid`)
}

// TestCaseFoldedColumnChangeE2E_MySQLDownRestoresTheColumn changes a column a
// MySQL schema file names in another case than the database does. MySQL
// compares column names without case, so the comparison pairs `SKU` with
// `sku`, and the rollback has to find `sku` in the database read under the
// name the file spells. MODIFY COLUMN writes the name as well as the type, so
// the up file renames the column to `SKU` and the down file names it `sku`
// again.
func TestCaseFoldedColumnChangeE2E_MySQLDownRestoresTheColumn(t *testing.T) {
	c := qt.New(t)
	scratch := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin)
	name, url := scratch.builtFrom(c, "CREATE TABLE items (id int PRIMARY KEY, sku varchar(64));")
	db, err := sql.Open("mysql", mySQLDSNForDatabase(c, scratch.adminDSN, name)+"?multiStatements=true")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	before := readMySQLColumns(c, db)
	c.Assert(before, qt.DeepEquals, []string{"id int", "sku varchar(64)"})

	work := c.TempDir()
	dir := filepath.Join(work, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	schema := filepath.Join(work, "after.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE items (id int PRIMARY KEY, SKU varchar(128));\n"), 0o600), qt.IsNil)
	runPtahNative(c, "migrations", "generate", "--db-url", url,
		"--schema-file", schema, "--migrations-dir", dir, "--name", "after")
	pairs := readMigrationPairs(c, dir)
	c.Assert(pairs, qt.HasLen, 1)

	_, err = db.ExecContext(c.Context(), pairs[0].Up)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", pairs[0].Up))
	c.Assert(readMySQLColumns(c, db), qt.DeepEquals, []string{"id int", "SKU varchar(128)"}, qt.Commentf("%s", pairs[0].Up))

	_, err = db.ExecContext(c.Context(), pairs[0].Down)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", pairs[0].Down))
	c.Assert(readMySQLColumns(c, db), qt.DeepEquals, before, qt.Commentf("%s", pairs[0].Down))
}

// readMySQLColumns reads the columns of items in the connection's database,
// with their types.
func readMySQLColumns(c *qt.C, db *sql.DB) []string {
	c.Helper()
	return queryRows(c, db, `SELECT CONCAT(COLUMN_NAME, ' ', COLUMN_TYPE) FROM information_schema.columns
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'items' ORDER BY ORDINAL_POSITION`)
}
