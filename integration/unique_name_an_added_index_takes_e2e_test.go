//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

const (
	// indexTakesNameBuilt holds the index the plan turns into a UNIQUE under its own name.
	indexTakesNameBuilt = "CREATE TABLE t (id int PRIMARY KEY, a int);\nCREATE INDEX u ON t (a);\n"
	// indexTakesNameDesired declares the UNIQUE.
	indexTakesNameDesired = "CREATE TABLE t (id int PRIMARY KEY, a int, CONSTRAINT u UNIQUE (a));\n"
	// mysqlFamilyIndexUnique prints whether the server holds u as a unique index.
	mysqlFamilyIndexUnique = "SELECT concat(index_name, ' unique=', IF(non_unique = 0, 'true', 'false')) FROM information_schema.statistics " +
		"WHERE table_schema = database() AND table_name = 't' AND index_name <> 'PRIMARY'"
	// postgresIndexUnique prints whether the server holds u as a unique index.
	postgresIndexUnique = "SELECT c.relname || ' unique=' || i.indisunique FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid " +
		"WHERE i.indrelid = 'public.t'::regclass AND NOT i.indisprimary"
)

// indexTakesNameRoundTrip applies the plan through a migration at target, reads the index
// back, runs a second plan, rolls back and reads the index again. The rollback adds the
// index back and removes the UNIQUE under one name, and with the index ahead of the
// removal the server refuses it.
func indexTakesNameRoundTrip(c *qt.C, target, read string, schemaFlags ...string) {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(indexTakesNameDesired), 0o600), qt.IsNil)
	generate := append([]string{"migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", dir, "--name", "key"}, schemaFlags...)
	generated := runPtahIn(c, generate...)
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(columnKeyString(c, target, read), qt.Equals, "u unique=true")
	again := runPtahIn(c, append([]string{"schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run"}, schemaFlags...)...)
	c.Assert(again.Stdout, qt.Contains, "Schema is synced")

	down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(columnKeyString(c, target, read), qt.Equals, "u unique=false")
}

// TestMigrationsUniqueNameAnAddedIndexTakesE2E_MySQLFamily runs the plan on MySQL and
// MariaDB, which answer Error 1061 with the index ahead of the removal.
func TestMigrationsUniqueNameAnAddedIndexTakesE2E_MySQLFamily(t *testing.T) {
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{"mysql", dbtarget.MySQLAdmin},
		{"mariadb", dbtarget.MariaDBAdmin},
	}
	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			_, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c, indexTakesNameBuilt)

			indexTakesNameRoundTrip(c, target, mysqlFamilyIndexUnique)
		})
	}
}

// TestMigrationsUniqueNameAnAddedIndexTakesE2E_PostgreSQL runs the plan on PostgreSQL,
// which answers `relation "u" already exists` with the index ahead of the removal.
func TestMigrationsUniqueNameAnAddedIndexTakesE2E_PostgreSQL(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, indexTakesNameBuilt)

	indexTakesNameRoundTrip(c, target, postgresIndexUnique, "--schemas", "public")
}
