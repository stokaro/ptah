//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A primary key declared as a PRIMARY KEY constraint, applied twice
// (stokaro/ptah#3959). The second apply is where the key has to meet its
// declaration: MySQL and MariaDB call it PRIMARY whatever it was declared as,
// and PostgreSQL keeps the declared name or gives an unnamed key
// `<table>_pkey`.

// primaryKeyConstraintEntities writes table hk with its primary key declared
// as a constraint, nameAttribute written before its type, and returns the
// directory.
func primaryKeyConstraintEntities(c *qt.C, nameAttribute string) string {
	c.Helper()
	dir := c.TempDir()
	source := fmt.Sprintf(`package models

//ptah:schema:table name="hk"
type HK struct {
	//ptah:schema:field name="id" type="INT" not_null="true"
	ID int
	//ptah:schema:field name="a" type="INT"
	A int
	//ptah:schema:constraint %stype="PRIMARY KEY" columns="id"
	_ int
}
`, nameAttribute)
	c.Assert(os.WriteFile(filepath.Join(dir, "models.go"), []byte(source), 0o600), qt.IsNil)
	return dir
}

// postgresPrimaryKeyName reads the name of the primary key of hk.
func postgresPrimaryKeyName(c *qt.C, url string) string {
	c.Helper()
	conn, err := sql.Open("pgx", url)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	var name string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT conname FROM pg_constraint WHERE conrelid = 'hk'::regclass AND contype = 'p'").Scan(&name), qt.IsNil)
	return name
}

// primaryKeyDeclarations are the two spellings of the declaration.
var primaryKeyDeclarations = []struct {
	name          string
	nameAttribute string
	postgresName  string
}{
	{name: "named", nameAttribute: `name="hk_pk" `, postgresName: "hk_pk"},
	{name: "unnamed", postgresName: "hk_pkey"},
}

// TestSchemaApplyReappliesADeclaredPrimaryKeyOnMySQLFamilyE2E applies the
// declaration twice: the second apply succeeds and the schema is then synced.
func TestSchemaApplyReappliesADeclaredPrimaryKeyOnMySQLFamilyE2E(t *testing.T) {
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "MySQL", engine: dbtarget.MySQLAdmin},
		{name: "MariaDB", engine: dbtarget.MariaDBAdmin},
	}
	for _, engine := range engines {
		for _, declaration := range primaryKeyDeclarations {
			t.Run(engine.name+" "+declaration.name, func(t *testing.T) {
				c := qt.New(t)
				_, target := newMySQLFamilyScratch(c, engine.engine).database(c, "hk_reapply")
				entities := primaryKeyConstraintEntities(c, declaration.nameAttribute)

				runPtahNative(c, "schema", "apply", "--db-url", target, "--root-dir", entities, "--auto-approve")
				runPtahNative(c, "schema", "apply", "--db-url", target, "--root-dir", entities, "--auto-approve")

				c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--root-dir", entities, "--dry-run"),
					qt.Contains, "Schema is synced")
			})
		}
	}
}

// TestSchemaApplyReappliesADeclaredPrimaryKeyOnPostgreSQLE2E applies the
// declaration twice on PostgreSQL: the key carries the declared name, or the
// server's for an unnamed one, and the schema is then synced.
func TestSchemaApplyReappliesADeclaredPrimaryKeyOnPostgreSQLE2E(t *testing.T) {
	for _, declaration := range primaryKeyDeclarations {
		t.Run(declaration.name, func(t *testing.T) {
			c := qt.New(t)
			target := postgresScratchDevURL(c, "hk_reapply")
			entities := primaryKeyConstraintEntities(c, declaration.nameAttribute)

			runPtahNative(c, "schema", "apply", "--db-url", target, "--root-dir", entities, "--auto-approve")
			runPtahNative(c, "schema", "apply", "--db-url", target, "--root-dir", entities, "--auto-approve")

			c.Assert(postgresPrimaryKeyName(c, target), qt.Equals, declaration.postgresName)
			c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--root-dir", entities, "--dry-run"),
				qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyRenamesAPrimaryKeyOnPostgreSQLE2E applies a declaration
// naming the key hk_pk to a table whose key PostgreSQL named hk_pkey: the old
// key is dropped before the new one is added, and the schema is then synced.
func TestSchemaApplyRenamesAPrimaryKeyOnPostgreSQLE2E(t *testing.T) {
	c := qt.New(t)
	target := postgresScratchDevURL(c, "hk_rename")
	conn, err := sql.Open("pgx", target)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	_, err = conn.ExecContext(c.Context(), "CREATE TABLE hk (id int NOT NULL, a int, PRIMARY KEY (id))")
	c.Assert(err, qt.IsNil)
	entities := primaryKeyConstraintEntities(c, `name="hk_pk" `)

	runPtahNative(c, "schema", "apply", "--db-url", target, "--root-dir", entities, "--auto-approve")

	c.Assert(postgresPrimaryKeyName(c, target), qt.Equals, "hk_pk")
	c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--root-dir", entities, "--dry-run"),
		qt.Contains, "Schema is synced")
}
