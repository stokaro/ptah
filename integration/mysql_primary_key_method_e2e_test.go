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

// A MySQL-family primary key built USING HASH (stokaro/ptah#3853). MariaDB
// 11.8.9 keeps the method: INDEX_TYPE HASH, and SHOW CREATE TABLE prints it
// back. MySQL 8.4.11 builds BTREE on InnoDB and drops the clause.

// hashKeyFile is a table whose primary key asks for HASH.
const hashKeyFile = "CREATE TABLE hk (id int NOT NULL, a int, PRIMARY KEY (id) USING HASH);\n"

// plainKeyFile is the same table with a primary key asking for no method.
const plainKeyFile = "CREATE TABLE hk (id int NOT NULL, a int, PRIMARY KEY (id));\n"

// primaryKeyMethodEngines are the engines the rows run on, with the index type
// each builds for hashKeyFile.
var primaryKeyMethodEngines = []struct {
	name     string
	engine   dbtarget.Engine
	wantType string
}{
	{name: "MySQL", engine: dbtarget.MySQLAdmin, wantType: "BTREE"},
	{name: "MariaDB", engine: dbtarget.MariaDBAdmin, wantType: "HASH"},
}

// primaryIndexType reads the INDEX_TYPE the server reports for the PRIMARY
// index of hk in the database dsn names.
func primaryIndexType(c *qt.C, dsn string) string {
	c.Helper()
	conn, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	var indexType string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT INDEX_TYPE FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() "+
			"AND TABLE_NAME = 'hk' AND INDEX_NAME = 'PRIMARY'").Scan(&indexType), qt.IsNil)
	return indexType
}

// writeKeyFile writes contents as a schema file.
func writeKeyFile(c *qt.C, contents string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(contents), 0o600), qt.IsNil)
	return path
}

// TestSchemaApplyBuildsAPrimaryKeyMethodE2E applies the file to an empty
// database and reads the key back: each engine builds what the file's own SQL
// builds, and the next apply is synced. On MySQL that is BTREE, which answers
// a key asking for HASH the way MySQL's own build of the same statement does.
func TestSchemaApplyBuildsAPrimaryKeyMethodE2E(t *testing.T) {
	for _, engine := range primaryKeyMethodEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			name, target := scratch.database(c, "hk_target")
			schema := writeKeyFile(c, hashKeyFile)

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(primaryIndexType(c, mySQLDSNForDatabase(c, scratch.adminDSN, name)), qt.Equals, engine.wantType)
			c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run"),
				qt.Contains, "Schema is synced")
		})
	}
}

// hashKeyEntity is table hk with its primary key declared as a PRIMARY KEY
// constraint asking for HASH, the spelling a Go annotation, a YAML document and
// an HCL constraint block share.
const hashKeyEntity = `package models

//ptah:schema:table name="hk"
type HK struct {
	//ptah:schema:field name="id" type="INT" not_null="true"
	ID int
	//ptah:schema:field name="a" type="INT"
	A int
	//ptah:schema:constraint name="hk_pk" type="PRIMARY KEY" columns="id" using="HASH"
	_ int
}
`

// TestSchemaApplyBuildsAPrimaryKeyConstraintMethodE2E applies the key spelled
// as a constraint to an empty database: each engine builds what the table
// spelling builds. A second apply of a key declared this way is
// stokaro/ptah#3959, whatever its method.
func TestSchemaApplyBuildsAPrimaryKeyConstraintMethodE2E(t *testing.T) {
	for _, engine := range primaryKeyMethodEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			name, target := scratch.database(c, "hk_constraint")
			entities := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(entities, "models.go"), []byte(hashKeyEntity), 0o600), qt.IsNil)

			runPtahNative(c, "schema", "apply", "--db-url", target, "--root-dir", entities, "--auto-approve")

			c.Assert(primaryIndexType(c, mySQLDSNForDatabase(c, scratch.adminDSN, name)), qt.Equals, engine.wantType)
		})
	}
}

// TestSchemaApplyRebuildsAPrimaryKeyForItsMethodE2E applies the file to a
// MariaDB database whose key is BTREE: the plan drops the key and adds it
// USING HASH, and the server then reports HASH.
func TestSchemaApplyRebuildsAPrimaryKeyForItsMethodE2E(t *testing.T) {
	c := qt.New(t)
	scratch := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin)
	name, target := scratch.builtFrom(c, plainKeyFile)
	schema := writeKeyFile(c, hashKeyFile)

	plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

	c.Assert(plan, qt.Contains, "DROP PRIMARY KEY")
	c.Assert(plan, qt.Contains, "PRIMARY KEY (`id`) USING HASH")
	c.Assert(primaryIndexType(c, mySQLDSNForDatabase(c, scratch.adminDSN, name)), qt.Equals, "HASH")
	c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run"),
		qt.Contains, "Schema is synced")
}

// TestDescriptionsCarryAPrimaryKeyMethodE2E reads a MariaDB database whose key
// is HASH, natively as SQL and through ptah-compat as HCL, where the pinned
// community binary v1.3.0 writes `type = HASH`. The SQL description applied to
// an empty database builds the same key.
func TestDescriptionsCarryAPrimaryKeyMethodE2E(t *testing.T) {
	c := qt.New(t)
	scratch := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin)
	_, source := scratch.builtFrom(c, hashKeyFile)
	name, target := scratch.database(c, "hk_copy")

	read, readErr, err := runPtahSplitStreams(c.Context(), []string{"db", "read", "--db-url", source})
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", readErr))
	inspected, err := runCompatVerb("schema", "inspect", "-u", source)
	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", writeKeyFile(c, read), "--auto-approve")

	c.Assert(read, qt.Contains, "USING HASH")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", inspected))
	c.Assert(inspected, qt.Matches, `(?s).*primary_key \{\n    columns = \[column\.id\]\n    type\s+= HASH\n  \}.*`)
	c.Assert(primaryIndexType(c, mySQLDSNForDatabase(c, scratch.adminDSN, name)), qt.Equals, "HASH")
}
