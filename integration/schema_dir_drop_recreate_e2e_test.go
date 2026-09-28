//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// schemaDirDropRecreateFiles is a schema directory whose later file drops the
// table the earlier file created and creates it again. The files run in order,
// so PostgreSQL 18.6 builds users with both columns, and Atlas CE v1.3.0 exits
// 0 on the directory. Without the replay the reader refuses it as a second
// declaration of users (stokaro/ptah#3914).
var schemaDirDropRecreateFiles = []struct {
	name string
	sql  string
}{
	{name: "1_a.sql", sql: "CREATE TABLE users (id int PRIMARY KEY);\n"},
	{name: "2_b.sql", sql: "DROP TABLE users;\nCREATE TABLE users (id int PRIMARY KEY, email text);\n"},
}

// TestSchemaApplyFindsATableADirectoryDropsAndCreatesSyncedE2E plans the
// directory against the database its files build, natively and through
// ptah-compat: both are synced.
func TestSchemaApplyFindsATableADirectoryDropsAndCreatesSyncedE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	var script strings.Builder
	for _, file := range schemaDirDropRecreateFiles {
		c.Assert(os.WriteFile(filepath.Join(dir, file.name), []byte(file.sql), 0o600), qt.IsNil)
		script.WriteString(file.sql)
	}
	target := postgresUniqueEngine.builtDB(c, script.String())

	native := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", dir, "--dry-run")
	compat, err := runCompatVerb("schema", "apply", "-u", target, "--to", "file://"+dir,
		"--dev-url", postgresUniqueEngine.emptyDB(c), "--dry-run")

	c.Assert(native, qt.Contains, "Schema is synced")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", compat))
	c.Assert(compat, qt.Contains, "Schema is synced")
}
