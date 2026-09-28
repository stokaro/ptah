//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// outsideDatabaseFile writes a schema file putting table u in database other,
// beside a table t the URL's database can hold.
func outsideDatabaseFile(c *qt.C, other string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE TABLE `"+other+"`.u (id int PRIMARY KEY);\n"+
		"CREATE TABLE t (id int PRIMARY KEY);\n"), 0o600), qt.IsNil)
	return path
}

// TestSchemaVerbsRefuseATableOutsideTheURLDatabaseE2E applies and diffs a
// schema file that puts a table in another database, against a URL naming one
// database, and reads the catalog back: nothing is created. Without the
// refusal, `ptah schema apply --auto-approve` creates the table in a database
// the URL does not name. The pinned community binary v1.3.0 refuses the file,
// measured on MySQL 8.4.11: `Error 1049 (42000): Unknown database 'other'`
// (stokaro/ptah#3928).
func TestSchemaVerbsRefuseATableOutsideTheURLDatabaseE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			_, url := scratch.database(c, "scope")
			_, dev := scratch.database(c, "scope_dev")
			other := fmt.Sprintf("ptah_other_%d", time.Now().UnixNano())
			c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), scratch.admin, other) })
			createMySQLDatabase(c, c.Context(), scratch.admin, other)
			schema := outsideDatabaseFile(c, other)

			native, nativeErr := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", schema, "--auto-approve")
			applied, appliedErr := runCompatVerb("schema", "apply", "--url", url, "--to", "file://"+schema, "--dev-url", dev, "--auto-approve")
			diffed, diffedErr := runCompatVerb("schema", "diff", "--from", url, "--to", "file://"+schema, "--dev-url", dev)

			refusal := `(?s).*table "` + other + `.u" is in database "` + other + `", but %s is limited to database "ptah_scope_\d+".*`
			c.Assert(nativeErr, qt.ErrorMatches, fmt.Sprintf(refusal, "the target URL"), qt.Commentf("%s", native))
			c.Assert(appliedErr, qt.ErrorMatches, fmt.Sprintf(refusal, "the target URL"), qt.Commentf("%s", applied))
			c.Assert(diffedErr, qt.ErrorMatches, fmt.Sprintf(refusal, "--from"), qt.Commentf("%s", diffed))
			c.Assert(scratch.tablesOf(c, other), qt.Equals, "")
		})
	}
}

// TestSchemaApplyTakesATableQualifiedWithTheURLDatabaseE2E is the control: a
// table qualified with the database the URL names is that database's, and is
// created there.
func TestSchemaApplyTakesATableQualifiedWithTheURLDatabaseE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			name, url := scratch.database(c, "scope")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte("CREATE TABLE `"+name+"`.t (id int PRIMARY KEY);\n"), 0o600), qt.IsNil)

			out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", schema, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(scratch.tablesOf(c, name), qt.Equals, "t")
		})
	}
}
