//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A PostgreSQL table changes between logged and unlogged with one ALTER
// TABLE, and the diff after the apply is in sync. YugabyteDB takes the
// UNLOGGED keyword and creates an ordinary table, so Ptah creates an ordinary
// table there and plans nothing more for it.

// unloggedDocument declares the table cache, logged or unlogged.
func unloggedDocument(unlogged bool) string {
	return map[bool]string{
		true:  "CREATE UNLOGGED TABLE cache (id bigint PRIMARY KEY, v text);\n",
		false: "CREATE TABLE cache (id bigint PRIMARY KEY, v text);\n",
	}[unlogged]
}

// postgresUnloggedDatabase is a scratch PostgreSQL database.
func postgresUnloggedDatabase(c *qt.C) devDialectDatabase {
	c.Helper()
	devURL := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL),
		"DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
	return devDialectDatabase{url: devURL, conn: connectDevDialect(c, devURL)}
}

// cachePersistence reads pg_class.relpersistence of cache: 'u' for an
// unlogged table and 'p' for an ordinary one.
func cachePersistence(c *qt.C, dev devDialectDatabase) string {
	c.Helper()
	var persistence string
	c.Assert(dev.conn.QueryRowContext(c.Context(),
		"SELECT relpersistence::text FROM pg_class WHERE oid = 'cache'::regclass").Scan(&persistence), qt.IsNil)
	return persistence
}

// applyUnloggedDocument applies the document for unlogged and diffs the
// database against it again.
func applyUnloggedDocument(c *qt.C, dev devDialectDatabase, unlogged bool) {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(unloggedDocument(unlogged)), 0o600), qt.IsNil)

	out, err := runPtahNativeWithError("schema", "apply", "--db-url", dev.url, "--schema-file", path, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runPtahNativeWithError("schema", "diff", "--from", dev.url, "--to", path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schemas are synced", qt.Commentf("%s", out))
}

// TestSchemaApplyChangesATableBetweenLoggedAndUnloggedPostgresE2E creates the
// table unlogged, makes it logged, and makes it unlogged again.
func TestSchemaApplyChangesATableBetweenLoggedAndUnloggedPostgresE2E(t *testing.T) {
	c := qt.New(t)
	dev := postgresUnloggedDatabase(c)

	applyUnloggedDocument(c, dev, true)
	c.Assert(cachePersistence(c, dev), qt.Equals, "u")

	applyUnloggedDocument(c, dev, false)
	c.Assert(cachePersistence(c, dev), qt.Equals, "p")

	applyUnloggedDocument(c, dev, true)
	c.Assert(cachePersistence(c, dev), qt.Equals, "u")
}

// TestSchemaApplyAnUnloggedTableYugabyteDBE2E applies an unlogged declaration
// to YugabyteDB, which holds the table as an ordinary one, and a second diff
// is in sync rather than planning SET UNLOGGED again.
func TestSchemaApplyAnUnloggedTableYugabyteDBE2E(t *testing.T) {
	c := qt.New(t)
	dev := yugabyteDevDatabase(c)

	applyUnloggedDocument(c, dev, true)
	c.Assert(cachePersistence(c, dev), qt.Equals, "p")
}
