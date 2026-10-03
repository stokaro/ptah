//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// The rollback of a ClickHouse migration that gave a column a default has to
// take the default away again. `ptah migrations generate` wrote that down
// migration as a MODIFY COLUMN naming only the type, which keeps the default,
// and `migrations down` reported success while system.columns still showed it
// (stokaro/ptah#4030).

// clickHouseColumnDefault reads n's default kind and expression from
// system.columns.
func clickHouseColumnDefault(c *qt.C, url string) string {
	c.Helper()
	conn := connectDevDialect(c, url)
	var kind, expression string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT default_kind, default_expression FROM system.columns WHERE database = currentDatabase() AND table = 'asn' AND name = 'n'",
	).Scan(&kind, &expression), qt.IsNil)
	return kind + " " + expression
}

func TestMigrationsClickHouseRollbackRemovesADefaultE2E(t *testing.T) {
	c := qt.New(t)
	url := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
	_, err := connectDevDialect(c, url).ExecContext(c.Context(),
		"CREATE TABLE asn (id Int32, n Int32) ENGINE = MergeTree ORDER BY id")
	c.Assert(err, qt.IsNil)
	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
	schema := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schema,
		[]byte("CREATE TABLE asn (id Int32, n Int32 DEFAULT 0) ENGINE = MergeTree ORDER BY id;\n"), 0o600), qt.IsNil)

	runPtahNative(c, "migrations", "generate", "--db-url", url, "--migrations-dir", migrations,
		"--schema-file", schema, "--name", "add_default")
	downs, err := filepath.Glob(filepath.Join(migrations, "*.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(downs, qt.HasLen, 1)
	down, err := os.ReadFile(downs[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(down), qt.Contains, "ALTER TABLE asn MODIFY COLUMN n REMOVE DEFAULT;")

	runPtahNative(c, "migrations", "up", "--db-url", url, "--migrations-dir", migrations)
	c.Assert(clickHouseColumnDefault(c, url), qt.Equals, "DEFAULT '0'")

	runPtahNative(c, "migrations", "down", "--db-url", url, "--migrations-dir", migrations, "--target", "0", "--confirm")
	c.Assert(clickHouseColumnDefault(c, url), qt.Equals, " ")
}
