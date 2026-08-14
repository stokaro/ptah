package migrate_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migrate"
	"go.5x5.cz/ptah/dbschema"
)

func runGenerate(args ...string) (string, error) {
	cmd := migrate.NewMigrateGenerateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// writeReplayFixture writes a ptah-format migration directory plus a desired
// schema file that adds one table on top of the replayed history.
func writeReplayFixture(t *testing.T) (migrationsDir, schemaPath string) {
	t.Helper()
	c := qt.New(t)
	migrationsDir = t.TempDir()
	files := map[string]string{
		"0000000001_users.up.sql":   "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"0000000001_users.down.sql": "DROP TABLE users;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, name), []byte(content), 0o600), qt.IsNil)
	}
	schemaPath = filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
	), 0o600), qt.IsNil)
	return migrationsDir, schemaPath
}

func TestMigrateGenerateReplayDerivesCurrentStateFromDirectory(t *testing.T) {
	c := qt.New(t)
	migrationsDir, schemaPath := writeReplayFixture(t)
	devPath := filepath.Join(t.TempDir(), "dev.db")

	out, err := runGenerate(
		"--replay",
		"--dev-url", "sqlite://"+devPath,
		"--migrations-dir", migrationsDir,
		"--schema-file", schemaPath,
		"--name", "add_orders",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "UP:")
	c.Assert(out, qt.Contains, "DOWN:")

	matches, err := filepath.Glob(filepath.Join(migrationsDir, "*_add_orders.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
	upSQL, err := os.ReadFile(matches[0])
	c.Assert(err, qt.IsNil)
	// The replayed state already contains users, so only orders is created.
	c.Assert(string(upSQL), qt.Contains, `CREATE TABLE "orders"`)
	c.Assert(string(upSQL), qt.Not(qt.Contains), `CREATE TABLE "users"`)
	assertGenerateReplayDevEmpty(c, devPath)
}

func assertGenerateReplayDevEmpty(c *qt.C, path string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(c.Context(), `
		SELECT COUNT(*)
		FROM main.sqlite_schema
		WHERE name NOT LIKE 'sqlite_%'
	`).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

func TestMigrateGenerateReplayRequiresDevURL(t *testing.T) {
	c := qt.New(t)
	migrationsDir, schemaPath := writeReplayFixture(t)

	out, err := runGenerate(
		"--replay",
		"--migrations-dir", migrationsDir,
		"--schema-file", schemaPath,
	)

	c.Assert(err, qt.ErrorMatches, "--dev-url is required with --replay", qt.Commentf("%s", out))
}

func TestMigrateGenerateReplayRejectsDBURL(t *testing.T) {
	c := qt.New(t)
	migrationsDir, schemaPath := writeReplayFixture(t)

	out, err := runGenerate(
		"--replay",
		"--db-url", "sqlite://"+filepath.Join(t.TempDir(), "target.db"),
		"--dev-url", "sqlite://"+filepath.Join(t.TempDir(), "dev.db"),
		"--migrations-dir", migrationsDir,
		"--schema-file", schemaPath,
	)

	c.Assert(err, qt.ErrorMatches, "--db-url cannot be combined with --replay: .*", qt.Commentf("%s", out))
}

func TestMigrateGenerateDevURLRequiresReplay(t *testing.T) {
	c := qt.New(t)

	out, err := runGenerate(
		"--dev-url", "sqlite://"+filepath.Join(t.TempDir(), "dev.db"),
		"--db-url", "sqlite://"+filepath.Join(t.TempDir(), "target.db"),
		"--migrations-dir", t.TempDir(),
	)

	c.Assert(err, qt.ErrorMatches, "--dev-url requires --replay", qt.Commentf("%s", out))
}

func TestMigrateGenerateQualifierRejectsUnsupportedDialect(t *testing.T) {
	c := qt.New(t)
	migrationsDir, schemaPath := writeReplayFixture(t)
	devPath := filepath.Join(t.TempDir(), "dev.db")

	out, err := runGenerate(
		"--replay",
		"--dev-url", "sqlite://"+devPath,
		"--migrations-dir", migrationsDir,
		"--schema-file", schemaPath,
		"--qualifier", "tenant",
	)

	c.Assert(err, qt.ErrorMatches, `--qualifier is not supported for dialect "sqlite"`, qt.Commentf("%s", out))
}

func TestMigrateGenerateQualifierRejectsInvalidValue(t *testing.T) {
	c := qt.New(t)

	out, err := runGenerate(
		"--db-url", "sqlite://"+filepath.Join(t.TempDir(), "target.db"),
		"--migrations-dir", t.TempDir(),
		"--qualifier", "a.b",
	)

	c.Assert(err, qt.ErrorMatches, `invalid --qualifier "a\.b": .*`, qt.Commentf("%s", out))
}
