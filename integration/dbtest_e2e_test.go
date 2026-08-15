//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/root"
	"go.5x5.cz/ptah/internal/dbtarget"
)

func TestDatabaseTestRunnersPostgresE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	migrationDBName := fmt.Sprintf("ptah_migration_test_e2e_%d", time.Now().UnixNano())
	schemaDBName := fmt.Sprintf("ptah_schema_test_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, migrationDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, migrationDBName)
	createE2EDatabase(c, ctx, adminDB, schemaDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, schemaDBName)

	runLiveMigrationTest(c, ctx, replaceDatabaseName(c, adminURL, migrationDBName))
	runLiveSchemaTest(c, ctx, replaceDatabaseName(c, adminURL, schemaDBName))
}

func runLiveMigrationTest(c *qt.C, ctx context.Context, databaseURL string) {
	c.Helper()
	migrationsDir := c.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_create_widgets.up.sql"),
		[]byte(`CREATE TYPE status_type AS ENUM ('draft', 'published', 'legacy');
CREATE TABLE widgets (id SERIAL PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE legacy_records (id SERIAL PRIMARY KEY, status status_type NOT NULL);
INSERT INTO legacy_records (status) VALUES ('legacy');
`),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_create_widgets.down.sql"),
		[]byte(`DROP TABLE legacy_records;
DROP TABLE widgets;
DROP TYPE status_type;
`),
		0o600,
	), qt.IsNil)

	rootDir := c.TempDir()
	writeLiveTestEntity(c, rootDir, "audit_events")
	writeLiveTestEnum(c, rootDir)
	seedDir := c.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(seedDir, "010_widgets.test.sql"),
		[]byte("INSERT INTO widgets (name) VALUES ('gear');\n"),
		0o600,
	), qt.IsNil)
	testDir := writeLiveTestCases(c, `cases:
  - name: live migration and schema steps
    steps:
      - name: migrate
        migrate_to: latest
      - name: apply desired schema
        apply_schema: true
      - name: seed widget
        seed:
          env: test
      - name: widget exists
        assert:
          query: SELECT name FROM widgets
          scalar: gear
      - name: desired table exists
        assert:
          query: SELECT id FROM audit_events
          row_count: 0
      - name: migration enum value remains available
        assert:
          query: SELECT status FROM legacy_records
          scalar: legacy
`)
	output := runLivePtahCommand(c, ctx,
		"migrations", "test",
		"--dir", testDir,
		"--migrations-dir", migrationsDir,
		"--root-dir", rootDir,
		"--seed-dir", seedDir,
		"--db-url", databaseURL,
		"--dir-format", "ptah",
	)
	c.Assert(output, qt.Contains, `PASS  case "live migration and schema steps"`)
}

func runLiveSchemaTest(c *qt.C, ctx context.Context, databaseURL string) {
	c.Helper()
	rootDir := c.TempDir()
	writeLiveTestEntity(c, rootDir, "users")
	seedDir := c.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(seedDir, "010_users.test.sql"),
		[]byte("INSERT INTO users (name) VALUES ('ada');\n"),
		0o600,
	), qt.IsNil)
	testDir := writeLiveTestCases(c, `cases:
  - name: live desired schema
    steps:
      - name: schema is provisioned
        apply_schema: true
      - name: seed user
        seed:
          env: test
      - name: user exists
        assert:
          query: SELECT name FROM users
          scalar: ada
      - name: drop desired table
        exec: DROP TABLE users
      - name: restore desired schema
        apply_schema: true
      - name: restored table is empty
        assert:
          query: SELECT id FROM users
          row_count: 0
`)
	output := runLivePtahCommand(c, ctx,
		"schema", "test",
		"--dir", testDir,
		"--root-dir", rootDir,
		"--seed-dir", seedDir,
		"--db-url", databaseURL,
	)
	c.Assert(output, qt.Contains, `PASS  case "live desired schema"`)
}

func writeLiveTestCases(c *qt.C, contents string) string {
	c.Helper()
	testDir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(testDir, "cases.yaml"), []byte(contents), 0o600), qt.IsNil)
	return testDir
}

func runLivePtahCommand(c *qt.C, ctx context.Context, args ...string) string {
	c.Helper()
	cmd := root.NewRootCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(ctx)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output.String()))
	return output.String()
}

func writeLiveTestEntity(c *qt.C, dir, table string) {
	c.Helper()
	content := fmt.Sprintf(`package models

//ptah:schema:table name=%q
type Entity struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="TEXT"
	Name string
}
`, table)
	c.Assert(os.WriteFile(filepath.Join(dir, "entity.go"), []byte(content), 0o600), qt.IsNil)
}

func writeLiveTestEnum(c *qt.C, dir string) {
	c.Helper()
	content := `package models

//ptah:schema:enum name="status_type" values="draft,published"
type Status string
`
	c.Assert(os.WriteFile(filepath.Join(dir, "enum.go"), []byte(content), 0o600), qt.IsNil)
}
