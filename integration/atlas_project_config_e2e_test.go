//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestAtlasProjectConfigMigrateStatusAndUpE2E(t *testing.T) {
	dbURL := postgresE2EDatabaseURL(t)
	if dbURL == "" {
		t.Skip("POSTGRES_TEST_DSN, POSTGRES_URL, or TEST_DATABASE_URL is not set")
	}
	if !strings.HasPrefix(dbURL, "postgres://") && !strings.HasPrefix(dbURL, "postgresql://") {
		t.Skip("PostgreSQL URL required for atlas.hcl project config e2e test")
	}

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c, ctx, repoRoot, binaryPath)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testDBName := fmt.Sprintf("ptah_atlas_project_config_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

	workDir := t.TempDir()
	writeAtlasProjectConfigFixture(c, workDir, replaceDatabaseName(c, dbURL, testDBName))

	output, err := runPtahInDir(ctx, workDir, binaryPath, "migrations", "status", "--env", "local", "--json")
	c.Assert(err, qt.IsNil, qt.Commentf("migrations status output:\n%s", output))
	c.Assert(readStatusField(c, output, "total_migrations"), qt.Equals, float64(1))
	c.Assert(readStatusField(c, output, "has_pending_changes"), qt.Equals, true)

	output, err = runPtahInDir(ctx, workDir, binaryPath, "migrations", "up", "--env", "local")
	c.Assert(err, qt.IsNil, qt.Commentf("migrations up output:\n%s", output))
	c.Assert(output, qt.Contains, "Migration directory format: atlas")
	c.Assert(output, qt.Contains, "Database is now at version: 20260719010101")

	output, err = runPtahInDir(ctx, workDir, binaryPath, "migrations", "status", "--env", "local", "--json")
	c.Assert(err, qt.IsNil, qt.Commentf("final migrations status output:\n%s", output))
	c.Assert(readStatusField(c, output, "current_version"), qt.Equals, float64(20260719010101))
	c.Assert(readStatusField(c, output, "has_pending_changes"), qt.Equals, false)

	verifyAtlasProjectConfigDatabaseState(c, ctx, replaceDatabaseName(c, dbURL, testDBName))
}

func writeAtlasProjectConfigFixture(c *qt.C, workDir, dbURL string) {
	migrationsDir := filepath.Join(workDir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(workDir, "atlas.hcl"), []byte(fmt.Sprintf(`env "local" {
  url = %q
  migration {
    dir              = "file://migrations"
    revisions_schema = "ptah_issue_276"
    lock_timeout     = "3s"
    exec_order       = "linear"
  }
}
`, dbURL)), 0600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "atlas.sum"), []byte(
		"h1:directory\n"+
			"20260719010101_create_project_config_widgets.sql h1:ptahissue276\n",
	), 0600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "20260719010101_create_project_config_widgets.sql"),
		[]byte("CREATE TABLE ptah_issue_276_widgets (id INT PRIMARY KEY);\n"),
		0600,
	), qt.IsNil)
}

func runPtahInDir(ctx context.Context, dir, binaryPath string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func readStatusField(c *qt.C, output, field string) any {
	var payload map[string]any
	c.Assert(json.Unmarshal([]byte(output), &payload), qt.IsNil, qt.Commentf("status output:\n%s", output))
	return payload[field]
}

func verifyAtlasProjectConfigDatabaseState(c *qt.C, ctx context.Context, dbURL string) {
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	var tableName string
	err = db.QueryRowContext(ctx, `SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = 'ptah_issue_276_widgets'`).Scan(&tableName)
	c.Assert(err, qt.IsNil)
	c.Assert(tableName, qt.Equals, "ptah_issue_276_widgets")

	var version, hash string
	err = db.QueryRowContext(ctx, `SELECT version, hash
FROM ptah_issue_276.atlas_schema_revisions
WHERE version = '20260719010101'`).Scan(&version, &hash)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, "20260719010101")
	c.Assert(hash, qt.Equals, "ptahissue276")
}
