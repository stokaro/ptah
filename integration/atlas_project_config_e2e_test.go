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

	"go.5x5.cz/ptah/internal/dbtarget"
)

const (
	atlasProjectConfigSeedVersion    = "20260719010000"
	atlasProjectConfigPendingVersion = "20260719010101"
	atlasProjectConfigSeedHash       = "BH+RgWEaFyoTPktaYRIv/patf+c8tCfnN+p6QfFNmR0="
	atlasProjectConfigPendingHash    = "JEFa2gqj5DNU9CSHe+Qmj7tnAsZeeRxI0pwuvymOX+0="
)

type atlasProjectConfigRevision struct {
	Version         string
	Description     string
	Type            int
	Applied         int
	Total           int
	Error           sql.NullString
	ErrorStatement  sql.NullString
	Hash            string
	PartialHashes   sql.NullString
	OperatorVersion string
}

type atlasProjectConfigRevisionTiming struct {
	ExecutedAt    time.Time
	ExecutionTime int64
}

func TestAtlasProjectConfigMigrateStatusAndUpE2E(t *testing.T) {
	dbURL := requireAtlasProjectConfigPostgresURL(t)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c.TB, ctx, repoRoot, binaryPath)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testDBName := fmt.Sprintf("ptah_atlas_project_config_%d", time.Now().UnixNano())
	createE2EDatabase(c.TB, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c.TB, context.Background(), adminDB, testDBName)

	workDir := t.TempDir()
	testDBURL := replaceDatabaseName(c.TB, dbURL, testDBName)
	t.Setenv("PTAH_ATLAS_PROJECT_CONFIG_E2E_URL", testDBURL)
	writeAtlasProjectConfigFixture(c.TB, repoRoot, workDir)
	seedAtlasProjectConfigDatabaseState(c.TB, ctx, testDBURL)

	output, err := runPtahInDir(ctx, workDir, binaryPath, "migrations", "status", "--env", "local", "--json")
	c.Assert(err, qt.IsNil, qt.Commentf("migrations status output:\n%s", output))
	c.Assert(readStatusField(c.TB, output, "total_migrations"), qt.Equals, float64(2))
	c.Assert(readStatusField(c.TB, output, "current_version"), qt.Equals, float64(20260719010000))
	c.Assert(readStatusField(c.TB, output, "applied_migrations"), qt.DeepEquals, []any{float64(20260719010000)})
	c.Assert(readStatusField(c.TB, output, "pending_migrations"), qt.DeepEquals, []any{float64(20260719010101)})
	c.Assert(readStatusField(c.TB, output, "has_pending_changes"), qt.Equals, true)

	applyStarted := time.Now()
	output, err = runPtahInDir(ctx, workDir, binaryPath, "migrations", "up", "--env", "local", "--verify-sum")
	applyFinished := time.Now()
	c.Assert(err, qt.IsNil, qt.Commentf("migrations up output:\n%s", output))
	c.Assert(output, qt.Contains, "Migration directory format: atlas")
	c.Assert(output, qt.Contains, "Database is now at version: 20260719010101")

	output, err = runPtahInDir(ctx, workDir, binaryPath, "migrations", "status", "--env", "local", "--json")
	c.Assert(err, qt.IsNil, qt.Commentf("final migrations status output:\n%s", output))
	c.Assert(readStatusField(c.TB, output, "current_version"), qt.Equals, float64(20260719010101))
	c.Assert(readStatusField(c.TB, output, "applied_migrations"), qt.DeepEquals, []any{
		float64(20260719010000),
		float64(20260719010101),
	})
	c.Assert(readStatusField(c.TB, output, "pending_migrations"), qt.DeepEquals, []any{})
	c.Assert(readStatusField(c.TB, output, "has_pending_changes"), qt.Equals, false)

	verifyAtlasProjectConfigDatabaseState(c.TB, ctx, testDBURL, applyStarted, applyFinished)
}

// requireAtlasProjectConfigPostgresURL returns a live PostgreSQL address in URL
// form.
//
// dbtarget refuses an address carrying another engine's scheme, but it admits a
// schemeless driver DSN on purpose. This test rewrites the database name
// through net/url, so the URL shape is its own requirement and the check
// survives the move to dbtarget.
func requireAtlasProjectConfigPostgresURL(t *testing.T) string {
	t.Helper()
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	if !strings.HasPrefix(dbURL, "postgres://") && !strings.HasPrefix(dbURL, "postgresql://") {
		t.Skip("PostgreSQL URL required for atlas.hcl project config e2e test")
	}
	return dbURL
}

func writeAtlasProjectConfigFixture(tb testing.TB, repoRoot, workDir string) {
	c := qt.New(tb)
	fixtureRoot := filepath.Join(repoRoot, "integration", "testdata", "atlas-project-config")
	c.Assert(os.CopyFS(workDir, os.DirFS(fixtureRoot)), qt.IsNil)
}

func seedAtlasProjectConfigDatabaseState(tb testing.TB, ctx context.Context, dbURL string) {
	c := qt.New(tb)
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, err = db.ExecContext(ctx, "CREATE TABLE ptah_issue_276_seed (id INT PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, "CREATE SCHEMA ptah_issue_276")
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, `CREATE TABLE ptah_issue_276.atlas_schema_revisions (
    version VARCHAR PRIMARY KEY,
    description VARCHAR NOT NULL,
    type BIGINT NOT NULL DEFAULT 2,
    applied BIGINT NOT NULL DEFAULT 0,
    total BIGINT NOT NULL DEFAULT 0,
    executed_at TIMESTAMPTZ NOT NULL,
    execution_time BIGINT NOT NULL,
    error TEXT NULL,
    error_stmt TEXT NULL,
    hash VARCHAR NOT NULL,
    partial_hashes JSONB NULL,
    operator_version VARCHAR NOT NULL
)`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, `INSERT INTO ptah_issue_276.atlas_schema_revisions
(version, description, type, applied, total, executed_at, execution_time, hash, operator_version)
VALUES ($1, $2, 2, 1, 1, $3, 100, $4, $5)`,
		atlasProjectConfigSeedVersion,
		"Seed project config",
		time.Now(),
		atlasProjectConfigSeedHash,
		"Atlas",
	)
	c.Assert(err, qt.IsNil)
}

func runPtahInDir(ctx context.Context, dir, binaryPath string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func readStatusField(tb testing.TB, output, field string) any {
	c := qt.New(tb)
	var payload map[string]any
	c.Assert(json.Unmarshal([]byte(output), &payload), qt.IsNil, qt.Commentf("status output:\n%s", output))
	return payload[field]
}

func verifyAtlasProjectConfigDatabaseState(
	tb testing.TB,
	ctx context.Context,
	dbURL string,
	applyStarted time.Time,
	applyFinished time.Time,
) {
	c := qt.New(tb)
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	var tableName string
	err = db.QueryRowContext(ctx, `SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = 'ptah_issue_276_widgets'`).Scan(&tableName)
	c.Assert(err, qt.IsNil)
	c.Assert(tableName, qt.Equals, "ptah_issue_276_widgets")

	err = db.QueryRowContext(ctx, `SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = 'ptah_issue_276_seed'`).Scan(&tableName)
	c.Assert(err, qt.IsNil)
	c.Assert(tableName, qt.Equals, "ptah_issue_276_seed")

	var indexName string
	err = db.QueryRowContext(ctx, `SELECT indexname
FROM pg_indexes
WHERE schemaname = 'public' AND indexname = 'idx_ptah_issue_276_seed_id'`).Scan(&indexName)
	c.Assert(err, qt.IsNil)
	c.Assert(indexName, qt.Equals, "idx_ptah_issue_276_seed_id")

	seedRevision, seedTiming := readAtlasProjectConfigRevision(c.TB, ctx, db, atlasProjectConfigSeedVersion)
	c.Assert(seedRevision, qt.DeepEquals, atlasProjectConfigRevision{
		Version:         atlasProjectConfigSeedVersion,
		Description:     "Seed project config",
		Type:            2,
		Applied:         1,
		Total:           1,
		Hash:            atlasProjectConfigSeedHash,
		OperatorVersion: "Atlas",
	})
	c.Assert(seedTiming.ExecutedAt.IsZero(), qt.IsFalse)
	c.Assert(seedTiming.ExecutionTime, qt.Equals, int64(100))

	pendingRevision, pendingTiming := readAtlasProjectConfigRevision(c.TB, ctx, db, atlasProjectConfigPendingVersion)
	c.Assert(pendingRevision, qt.DeepEquals, atlasProjectConfigRevision{
		Version:         atlasProjectConfigPendingVersion,
		Description:     "create_project_config_widgets",
		Type:            2,
		Applied:         3,
		Total:           3,
		Error:           sql.NullString{Valid: true},
		ErrorStatement:  sql.NullString{Valid: true},
		Hash:            atlasProjectConfigPendingHash,
		PartialHashes:   sql.NullString{String: "null", Valid: true},
		OperatorVersion: "Ptah",
	})
	timingComment := qt.Commentf(
		"executed_at=%s apply_started=%s apply_finished=%s execution_time=%s",
		pendingTiming.ExecutedAt,
		applyStarted,
		applyFinished,
		time.Duration(pendingTiming.ExecutionTime),
	)
	c.Assert(pendingTiming.ExecutedAt.Before(applyStarted), qt.IsFalse, timingComment)
	c.Assert(pendingTiming.ExecutedAt.After(applyFinished), qt.IsFalse, timingComment)
	c.Assert(pendingTiming.ExecutionTime >= int64(50*time.Millisecond), qt.IsTrue, timingComment)

	var revisionCount int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ptah_issue_276.atlas_schema_revisions`).Scan(&revisionCount)
	c.Assert(err, qt.IsNil)
	c.Assert(revisionCount, qt.Equals, 2)
}

func readAtlasProjectConfigRevision(
	tb testing.TB,
	ctx context.Context,
	db *sql.DB,
	version string,
) (atlasProjectConfigRevision, atlasProjectConfigRevisionTiming) {
	c := qt.New(tb)
	c.Helper()

	var revision atlasProjectConfigRevision
	var timing atlasProjectConfigRevisionTiming
	err := db.QueryRowContext(ctx, `SELECT
    version,
    description,
    type,
    applied,
    total,
    executed_at,
    execution_time,
    error,
    error_stmt,
    hash,
    partial_hashes,
    operator_version
FROM ptah_issue_276.atlas_schema_revisions
WHERE version = $1`, version).Scan(
		&revision.Version,
		&revision.Description,
		&revision.Type,
		&revision.Applied,
		&revision.Total,
		&timing.ExecutedAt,
		&timing.ExecutionTime,
		&revision.Error,
		&revision.ErrorStatement,
		&revision.Hash,
		&revision.PartialHashes,
		&revision.OperatorVersion,
	)
	c.Assert(err, qt.IsNil)
	return revision, timing
}
