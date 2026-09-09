//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestOCIReferrerFetchReadsTheReportPtahWroteE2E is the round trip
// `ptah oci fetch` exists to perform.
//
// A unit test over a hand-built artifact cannot see the defect this covers: the
// writer and the reader disagreed about the layer media type, so an artifact
// built with the reader's own default round-tripped while every report Ptah
// actually attaches was unreadable (stokaro/ptah#3120). Only a run that pushes,
// applies and then fetches drives both halves.
func TestOCIReferrerFetchReadsTheReportPtahWroteE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	adminURL := requiredPostgresE2EURL(t)
	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c, ctx, repoRoot, binaryPath)

	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	suffix := time.Now().UnixNano()
	databaseName := fmt.Sprintf("ptah_oci_fetch_%d", suffix)
	createE2EDatabase(c, ctx, adminDB, databaseName)
	defer dropE2EDatabase(c, context.Background(), adminDB, databaseName)
	databaseURL := replaceDatabaseName(c, adminURL, databaseName)

	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	writeOCIMigration(c, migrationsDir, ociMigrationVersion, "widgets")
	reference := fmt.Sprintf("oci://%s/ptah/oci-fetch-%d:latest", registry, suffix)

	pushOutput, err := runPtahInDir(ctx, repoRoot, binaryPath,
		"migrations", "push", reference,
		"--migrations-dir", migrationsDir,
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("push output:\n%s", pushOutput))

	// The apply attaches the deployment report this test then reads back.
	upOutput, err := runPtahInDir(ctx, repoRoot, binaryPath,
		"migrations", "up",
		"--migrations-dir", reference,
		"--db-url", databaseURL,
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("up output:\n%s", upOutput))

	fetchOutput, err := runPtahInDir(ctx, repoRoot, binaryPath,
		"oci", "fetch", reference,
		"--type", "deployment",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("fetch output:\n%s", fetchOutput))

	var report struct {
		SchemaVersion int    `json:"schema_version"`
		Dialect       string `json:"dialect"`
		Outcome       string `json:"outcome"`
	}
	c.Assert(json.Unmarshal([]byte(fetchOutput), &report), qt.IsNil,
		qt.Commentf("fetch output was not the report JSON:\n%s", fetchOutput))
	c.Assert(report.SchemaVersion, qt.Equals, 1)
	c.Assert(report.Dialect, qt.Equals, "postgres")
	c.Assert(report.Outcome, qt.Equals, "succeeded")
}

// TestOCIReferrerFetchReadsTheLintReportE2E covers the second referrer kind
// through its own writer, because the three kinds carry three media types and
// one passing kind says nothing about the others.
func TestOCIReferrerFetchReadsTheLintReportE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c, ctx, repoRoot, binaryPath)

	suffix := time.Now().UnixNano()
	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	writeOCIMigration(c, migrationsDir, ociMigrationVersion, "widgets")
	reference := fmt.Sprintf("oci://%s/ptah/oci-fetch-lint-%d:latest", registry, suffix)

	pushOutput, err := runPtahInDir(ctx, repoRoot, binaryPath,
		"migrations", "push", reference,
		"--migrations-dir", migrationsDir,
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("push output:\n%s", pushOutput))

	lintOutput, err := runPtahInDir(ctx, repoRoot, binaryPath,
		"migrations", "lint",
		"--dir", reference,
		"--dialect", "postgres",
		"--attach",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("lint output:\n%s", lintOutput))

	fetchOutput, err := runPtahInDir(ctx, repoRoot, binaryPath,
		"oci", "fetch", reference,
		"--type", "lint",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("fetch output:\n%s", fetchOutput))

	var report map[string]any
	c.Assert(json.Unmarshal([]byte(fetchOutput), &report), qt.IsNil,
		qt.Commentf("fetch output was not the report JSON:\n%s", fetchOutput))
	c.Assert(len(report) > 0, qt.IsTrue)
}
