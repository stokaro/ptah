//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/schemaartifact"
)

// declaredRowsEntities is the publisher's working copy: one reference table and
// the annotation that says which file carries its rows.
const declaredRowsEntities = `package entities

//ptah:schema:data table="regions" key="code" file="regions.yaml"
//ptah:schema:table name="regions"
type Region struct {
	//ptah:schema:field name="code" type="TEXT" primary="true"
	Code string

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`

const declaredRowsYAML = `- code: CZ
  name: Czechia
- code: SK
  name: Slovakia
`

// TestOCISchemaDeclaredRowsReachTheDatabaseE2E drives the path a consumer that
// cannot reach the registry has to take: the artifact is materialized to disk
// first, and everything after that reads files.
//
// The publisher's working copy is deleted before the consumer runs, because
// that is the whole question. `regions.yaml` is a path into the directory that
// published the artifact, so a materialization carrying the canonical HCL alone
// leaves the consumer with a declaration and no rows -- and a plan computed
// from it creates the table with nothing in it, or refuses for a file it was
// never going to find (stokaro/ptah#3256).
//
// Only a server can answer this. The rows are compared against what the
// database holds, and a plan that renders the right INSERT statements into a
// buffer says nothing about whether they arrived.
func TestOCISchemaDeclaredRowsReachTheDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	adminURL := requiredPostgresE2EURL(t)
	repoRoot := e2eRepoRoot(t)
	workDir := c.TempDir()
	binaryPath := filepath.Join(workDir, "ptah")
	buildPtah(c, ctx, repoRoot, binaryPath)

	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	suffix := time.Now().UnixNano()
	databaseName := fmt.Sprintf("ptah_oci_rows_%d", suffix)
	createE2EDatabase(c, ctx, adminDB, databaseName)
	defer dropE2EDatabase(c, context.Background(), adminDB, databaseName)
	databaseURL := replaceDatabaseName(c, adminURL, databaseName)

	publisher := filepath.Join(workDir, "publisher")
	c.Assert(os.MkdirAll(publisher, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(publisher, "schema.go"), []byte(declaredRowsEntities), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(publisher, "regions.yaml"), []byte(declaredRowsYAML), 0o600), qt.IsNil)

	reference := fmt.Sprintf("oci://%s/ptah/oci-rows-%d:latest", registry, suffix)
	pushOutput, err := runPtahInDir(
		ctx, repoRoot, binaryPath,
		"schema", "push", reference,
		"--root-dir", publisher,
		"--version", "v20260915000001",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("schema push output:\n%s", pushOutput))

	consumer := filepath.Join(workDir, "consumer")
	pulledSchema := filepath.Join(consumer, "schema.hcl")
	pullOutput, err := runPtahInDir(
		ctx, repoRoot, binaryPath,
		"schema", "pull", reference,
		"--out", pulledSchema,
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("schema pull output:\n%s", pullOutput))
	c.Assert(pullOutput, qt.Contains, schemaartifact.ManagedDataFileName)

	// The working copy that declared the rows is gone. What the consumer has is
	// what the registry handed it.
	c.Assert(os.RemoveAll(publisher), qt.IsNil)

	applyOutput, err := runPtahInDir(
		ctx, repoRoot, binaryPath,
		"schema", "apply",
		"--schema-file", pulledSchema,
		"--db-url", databaseURL,
		"--auto-approve",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("schema apply output:\n%s", applyOutput))

	targetDB, err := sql.Open("pgx", databaseURL)
	c.Assert(err, qt.IsNil)
	defer targetDB.Close()
	rows, err := targetDB.QueryContext(ctx, "SELECT code, name FROM regions ORDER BY code")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	declared := map[string]string{}
	for rows.Next() {
		var code, name string
		c.Assert(rows.Scan(&code, &name), qt.IsNil)
		declared[code] = name
	}
	c.Assert(rows.Err(), qt.IsNil)
	c.Assert(declared, qt.DeepEquals, map[string]string{"CZ": "Czechia", "SK": "Slovakia"})
}

// TestOCISchemaMaterializedWithoutItsRowsRefusesE2E is the control on the test
// above. With the row layer removed, the same consumer has a declaration and no
// rows, and the plan refuses rather than creating the table empty and reporting
// success.
func TestOCISchemaMaterializedWithoutItsRowsRefusesE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	adminURL := requiredPostgresE2EURL(t)
	repoRoot := e2eRepoRoot(t)
	workDir := c.TempDir()
	binaryPath := filepath.Join(workDir, "ptah")
	buildPtah(c, ctx, repoRoot, binaryPath)

	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	suffix := time.Now().UnixNano()
	databaseName := fmt.Sprintf("ptah_oci_norows_%d", suffix)
	createE2EDatabase(c, ctx, adminDB, databaseName)
	defer dropE2EDatabase(c, context.Background(), adminDB, databaseName)
	databaseURL := replaceDatabaseName(c, adminURL, databaseName)

	publisher := filepath.Join(workDir, "publisher")
	c.Assert(os.MkdirAll(publisher, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(publisher, "schema.go"), []byte(declaredRowsEntities), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(publisher, "regions.yaml"), []byte(declaredRowsYAML), 0o600), qt.IsNil)

	reference := fmt.Sprintf("oci://%s/ptah/oci-norows-%d:latest", registry, suffix)
	pushOutput, err := runPtahInDir(
		ctx, repoRoot, binaryPath,
		"schema", "push", reference,
		"--root-dir", publisher,
		"--version", "v20260915000002",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("schema push output:\n%s", pushOutput))

	consumer := filepath.Join(workDir, "consumer")
	pulledSchema := filepath.Join(consumer, "schema.hcl")
	pullOutput, err := runPtahInDir(
		ctx, repoRoot, binaryPath,
		"schema", "pull", reference,
		"--out", pulledSchema,
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("schema pull output:\n%s", pullOutput))
	c.Assert(os.RemoveAll(publisher), qt.IsNil)
	c.Assert(os.Remove(filepath.Join(consumer, schemaartifact.ManagedDataFileName)), qt.IsNil)

	applyOutput, err := runPtahInDir(
		ctx, repoRoot, binaryPath,
		"schema", "apply",
		"--schema-file", pulledSchema,
		"--db-url", databaseURL,
		"--auto-approve",
	)

	c.Assert(err, qt.IsNotNil, qt.Commentf("schema apply output:\n%s", applyOutput))
	c.Assert(applyOutput, qt.Contains, "regions")

	targetDB, err := sql.Open("pgx", databaseURL)
	c.Assert(err, qt.IsNil)
	defer targetDB.Close()
	var tables int
	c.Assert(targetDB.QueryRowContext(
		ctx,
		"SELECT count(*) FROM information_schema.tables WHERE table_name = 'regions'",
	).Scan(&tables), qt.IsNil)
	c.Assert(tables, qt.Equals, 0, qt.Commentf("a refusal must not leave the table behind"))
}
