//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
)

// applyDeclaredRowsEntities declares one reference table and the file that
// carries its rows. It is an ordinary working copy: the rows are beside the
// schema, not inside it.
const applyDeclaredRowsEntities = `package entities

//ptah:schema:data table="regions" key="code" file="regions.yaml"
//ptah:schema:table name="regions"
type Region struct {
	//ptah:schema:field name="code" type="TEXT" primary="true"
	Code string

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`

const applyDeclaredRowsYAML = `- code: CZ
  name: Czechia
- code: SK
  name: Slovakia
`

// TestPostgresApplyDeclaredRowsE2E drives `ptah schema apply` against a live
// PostgreSQL from a working copy that declares rows in a file.
//
// Only a server answers the question. Publication read that file and drift read
// it through its own loader, while planning read neither and refused the
// declaration instead, so a declared row set could not reach a database from the
// working copy that declared it (stokaro/ptah#3269). A unit test can say which
// INSERT statements were rendered; whether they arrived is the database's
// answer.
func TestPostgresApplyDeclaredRowsE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testDBName := fmt.Sprintf("ptah_apply_rows_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)
	scopedURL := replaceDatabaseName(c, dbURL, testDBName)

	repoRoot := e2eRepoRoot(t)
	workDir := c.TempDir()
	binary := filepath.Join(workDir, "ptah")
	buildPtah(c, ctx, repoRoot, binary)

	root := filepath.Join(workDir, "entities")
	c.Assert(os.MkdirAll(root, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(applyDeclaredRowsEntities), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "regions.yaml"), []byte(applyDeclaredRowsYAML), 0o600), qt.IsNil)

	output, err := runPtahInDir(
		ctx, repoRoot, binary,
		"schema", "apply",
		"--root-dir", root,
		"--db-url", scopedURL,
		"--auto-approve",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("schema apply output:\n%s", output))

	targetDB, err := sql.Open("pgx", scopedURL)
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

// TestPostgresApplyRefusesARowFileOutsideTheProjectE2E is the boundary the read
// above must not widen. A declaration names the file carrying its rows, and a
// desired state is not always one the reader wrote, so the path stays inside the
// project the command was pointed at. The refusal arrives before the database is
// touched, which is what the assertion on the empty database measures.
func TestPostgresApplyRefusesARowFileOutsideTheProjectE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testDBName := fmt.Sprintf("ptah_apply_escape_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)
	scopedURL := replaceDatabaseName(c, dbURL, testDBName)

	repoRoot := e2eRepoRoot(t)
	workDir := c.TempDir()
	binary := filepath.Join(workDir, "ptah")
	buildPtah(c, ctx, repoRoot, binary)

	// The file the declaration reaches for exists, so the refusal is about where
	// it is rather than about it being missing.
	c.Assert(os.WriteFile(filepath.Join(workDir, "outside.yaml"),
		[]byte("- code: XX\n  name: outside\n"), 0o600), qt.IsNil)
	root := filepath.Join(workDir, "entities")
	c.Assert(os.MkdirAll(root, 0o750), qt.IsNil)
	escaping := strings.Replace(
		applyDeclaredRowsEntities, `file="regions.yaml"`, `file="../outside.yaml"`, 1,
	)
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(escaping), 0o600), qt.IsNil)

	output, err := runPtahInDir(
		ctx, repoRoot, binary,
		"schema", "apply",
		"--root-dir", root,
		"--db-url", scopedURL,
		"--auto-approve",
	)

	c.Assert(err, qt.IsNotNil, qt.Commentf("schema apply output:\n%s", output))
	c.Assert(output, qt.Contains, "is outside")
	var tables int
	c.Assert(targetTableCount(c, ctx, scopedURL, &tables), qt.IsNil)
	c.Assert(tables, qt.Equals, 0, qt.Commentf("a refusal must not reach the database"))
}

func targetTableCount(c *qt.C, ctx context.Context, url string, out *int) error {
	c.Helper()
	db, err := sql.Open("pgx", url)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.QueryRowContext(
		ctx, "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'",
	).Scan(out)
}
