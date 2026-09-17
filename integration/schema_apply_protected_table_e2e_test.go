//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// protectedRowsEntities declares one reference table and the file that carries
// its rows.
const protectedRowsEntities = `package entities

//ptah:schema:data table="regions" key="code" file="regions.yaml"
//ptah:schema:table name="regions"
type Region struct {
	//ptah:schema:field name="code" type="TEXT" primary="true"
	Code string

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`

// TestSchemaApplyProtectedTableE2E is the guarantee the versioned path already
// offers, measured on the declarative one: a fenced reference table does not
// change, and the proof is what the database holds afterwards rather than what
// the command printed (stokaro/ptah#3362).
//
// The engine is SQLite because the fence is decided before any statement is
// rendered and the answer does not vary by dialect; what needs a process here
// is that the shipped binary refuses and writes nothing.
func TestSchemaApplyProtectedTableE2E(t *testing.T) {
	c := qt.New(t)
	ctx := c.Context()

	work := c.TempDir()
	entities := filepath.Join(work, "entities")
	c.Assert(os.MkdirAll(entities, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(entities, "schema.go"), []byte(protectedRowsEntities), 0o600), qt.IsNil)
	rowsFile := filepath.Join(entities, "regions.yaml")
	c.Assert(os.WriteFile(rowsFile, []byte("- code: NO\n  name: Norway\n"), 0o600), qt.IsNil)

	dbPath := filepath.Join(work, "target.db")
	dbURL := "sqlite://" + filepath.ToSlash(dbPath)
	apply := func(args ...string) clirun.Result {
		c.Helper()
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
			append([]string{"schema", "apply", "--root-dir", entities, "--db-url", dbURL, "--auto-approve"}, args...)...)
	}

	seeded := apply()
	c.Assert(seeded.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", seeded.Stdout, seeded.Stderr))
	c.Assert(regionNames(c, ctx, dbPath), qt.DeepEquals, map[string]string{"NO": "Norway"})

	// The declaration now says something else about the same row.
	c.Assert(os.WriteFile(rowsFile, []byte("- code: NO\n  name: Norge\n"), 0o600), qt.IsNil)

	refused := apply("--protected-table", "regions")

	c.Assert(refused.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
	c.Assert(refused.Stderr, qt.Contains, "protected table(s) regions")
	c.Assert(refused.Stderr, qt.Contains, "no override")
	// The row is what the refusal is about.
	c.Assert(regionNames(c, ctx, dbPath), qt.DeepEquals, map[string]string{"NO": "Norway"})

	// The control: the same change, the same binary, no fence.
	applied := apply()

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(regionNames(c, ctx, dbPath), qt.DeepEquals, map[string]string{"NO": "Norge"})
}

// TestSchemaApplyProtectedTableLeavesOtherTablesAloneE2E keeps the fence from
// reading as "any entry refuses the run": the entry names a table this apply
// does not touch, and the apply goes through.
func TestSchemaApplyProtectedTableLeavesOtherTablesAloneE2E(t *testing.T) {
	c := qt.New(t)
	ctx := c.Context()

	work := c.TempDir()
	entities := filepath.Join(work, "entities")
	c.Assert(os.MkdirAll(entities, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(entities, "schema.go"), []byte(protectedRowsEntities), 0o600), qt.IsNil)
	rowsFile := filepath.Join(entities, "regions.yaml")
	c.Assert(os.WriteFile(rowsFile, []byte("- code: NO\n  name: Norway\n"), 0o600), qt.IsNil)

	dbPath := filepath.Join(work, "target.db")
	dbURL := "sqlite://" + filepath.ToSlash(dbPath)
	apply := func(args ...string) clirun.Result {
		c.Helper()
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
			append([]string{"schema", "apply", "--root-dir", entities, "--db-url", dbURL, "--auto-approve"}, args...)...)
	}

	seeded := apply("--protected-table", "countries")
	c.Assert(seeded.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", seeded.Stdout, seeded.Stderr))

	c.Assert(os.WriteFile(rowsFile, []byte("- code: NO\n  name: Norge\n"), 0o600), qt.IsNil)
	applied := apply("--protected-table", "countries")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(regionNames(c, ctx, dbPath), qt.DeepEquals, map[string]string{"NO": "Norge"})
}

// regionNames reads the reference table back through a driver of its own, so
// the answer does not pass through the reader the command used.
func regionNames(c *qt.C, ctx context.Context, dbPath string) map[string]string {
	c.Helper()

	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, "SELECT code, name FROM regions ORDER BY code")
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()

	names := map[string]string{}
	for rows.Next() {
		var code, name string
		c.Assert(rows.Scan(&code, &name), qt.IsNil)
		names[code] = name
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}
