//go:build integration

package integration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// TestAtlasCompatLeadingSchemaTypeSelectorE2E pins the deliberate
// stokaro/ptah#933 divergence behind the multi-segment selector.
//
// Measured on 2026-08-12 against the pinned community binary v1.3.0, using the
// same two SQLite schema files: `*[type=schema].*[type=table]` exits 0 there
// but leaves the CREATE TABLE plan unchanged. Ptah removes every table and
// reports the schemas as synced. The Ptah answer follows the selector's literal
// meaning (every table inside every schema) and avoids pretending that an
// accepted exclusion took no effect.
//
// The unfiltered and final-type-only controls below prove that an empty plan is
// the selector's effect rather than a broken fixture or blanket table filter.
func TestAtlasCompatLeadingSchemaTypeSelectorE2E(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := filepath.Join(dir, "from.sql")
	toPath := filepath.Join(dir, "to.sql")
	c.Assert(os.WriteFile(fromPath, []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(toPath, []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"+
		"CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT);\n"), 0o600), qt.IsNil)

	runDiff := func(extraArgs ...string) string {
		c.Helper()
		cmd := atlas.NewCompatCommand("atlas")
		var out, stderr bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&stderr)
		args := []string{
			"schema", "diff",
			"--from", "file://" + fromPath,
			"--to", "file://" + toPath,
			"--dev-url", "sqlite://" + filepath.Join(t.TempDir(), "dev.db"),
		}
		args = append(args, extraArgs...)
		cmd.SetArgs(args)

		err := cmd.Execute()

		c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))
		c.Assert(stderr.String(), qt.Equals, "")
		return out.String()
	}

	unfiltered := runDiff()
	c.Assert(unfiltered, qt.Contains, "CREATE TABLE")
	c.Assert(unfiltered, qt.Contains, "posts")

	finalType := runDiff("--exclude", "*[type=table]")
	c.Assert(finalType, qt.Equals, "Schemas are synced, no changes to be made.\n")

	leadingSchema := runDiff("--exclude", "*[type=schema].*[type=table]")
	c.Assert(leadingSchema, qt.Equals, "Schemas are synced, no changes to be made.\n")
}
