package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ptaherr"
)

// TestSchemaDiffRefusesTheRetiredRefreshStrategy keeps this file's subject --
// the compat surface never reports a synced schema over a declaration it did
// not model -- on the refusal that is now correct.
//
// The old refusal said `postgres cannot represent … only "manual" is currently
// supported`, which named the target as the limitation. No target refreshes as
// part of reconciliation, so the refusal is about what a schema can state, and
// it is the same on every target (stokaro/ptah#1625).
func TestSchemaDiffRefusesTheRetiredRefreshStrategy(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	from := writeCompatMaterializedViewSchema(c, dir, "from.hcl", "manual")
	to := writeCompatMaterializedViewSchema(c, dir, "to.hcl", "concurrently")

	out, err := runCompatCommand(
		t,
		"schema", "diff",
		"--dev-url", "postgres://localhost/dev",
		"--from", "file://"+from,
		"--to", "file://"+to,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrRetiredAttribute)
	c.Assert(err, qt.ErrorMatches, `.*materialized view "user_counts" declares refresh_strategy.*`)
	c.Assert(err, qt.Not(qt.ErrorMatches), `.*postgres cannot represent.*`)
	c.Assert(out, qt.Not(qt.Contains), "CREATE MATERIALIZED VIEW")
}

// TestSchemaDiffRefusesTheRetiredRefreshStrategyDespiteExclusion inverts the
// ordering this file used to pin, deliberately.
//
// `--exclude legacy_stats` used to rescue an unsupported declaration, and that
// was right while the refusal was a per-target capability judgment: an object
// nobody compares raises no capability question. It is now a statement about
// the document -- the attribute is not schema state on any target -- so a
// selection made after loading cannot make an unmodeled declaration modeled.
// The refusal fires while the file is parsed, before any selection exists.
func TestSchemaDiffRefusesTheRetiredRefreshStrategyDespiteExclusion(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	from := writeCompatScopedMaterializedViewSchema(c, dir, "from.hcl")
	to := writeCompatScopedMaterializedViewSchema(c, dir, "to.hcl")

	out, err := runCompatCommand(
		t,
		"schema", "diff",
		"--dev-url", "postgres://localhost/dev",
		"--from", "file://"+from,
		"--to", "file://"+to,
		"--exclude", "legacy_stats",
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrRetiredAttribute)
	c.Assert(out, qt.Not(qt.Contains), "Schemas are synced")
}

// TestSchemaDiffAcceptsAMaterializedViewWithoutTheAttribute is the control that
// keeps the refusal scoped to the attribute rather than to the object.
func TestSchemaDiffAcceptsAMaterializedViewWithoutTheAttribute(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	from := writeCompatPlainMaterializedViewSchema(c, dir, "from.hcl")
	to := writeCompatPlainMaterializedViewSchema(c, dir, "to.hcl")

	out, err := runCompatCommand(
		t,
		"schema", "diff",
		"--dev-url", "postgres://localhost/dev",
		"--from", "file://"+from,
		"--to", "file://"+to,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Equals, "Schemas are synced, no changes to be made.\n")
}

func writeCompatMaterializedViewSchema(c *qt.C, dir, name, strategy string) string {
	c.Helper()
	path := filepath.Join(dir, name)
	contents := []byte(`
materialized "user_counts" {
  as               = "SELECT count(*) AS total FROM users"
  refresh_strategy = "` + strategy + `"
}
`)
	c.Assert(os.WriteFile(path, contents, 0o600), qt.IsNil)
	return path
}

func writeCompatPlainMaterializedViewSchema(c *qt.C, dir, name string) string {
	c.Helper()
	path := filepath.Join(dir, name)
	contents := []byte(`
materialized "user_counts" {
  as = "SELECT count(*) AS total FROM users"
}
`)
	c.Assert(os.WriteFile(path, contents, 0o600), qt.IsNil)
	return path
}

func writeCompatScopedMaterializedViewSchema(c *qt.C, dir, name string) string {
	c.Helper()
	path := filepath.Join(dir, name)
	contents := []byte(`
materialized "current_stats" {
  as = "SELECT count(*) AS total FROM users"
}

materialized "legacy_stats" {
  as               = "SELECT count(*) AS total FROM legacy_users"
  refresh_strategy = "concurrently"
}
`)
	c.Assert(os.WriteFile(path, contents, 0o600), qt.IsNil)
	return path
}
