package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ptaherr"
)

func TestSchemaDiffRefusesMaterializedViewRefreshStrategyBeforeSyncedOutput(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	from := writeCompatMaterializedViewSchema(c.TB, dir, "from.hcl", "manual")
	to := writeCompatMaterializedViewSchema(c.TB, dir, "to.hcl", "concurrently")

	out, err := runCompatCommand(
		t,
		"schema", "diff",
		"--dev-url", "postgres://localhost/dev",
		"--from", "file://"+from,
		"--to", "file://"+to,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `postgres cannot represent materialized view "user_counts" refresh strategy "concurrently"; only "manual" is currently supported`)
	c.Assert(out, qt.Not(qt.Contains), "CREATE MATERIALIZED VIEW")
}

func TestSchemaDiffScopesMaterializedViewRefreshStrategyBeforeValidation(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	from := writeCompatScopedMaterializedViewSchema(c.TB, dir, "from.hcl")
	to := writeCompatScopedMaterializedViewSchema(c.TB, dir, "to.hcl")

	out, err := runCompatCommand(
		t,
		"schema", "diff",
		"--dev-url", "postgres://localhost/dev",
		"--from", "file://"+from,
		"--to", "file://"+to,
		"--exclude", "legacy_stats",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Equals, "Schemas are synced, no changes to be made.\n")
}

func writeCompatMaterializedViewSchema(tb testing.TB, dir, name, strategy string) string {
	c := qt.New(tb)
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

func writeCompatScopedMaterializedViewSchema(tb testing.TB, dir, name string) string {
	c := qt.New(tb)
	c.Helper()
	path := filepath.Join(dir, name)
	contents := []byte(`
materialized "current_stats" {
  as               = "SELECT count(*) AS total FROM users"
  refresh_strategy = "manual"
}

materialized "legacy_stats" {
  as               = "SELECT count(*) AS total FROM legacy_users"
  refresh_strategy = "concurrently"
}
`)
	c.Assert(os.WriteFile(path, contents, 0o600), qt.IsNil)
	return path
}
