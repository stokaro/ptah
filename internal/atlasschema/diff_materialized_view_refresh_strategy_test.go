package atlasschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/atlasschema"
)

// TestDiffRefusesTheRetiredRefreshStrategyBeforeComparing keeps this file's
// subject on the refusal that is now correct: the message no longer names a
// target, because no target refreshes as part of reconciliation
// (stokaro/ptah#1625).
func TestDiffRefusesTheRetiredRefreshStrategyBeforeComparing(t *testing.T) {
	tests := []struct {
		name         string
		fromStrategy string
		toStrategy   string
	}{
		{name: "from declaration", fromStrategy: "concurrently", toStrategy: "manual"},
		{name: "to declaration", fromStrategy: "manual", toStrategy: "every 5 minutes"},
		{name: "the value that used to be accepted", fromStrategy: "manual", toStrategy: "manual"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			from := writeMaterializedViewSchema(c, dir, "from.hcl", test.fromStrategy)
			to := writeMaterializedViewSchema(c, dir, "to.hcl", test.toStrategy)

			report, err := atlasschema.Diff(t.Context(), atlasschema.DiffOptions{
				FromURLs: []string{"file://" + from},
				ToURLs:   []string{"file://" + to},
				DevURL:   "postgres://localhost/dev",
			})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrRetiredAttribute)
			c.Assert(err, qt.ErrorMatches, `.*materialized view "user_counts" declares refresh_strategy.*`)
			c.Assert(err, qt.Not(qt.ErrorMatches), `.*postgres cannot represent.*`)
			c.Assert(report.Changes, qt.HasLen, 0)
		})
	}
}

// TestDiffRefusesTheRetiredRefreshStrategyDespiteExclusion inverts what this
// test pinned, deliberately.
//
// `--exclude` used to rescue an unsupported declaration, and that followed from
// the refusal being a per-target capability judgment: an object nobody compares
// raises no capability question. The refusal is now about the DOCUMENT -- the
// attribute is not schema state on any target -- and it fires while the file is
// parsed, before a selection exists to rescue anything.
func TestDiffRefusesTheRetiredRefreshStrategyDespiteExclusion(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	from := writeScopedMaterializedViewSchema(c, dir, "from.hcl")
	to := writeScopedMaterializedViewSchema(c, dir, "to.hcl")

	report, err := atlasschema.Diff(t.Context(), atlasschema.DiffOptions{
		FromURLs: []string{"file://" + from},
		ToURLs:   []string{"file://" + to},
		DevURL:   "postgres://localhost/dev",
		Exclude:  []string{"legacy_stats"},
	})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrRetiredAttribute)
	c.Assert(report.Changes, qt.HasLen, 0)
}

func writeMaterializedViewSchema(c *qt.C, dir, name, strategy string) string {
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

func writeScopedMaterializedViewSchema(c *qt.C, dir, name string) string {
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
