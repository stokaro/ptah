package atlasschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/atlasschema"
)

func TestDiffRefusesMaterializedViewRefreshStrategyBeforeComparing(t *testing.T) {
	tests := []struct {
		name         string
		fromStrategy string
		toStrategy   string
	}{
		{name: "from declaration", fromStrategy: "concurrently", toStrategy: "manual"},
		{name: "to declaration", fromStrategy: "manual", toStrategy: "every 5 minutes"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			from := writeMaterializedViewSchema(c.TB, dir, "from.hcl", test.fromStrategy)
			to := writeMaterializedViewSchema(c.TB, dir, "to.hcl", test.toStrategy)

			report, err := atlasschema.Diff(t.Context(), atlasschema.DiffOptions{
				FromURLs: []string{"file://" + from},
				ToURLs:   []string{"file://" + to},
				DevURL:   "postgres://localhost/dev",
			})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `postgres cannot represent materialized view "user_counts" refresh strategy ".+"; only "manual" is currently supported`)
			c.Assert(report.Changes, qt.HasLen, 0)
		})
	}
}

func TestDiffValidatesMaterializedViewRefreshStrategyAfterExclusion(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	from := writeScopedMaterializedViewSchema(c.TB, dir, "from.hcl")
	to := writeScopedMaterializedViewSchema(c.TB, dir, "to.hcl")

	report, err := atlasschema.Diff(t.Context(), atlasschema.DiffOptions{
		FromURLs: []string{"file://" + from},
		ToURLs:   []string{"file://" + to},
		DevURL:   "postgres://localhost/dev",
		Exclude:  []string{"legacy_stats"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Changes, qt.HasLen, 0)
}

func writeMaterializedViewSchema(tb testing.TB, dir, name, strategy string) string {
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

func writeScopedMaterializedViewSchema(tb testing.TB, dir, name string) string {
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
