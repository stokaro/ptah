package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// TestAnalyze_AtlasPositionsAskForTheStartingSchema asks for the starting
// schema of v1_x.sql, whose name spells no number, under the key the migrator
// runs it under: its position 2 after 1_a.sql. The replay keys the schema it
// reads by that key, so a file keyed 0 would ask for nothing and get nothing.
func TestAnalyze_AtlasPositionsAskForTheStartingSchema(t *testing.T) {
	c := qt.New(t)

	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"1_a.sql":  "CREATE TABLE users (id int);\n",
		"v1_x.sql": "DROP TYPE pair;\n",
	}), lint.Options{
		Dialect: "postgres", ChangeCount: lint.ChangeCountAtlasCommunity, DirFormat: migrationfile.DirFormatAtlas,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(analysis.BaselineVersions(), qt.DeepEquals, []int64{2})
	c.Assert(analysis.Files()[1].Name, qt.Equals, "v1_x.sql")
	c.Assert(analysis.Files()[1].Version, qt.Equals, int64(2))
	c.Assert(analysis.Files()[1].RevisionVersion, qt.Equals, "v1")
}
