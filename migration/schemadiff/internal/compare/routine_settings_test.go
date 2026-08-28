package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// TestFunctionDefinitions_SettingsAreCompared pins that a property the
// comparison cannot see is one it plans forever.
//
// A pinned search_path was invisible: an author who added one saw it dropped on
// the next apply with nothing reporting the difference, and an author who
// removed one saw the routine keep it (stokaro/ptah#2356).
func TestFunctionDefinitions_SettingsAreCompared(t *testing.T) {
	rows := []struct {
		name       string
		declared   []string
		reported   []string
		wantChange bool
	}{
		{
			name:     "the same setting, written the way each side writes it",
			declared: []string{"search_path=pg_catalog, pg_temp"},
			reported: []string{"search_path=pg_catalog,pg_temp"},
		},
		{
			name:     "neither side sets anything",
			declared: nil,
			reported: nil,
		},
		{
			name:       "the author pinned one and the server has none",
			declared:   []string{"search_path=pg_catalog"},
			reported:   nil,
			wantChange: true,
		},
		{
			name:       "the server has one the author removed",
			declared:   nil,
			reported:   []string{"search_path=pg_catalog"},
			wantChange: true,
		},
		{
			name:       "a different value",
			declared:   []string{"search_path=app"},
			reported:   []string{"search_path=pg_catalog"},
			wantChange: true,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.FunctionDefinitions(
				schemamodel.Function{Name: "f", Settings: row.declared},
				catalog.Function{Name: "f", Settings: row.reported},
			)

			_, changed := diff.Changes["settings"]
			c.Assert(changed, qt.Equals, row.wantChange,
				qt.Commentf("changes: %#v", diff.Changes))
		})
	}
}
