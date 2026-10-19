package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// checked is a descriptor carrying every field cleancheck.go declares, each
// at a value the checks accept.
func checked() descriptor.Dialect {
	described := head("alpha")
	described.DefaultSchema = "main"
	described.Schemas = descriptor.Schemas{Selection: descriptor.SchemaSelectionSingle}
	described.CleanCheck = descriptor.CleanCheck{
		Adoption:        true,
		AdoptionCounts:  true,
		Dev:             true,
		Tables:          "SELECT name FROM catalog",
		CatalogOrder:    true,
		ConnectedSchema: true,
		StartingPoint:   true,
	}
	described.SchemaAttributesQuery = "SELECT charset, collation FROM schemata WHERE name = ?"
	described.QualifierRefusal = "nobody measured it"
	return described
}

func TestNewRegistry_CleanCheck_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		described descriptor.Dialect
	}{
		{name: "every field at a value the checks accept", described: checked()},
		{name: "a search-path engine reads its own catalog", described: withCleanCheck(func(d *descriptor.Dialect) {
			d.Schemas.Selection = descriptor.SchemaSelectionSearchPath
			d.CleanCheck.Tables = ""
		})},
		{name: "a reset listing alone", described: withCleanCheck(func(d *descriptor.Dialect) {
			d.CleanCheck = descriptor.CleanCheck{Dev: true, ResetListing: true}
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.described)

			c.Assert(err, qt.IsNil)
			described, found := registry.Lookup("alpha")
			c.Assert(found, qt.IsTrue)
			c.Assert(described, qt.DeepEquals, test.described)
		})
	}
}

func TestNewRegistry_CleanCheck_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		edit    descriptor.Dialect
		wantErr string
	}{
		{
			name:    "a tables query where the URL selects a schema by search_path",
			edit:    withCleanCheck(func(d *descriptor.Dialect) { d.Schemas.Selection = descriptor.SchemaSelectionSearchPath }),
			wantErr: `invalid dialect descriptor: dialect "alpha": a clean check states no tables query where the URL selects a schema by "search-path"`,
		},
		{
			name:    "a tables query where the URL selects a database",
			edit:    withCleanCheck(func(d *descriptor.Dialect) { d.Schemas.Selection = descriptor.SchemaSelectionDatabase }),
			wantErr: `invalid dialect descriptor: dialect "alpha": a clean check states no tables query where the URL selects a schema by "database"`,
		},
		{
			name:    "a tables query beside the reset listing",
			edit:    withCleanCheck(func(d *descriptor.Dialect) { d.CleanCheck.ResetListing = true }),
			wantErr: `invalid dialect descriptor: dialect "alpha": a clean check that reads the reset listing states no tables query`,
		},
		{
			name:    "a counted adoption refusal with no refused adoption",
			edit:    withCleanCheck(func(d *descriptor.Dialect) { d.CleanCheck.Adoption = false }),
			wantErr: `invalid dialect descriptor: dialect "alpha": a counted adoption refusal needs a refused adoption`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.edit)

			c.Assert(err, qt.ErrorIs, descriptor.ErrInvalidDescriptor)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(registry, qt.IsNil)
		})
	}
}

// withCleanCheck returns checked with one change applied, so each row carries
// the one value it is about.
func withCleanCheck(change func(*descriptor.Dialect)) descriptor.Dialect {
	described := checked()
	change(&described)
	return described
}
