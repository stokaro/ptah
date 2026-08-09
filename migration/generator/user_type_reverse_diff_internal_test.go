package generator

// White-box testing required: reverseSchemaDiffWithSchema is unexported and the
// property under test is a field of the diff it returns, not a statement in the
// SQL. Going through the exported generator would need a live database on both
// ends and would still only show the ordering indirectly.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestReverseSchemaDiff_ModifiedUserTypesCarryTheDownDirectionsCurrentShape
// pins where the reversed diff's from-side comes from.
//
// CurrentBaseType and CurrentFieldTypes order the non-CASCADE DROP the recreate
// path emits, and that DROP runs against whatever the database holds when the
// statement executes. For a down migration that is the shape the up migration
// created, which is the up direction's TARGET -- so the values are re-derived
// from the target schema rather than carried across from the forward diff,
// where they describe the shape the up migration replaced.
func TestReverseSchemaDiff_ModifiedUserTypesCarryTheDownDirectionsCurrentShape(t *testing.T) {
	c := qt.New(t)

	target := &goschema.Database{
		Domains: []goschema.Domain{{Name: "dd", BaseType: "integer"}},
		CompositeTypes: []goschema.CompositeType{
			{Name: "cc", Fields: []goschema.CompositeTypeField{{Name: "f", Type: "dd"}}},
		},
	}
	forward := &types.SchemaDiff{
		DomainsModified: []types.DomainDiff{
			{DomainName: "dd", Changes: map[string]string{"type": "cc -> integer"}, CurrentBaseType: "cc"},
		},
		CompositeTypesModified: []types.CompositeTypeDiff{
			{TypeName: "cc", Changes: map[string]string{"fields": "f integer -> f dd"}, CurrentFieldTypes: []string{"integer"}},
		},
	}

	reversed := reverseSchemaDiffWithSchema(forward, target, nil)

	c.Assert(reversed.DomainsModified, qt.HasLen, 1)
	c.Assert(reversed.DomainsModified[0].CurrentBaseType, qt.Equals, "integer")
	c.Assert(reversed.DomainsModified[0].Changes["type"], qt.Equals, "integer -> cc")
	c.Assert(reversed.CompositeTypesModified, qt.HasLen, 1)
	c.Assert(reversed.CompositeTypesModified[0].CurrentFieldTypes, qt.DeepEquals, []string{"dd"})
}

// TestReverseSchemaDiff_ModifiedUserTypesWithoutATargetSchema covers the
// deprecated nil-schema caller: there is no shape to derive a from-side from,
// so the reversed diff carries none and the drops fall back to declaration
// order rather than to a graph nobody measured.
func TestReverseSchemaDiff_ModifiedUserTypesWithoutATargetSchema(t *testing.T) {
	c := qt.New(t)

	forward := &types.SchemaDiff{
		DomainsModified: []types.DomainDiff{
			{DomainName: "dd", Changes: map[string]string{"type": "cc -> integer"}, CurrentBaseType: "cc"},
		},
		CompositeTypesModified: []types.CompositeTypeDiff{
			{TypeName: "cc", Changes: map[string]string{"fields": "f integer -> f dd"}, CurrentFieldTypes: []string{"integer"}},
		},
	}

	reversed := reverseSchemaDiff(forward)

	c.Assert(reversed.DomainsModified, qt.HasLen, 1)
	c.Assert(reversed.DomainsModified[0].CurrentBaseType, qt.Equals, "")
	c.Assert(reversed.CompositeTypesModified, qt.HasLen, 1)
	c.Assert(reversed.CompositeTypesModified[0].CurrentFieldTypes, qt.IsNil)
}
