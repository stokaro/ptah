package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestCompositeTypes_AttributeDeltaOnlyWhenALTERReachesTheDeclaredShape pins the
// rule that decides between ALTER TYPE and a rebuild.
//
// PostgreSQL appends: a new attribute lands last whatever position the
// declaration gives it. So a declaration that inserts a field in the middle
// cannot be reached by ALTER TYPE, and planning one would leave the catalog in
// an order the next comparison reports as different again -- forever. The
// comparator simulates the drops and appends and only reports a delta when the
// result equals the declaration (stokaro/ptah#1717).
func TestCompositeTypes_AttributeDeltaOnlyWhenALTERReachesTheDeclaredShape(t *testing.T) {
	tests := []struct {
		name        string
		declared    []goschema.CompositeTypeField
		current     []types.DBCompositeField
		wantAdded   []string
		wantRemoved []string
	}{
		{
			name:      "a field appended at the end",
			declared:  fields("street", "city", "zip"),
			current:   dbFields("street", "city"),
			wantAdded: []string{"zip"},
		},
		{
			name:        "a field removed",
			declared:    fields("street"),
			current:     dbFields("street", "city"),
			wantRemoved: []string{"city"},
		},
		{
			name:        "one removed and one appended",
			declared:    fields("street", "zip"),
			current:     dbFields("street", "city"),
			wantAdded:   []string{"zip"},
			wantRemoved: []string{"city"},
		},
		{
			// ALTER TYPE would land zip last and the declaration wants it in
			// the middle, so this one rebuilds instead.
			name:     "a field inserted in the middle",
			declared: fields("street", "zip", "city"),
			current:  dbFields("street", "city"),
		},
		{
			// The surviving fields would come back in catalog order, which is
			// not the order declared here.
			name:     "the surviving fields reordered",
			declared: fields("city", "street"),
			current:  dbFields("street", "city"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			generated := &goschema.Database{CompositeTypes: []goschema.CompositeType{
				{Name: "addr", Fields: test.declared},
			}}
			database := &types.DBSchema{Composites: []types.DBComposite{
				{Name: "addr", Fields: test.current},
			}}
			diff := &difftypes.SchemaDiff{}

			compare.CompositeTypes(generated, database, diff, compare.CoverageOf(generated, database))

			c.Assert(diff.CompositeTypesModified, qt.HasLen, 1)
			c.Assert(addedNames(diff.CompositeTypesModified[0]), qt.DeepEquals, test.wantAdded)
			c.Assert(diff.CompositeTypesModified[0].AttributesRemoved, qt.DeepEquals, test.wantRemoved)
		})
	}
}

// TestCompositeTypes_AChangedFieldTypeIsNotAnAttributeDelta keeps the one
// operation PostgreSQL refuses out of the in-place path.
//
// `ALTER TYPE ... ALTER ATTRIBUTE` is answered with
// `cannot alter type "addr" because column "uses_addr.a" uses it` on 18.4, with
// CASCADE and without, so a field whose type changed has no in-place route at
// all and the rebuild keeps it.
func TestCompositeTypes_AChangedFieldTypeIsNotAnAttributeDelta(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{CompositeTypes: []goschema.CompositeType{
		{Name: "addr", Fields: []goschema.CompositeTypeField{
			{Name: "street", Type: "varchar(80)"},
			{Name: "city", Type: "text"},
		}},
	}}
	database := &types.DBSchema{Composites: []types.DBComposite{
		{Name: "addr", Fields: []types.DBCompositeField{
			{Name: "street", Type: "text"},
			{Name: "city", Type: "text"},
		}},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.CompositeTypes(generated, database, diff, compare.CoverageOf(generated, database))

	c.Assert(diff.CompositeTypesModified, qt.HasLen, 1)
	c.Assert(diff.CompositeTypesModified[0].AttributesAdded, qt.HasLen, 0)
	c.Assert(diff.CompositeTypesModified[0].AttributesRemoved, qt.HasLen, 0)
}

// fields builds declared text fields with the given names.
func fields(names ...string) []goschema.CompositeTypeField {
	built := make([]goschema.CompositeTypeField, 0, len(names))
	for _, name := range names {
		built = append(built, goschema.CompositeTypeField{Name: name, Type: "text"})
	}
	return built
}

// dbFields builds catalog text fields with the given names.
func dbFields(names ...string) []types.DBCompositeField {
	built := make([]types.DBCompositeField, 0, len(names))
	for _, name := range names {
		built = append(built, types.DBCompositeField{Name: name, Type: "text"})
	}
	return built
}

// addedNames lists the names of the attributes a diff reports as added.
func addedNames(compositeDiff difftypes.CompositeTypeDiff) []string {
	var names []string
	for _, attribute := range compositeDiff.AttributesAdded {
		names = append(names, attribute.Name)
	}
	return names
}
