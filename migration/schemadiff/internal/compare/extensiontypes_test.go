package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestExtensions_AnExtensionADeclaredTypeNeedsIsNotRemoved is stokaro/ptah#2389.
//
// The author declared a column, not an extension. Reading that as "the
// extension is unrequired" produced a plan that created the column and dropped
// the extension its type comes from -- one transaction contradicting itself,
// and on the orderings where the drop went first, an extension removed out from
// under an operator who never asked.
func TestExtensions_AnExtensionADeclaredTypeNeedsIsNotRemoved(t *testing.T) {
	tests := []struct {
		name      string
		declared  string
		extension string
	}{
		{name: "a vector column", declared: "vector(384)", extension: "vector"},
		{name: "half precision", declared: "halfvec(384)", extension: "vector"},
		{name: "sparse", declared: "sparsevec(384)", extension: "vector"},
		{name: "case and spacing", declared: "  VECTOR( 384 )  ", extension: "vector"},
		{name: "an array of them", declared: "vector(384)[]", extension: "vector"},
		{name: "a bare type with no modifier", declared: "hstore", extension: "hstore"},
		{name: "case insensitive", declared: "CITEXT", extension: "citext"},
		{name: "a label path", declared: "ltree", extension: "ltree"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.Extensions(
				schemaDeclaring(test.declared),
				databaseHolding(test.extension),
				diff, nil, compare.CoverageOf(nil, nil))

			c.Assert(diff.ExtensionsRemoved, qt.HasLen, 0,
				qt.Commentf("%s needs %s, and the plan proposed dropping it",
					test.declared, test.extension))
		})
	}
}

// TestExtensions_AnExtensionNothingDeclaresIsStillRemoved is the control.
//
// Every row above is satisfied by an implementation that stopped removing
// extensions altogether, which would be a different defect wearing the same
// green. An extension no declared type needs and no annotation names is still
// something the desired schema does not have.
func TestExtensions_AnExtensionNothingDeclaresIsStillRemoved(t *testing.T) {
	tests := []struct {
		name     string
		declared string
	}{
		// pg_trgm provides operators and index support and no type at all, so
		// no column can imply it.
		{name: "a type from another extension", declared: "vector(384)"},
		{name: "a core type", declared: "TEXT"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.Extensions(
				schemaDeclaring(test.declared),
				databaseHolding("pg_trgm"),
				diff, nil, compare.CoverageOf(nil, nil))

			c.Assert(diff.ExtensionsRemoved, qt.HasLen, 1)
			c.Assert(diff.ExtensionsRemoved[0].Name, qt.Equals, "pg_trgm")
		})
	}
}

// TestExtensions_ATypeNamedLikeAnExtensionTypeDoesNotCount pins the reading of
// the type name.
//
// The column type is matched whole, after its modifier and array marker are
// removed. A type whose name merely CONTAINS one of these -- a domain called
// `vector_id`, a table's own `hstore_key` -- names a different thing, and
// keeping an extension because of it would make an unrelated extension
// undroppable with no way for the author to say otherwise.
func TestExtensions_ATypeNamedLikeAnExtensionTypeDoesNotCount(t *testing.T) {
	tests := []struct {
		name     string
		declared string
	}{
		{name: "a longer name", declared: "vector_id"},
		{name: "a prefixed name", declared: "myvector"},
		{name: "a similar core type", declared: "VARCHAR(255)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.Extensions(
				schemaDeclaring(test.declared),
				databaseHolding("vector"),
				diff, nil, compare.CoverageOf(nil, nil))

			c.Assert(diff.ExtensionsRemoved, qt.HasLen, 1)
			c.Assert(diff.ExtensionsRemoved[0].Name, qt.Equals, "vector")
		})
	}
}

// TestExtensions_ADeclaredExtensionIsStillKept is the path that already worked.
//
// An author who writes the annotation gets what they wrote, and this stays
// asserted so the new rule is measured as an addition rather than as the only
// thing keeping extensions alive.
func TestExtensions_ADeclaredExtensionIsStillKept(t *testing.T) {
	c := qt.New(t)
	desired := schemaDeclaring("TEXT")
	desired.Extensions = []schemamodel.Extension{{Name: "pg_trgm"}}
	diff := &difftypes.SchemaDiff{}

	compare.Extensions(desired, databaseHolding("pg_trgm"), diff, nil, compare.CoverageOf(nil, nil))

	c.Assert(diff.ExtensionsRemoved, qt.HasLen, 0)
	c.Assert(diff.ExtensionsAdded, qt.HasLen, 0)
}

// schemaDeclaring is a desired schema holding one column of a given type.
func schemaDeclaring(declared string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Article", Name: "articles"}},
		Fields: []schemamodel.Field{
			{StructName: "Article", Name: "value", Type: declared},
		},
	}
}

// databaseHolding is a live database with one extension installed.
func databaseHolding(name string) *catalog.Database {
	return &catalog.Database{Extensions: []catalog.Extension{{Name: name}}}
}
