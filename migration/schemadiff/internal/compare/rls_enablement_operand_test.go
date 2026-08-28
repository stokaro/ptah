package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// TestRLSEnabledTables_AnAdditionCarriesItsDeclaration pins the operand half of
// the enablement lists.
//
// They used to be table names alone, so a planner rendering anything beyond the
// name -- a declared comment, on the targets that carry one -- had to find the
// declaration in a schema handed to it alongside the diff, and planned NOTHING
// for an enablement whose table that schema spelled differently
// (stokaro/ptah#2315).
//
// A removal carries the name and nothing else, and that is not an oversight:
// the enablement being removed is one the database reports and no declaration
// describes, so there is nothing else to carry.
func TestRLSEnabledTables_AnAdditionCarriesItsDeclaration(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "guarded", StructName: "Guarded"},
			{Name: "legacy", StructName: "Legacy"},
		},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{{
			StructName: "Guarded", Table: "guarded", Comment: "tenant isolation",
		}},
	}
	database := &catalog.Database{Tables: []catalog.Table{
		{Schema: "public", Name: "guarded"},
		{Schema: "public", Name: "legacy", RLSEnabled: true},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.RLSEnabledTablesWithSemantics(
		desired, database, diff, identifier.ForDialect("postgres"))

	c.Assert(diff.RLSEnabledTablesAdded, qt.HasLen, 1)
	c.Assert(diff.RLSEnabledTablesAdded[0].Table, qt.Equals, "guarded")
	c.Assert(diff.RLSEnabledTablesAdded[0].Comment, qt.Equals, "tenant isolation",
		qt.Commentf("the addition carries the declaration, not only its table name"))

	c.Assert(diff.RLSEnabledTablesRemoved, qt.HasLen, 1)
	c.Assert(diff.RLSEnabledTablesRemoved[0].Table, qt.Equals, "public.legacy")
	c.Assert(diff.RLSEnabledTablesRemoved[0], qt.DeepEquals,
		schemamodel.RLSEnabledTable{Table: "public.legacy"},
		qt.Commentf("a removal is one the database reports; there is no declaration to carry"))
}

// TestRLSEnabledTableChanges_NamesIsTheWireShape pins what the JSON keeps.
//
// `rls_enabled_tables_added` and `rls_enabled_tables_removed` have always been
// arrays of table names, and carrying the declaration in memory must not change
// what a stored plan holds.
func TestRLSEnabledTableChanges_NamesIsTheWireShape(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.RLSEnabledTableChanges{
		{StructName: "Guarded", Table: "guarded", Comment: "tenant isolation"},
		{Table: "public.legacy"},
	}

	c.Assert(changes.Names(), qt.DeepEquals, []string{"guarded", "public.legacy"})

	encoded, err := changes.MarshalJSON()
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Equals, `["guarded","public.legacy"]`)
}
