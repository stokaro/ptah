package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/compare"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestDomains_AddRemoveModify(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{Domains: []goschema.Domain{
		{Name: "email", BaseType: "TEXT", NotNull: true},
		{Name: "changed", BaseType: "INTEGER", NotNull: true},
	}}
	database := &types.DBSchema{Domains: []types.DBDomain{
		{Name: "changed", BaseType: "integer", NotNull: false}, // not_null differs
		{Name: "legacy", BaseType: "TEXT"},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.Domains(generated, database, diff)

	c.Assert(diff.DomainsAdded, qt.DeepEquals, []string{"email"})
	c.Assert(diff.DomainsRemoved, qt.DeepEquals, []string{"legacy"})
	c.Assert(diff.DomainsModified, qt.HasLen, 1)
	c.Assert(diff.DomainsModified[0].DomainName, qt.Equals, "changed")
	c.Assert(diff.DomainsModified[0].Changes["not_null"], qt.Equals, "false -> true")
}

func TestDomains_TypeCaseInsensitiveNoChurn(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{Domains: []goschema.Domain{{Name: "email", BaseType: "TEXT"}}}
	database := &types.DBSchema{Domains: []types.DBDomain{{Name: "email", BaseType: "text"}}}
	diff := &difftypes.SchemaDiff{}

	compare.Domains(generated, database, diff)

	c.Assert(diff.DomainsAdded, qt.IsNil)
	c.Assert(diff.DomainsModified, qt.IsNil)
}

func TestCompositeTypes_AddRemoveModify(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{CompositeTypes: []goschema.CompositeType{
		{Name: "address", Fields: []goschema.CompositeTypeField{{Name: "street", Type: "TEXT"}, {Name: "zip", Type: "INTEGER"}}},
	}}
	database := &types.DBSchema{Composites: []types.DBComposite{
		{Name: "address", Fields: []types.DBCompositeField{{Name: "street", Type: "text"}}}, // field count differs
	}}
	diff := &difftypes.SchemaDiff{}

	compare.CompositeTypes(generated, database, diff)

	c.Assert(diff.CompositeTypesModified, qt.HasLen, 1)
	c.Assert(diff.CompositeTypesModified[0].TypeName, qt.Equals, "address")
}

func TestCompositeTypes_UnchangedNoChurn(t *testing.T) {
	c := qt.New(t)

	fields := []goschema.CompositeTypeField{{Name: "street", Type: "TEXT"}, {Name: "zip", Type: "INTEGER"}}
	generated := &goschema.Database{CompositeTypes: []goschema.CompositeType{{Name: "address", Fields: fields}}}
	database := &types.DBSchema{Composites: []types.DBComposite{
		{Name: "address", Fields: []types.DBCompositeField{{Name: "street", Type: "text"}, {Name: "zip", Type: "integer"}}},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.CompositeTypes(generated, database, diff)

	c.Assert(diff.CompositeTypesAdded, qt.IsNil)
	c.Assert(diff.CompositeTypesModified, qt.IsNil)
}

func TestRanges_AddRemoveByNameOnly(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{Ranges: []goschema.Range{{Name: "floatrange", Subtype: "float8"}}}
	// Subtype spelling differs (float8 vs double precision) but ranges compare by name only.
	database := &types.DBSchema{Ranges: []types.DBRange{{Name: "floatrange", Subtype: "double precision"}, {Name: "legacy", Subtype: "integer"}}}
	diff := &difftypes.SchemaDiff{}

	compare.Ranges(generated, database, diff)

	c.Assert(diff.RangesAdded, qt.IsNil)
	c.Assert(diff.RangesRemoved, qt.DeepEquals, []string{"legacy"})
}
