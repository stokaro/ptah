package sqlitetable_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/sqlite/sqlitetable"
)

// TestHCL_EncodeHCL_HappyPath writes a table's options, declared or read, as
// the strict and without_rowid attributes of the writer's table block, each
// only when set. A table without either, and a virtual table's module, take
// nothing.
func TestHCL_EncodeHCL_HappyPath(t *testing.T) {
	c := qt.New(t)
	table := func(name string, value schemaext.Value) hclext.TableFacets {
		return hclext.TableFacets{Table: hclext.Table{Schema: "main", Name: name}, Facets: must.Must(schemaext.NewFacets(value))}
	}
	yes := func(name string) hclext.EncodedAttribute {
		return hclext.EncodedAttribute{Name: name, Value: hclext.Value{Kind: hclext.Bool, Bool: true}}
	}
	request := hclext.EncodeRequest{Tables: []hclext.TableFacets{
		table("both", &sqlitetable.DesiredTable{Options: sqlitetable.Options{Strict: true, WithoutRowID: true}}),
		table("strict", &sqlitetable.ObservedTable{Options: sqlitetable.Options{Strict: true}}),
		table("rowid", &sqlitetable.DesiredTable{Options: sqlitetable.Options{WithoutRowID: true}}),
		table("plain", &sqlitetable.DesiredTable{}),
		table("virtual", &sqlitetable.DesiredVirtual{Virtual: sqlitetable.Virtual{Module: "fts5", Arguments: "body"}}),
	}}

	result, err := sqlitetable.HCL().Encoder.EncodeHCL(c.Context(), request)

	c.Assert(err, qt.IsNil)
	slot := func(name string) hclext.Slot {
		return hclext.Slot{Kind: hclext.SlotTable, Table: hclext.Table{Schema: "main", Name: name}}
	}
	c.Assert(result, qt.DeepEquals, hclext.EncodeResult{Attributes: []hclext.SlotAttributes{
		{Slot: slot("both"), Attributes: []hclext.EncodedAttribute{yes("strict"), yes("without_rowid")}},
		{Slot: slot("strict"), Attributes: []hclext.EncodedAttribute{yes("strict")}},
		{Slot: slot("rowid"), Attributes: []hclext.EncodedAttribute{yes("without_rowid")}},
	}})
}

// TestHCL_EncodesTheOwnersKinds writes the table options and accounts for a
// virtual table's module, reads no block, and claims every table's options:
// a table block without them describes a table with neither.
func TestHCL_EncodesTheOwnersKinds(t *testing.T) {
	c := qt.New(t)

	extension := sqlitetable.HCL()

	c.Assert(extension.Owner, qt.Equals, sqlitetable.Owner)
	c.Assert(extension.Kinds, qt.DeepEquals, []schemaext.Kind{sqlitetable.TableKind, sqlitetable.VirtualKind})
	c.Assert(extension.Blocks, qt.IsNil)
	claim, err := extension.Coverage(hclext.CoverageRequest{})
	c.Assert(err, qt.IsNil)
	c.Assert(claim.Lookup(sqlitetable.TableKind, objectidentity.NewBuilder(identifier.ForDialect("sqlite")).Table("t")).State,
		qt.Equals, schemaext.Complete)
}
