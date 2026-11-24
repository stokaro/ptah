package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// spacedTablesHeld is a read holding the tables " t" and t, which PostgreSQL
// keeps as two.
func spacedTablesHeld() *catalog.Database {
	column := catalog.Column{Name: "id", DataType: "integer", UDTName: "int4", IsNullable: "YES"}
	return &catalog.Database{Tables: []catalog.Table{
		{Name: " t", Schema: "public", Type: "BASE TABLE", Columns: []catalog.Column{column}},
		{Name: "t", Schema: "public", Type: "BASE TABLE", Columns: []catalog.Column{column}},
	}}
}

// spacedTablesInfo is the PostgreSQL server the reads come from.
var spacedTablesInfo = catalog.ServerInfo{Dialect: "postgres", Schema: "public", IdentifierSemantics: identifier.ForDialect("postgres")}

// A table the read holds as " t" is the table a declaration of " t" names, and
// t beside it is another: the comparison pairs each with its own declaration
// and plans the new column on " t" alone. Keyed without the space, the two
// tables would be one identity, which the comparison refuses.
func TestCompareWithDatabaseInfo_PairsATableWithASpacedName(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Spaced", Name: " t"}, {StructName: "Plain", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "Spaced", Name: "id", Type: "integer", Nullable: true},
			{StructName: "Spaced", Name: "extra", Type: "text", Nullable: true},
			{StructName: "Plain", Name: "id", Type: "integer", Nullable: true},
		},
	}

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, spacedTablesHeld(), spacedTablesInfo, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesAdded.Names(), qt.HasLen, 0)
	c.Assert(diff.TablesRemoved.Names(), qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].TableName, qt.Equals, `" t"`)
	c.Assert(diff.TablesModified[0].ColumnsAdded.Names(), qt.DeepEquals, []string{"extra"})
}

// The table the declaration leaves out is " t", and its removal names it
// quoted: a removal of the bare name would drop t.
func TestCompareWithDatabaseInfo_RemovesATableWithASpacedName(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Plain", Name: "t"}},
		Fields: []schemamodel.Field{{StructName: "Plain", Name: "id", Type: "integer", Nullable: true}},
	}

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, spacedTablesHeld(), spacedTablesInfo, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert([]string(diff.TablesRemoved.Names()), qt.DeepEquals, []string{`public." t"`})
	c.Assert(diff.TablesAdded.Names(), qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
}
