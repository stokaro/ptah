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

// A PostgreSQL schema name may begin with a space, so a table the read holds
// in ` a` is removed by that name. Trimmed, the removal named `a.t`, the
// declared table of the same name, and the DROP destroyed it.
func TestCompareWithDatabaseInfo_RemovesATableInASpacedSchema(t *testing.T) {
	c := qt.New(t)
	column := catalog.Column{Name: "id", DataType: "integer", UDTName: "int4", IsNullable: "YES"}
	held := &catalog.Database{Tables: []catalog.Table{
		{Name: "t", Schema: "a", Type: "BASE TABLE", Columns: []catalog.Column{column}},
		{Name: "t", Schema: " a", Type: "BASE TABLE", Columns: []catalog.Column{column}},
	}}
	desired := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "a"}, {Name: " a"}},
		Tables:  []schemamodel.Table{{StructName: "T", Name: "t", Schema: "a"}},
		Fields:  []schemamodel.Field{{StructName: "T", Name: "id", Type: "integer", Nullable: true}},
	}
	info := catalog.ServerInfo{Dialect: "postgres", Schema: "public", IdentifierSemantics: identifier.ForDialect("postgres")}

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, held, info, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert([]string(diff.TablesRemoved.Names()), qt.DeepEquals, []string{`" a".t`})
	c.Assert(diff.TablesAdded.Names(), qt.HasLen, 0)
}
