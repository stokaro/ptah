package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
)

// familySchema is a table t whose body column sits in family cold, declared as
// the YDB owner's facet.
func familySchema(families ...ydbschema.ColumnFamily) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"},
			Facets: must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{Families: families}))}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "int", Primary: true},
			{StructName: "T", Name: "body", Type: "TEXT", Nullable: true},
		},
	}
}

var coldBody = ydbschema.ColumnFamily{Name: "cold", Compression: "lz4", Columns: []string{"body"}}

// familyNode is table t as a new table's node, with families declared.
func familyNode(families ...ydbschema.ColumnFamily) *ast.CreateTableNode {
	return &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{
		ast.NewColumn("id", "INTEGER").SetPrimary(), ast.NewColumn("body", "TEXT"),
	}, Facets: must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{Families: families}))}
}

// familyChange is the in-place change that adds family cold to table t.
func familyChange() *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{
		Payload: &ydbast.AlterColumnFamilies{Change: ydbdiff.ColumnFamilies{After: &ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{coldBody}}}},
	}}}
}

// TestRender_ColumnFamilies_FailurePath refuses YDB column families on every
// other target, through the whole-schema render, the validation a plan runs
// before it compares, a new table's node and a change of one alike: built
// without them, every column would sit in one storage pool, uncompressed, and
// nothing would report the difference. Even the default family stating
// nothing is refused there, since a family is YDB's alone.
func TestRender_ColumnFamilies_FailurePath(t *testing.T) {
	tests := []struct {
		dialect  string
		caps     capability.Capabilities
		families []ydbschema.ColumnFamily
	}{
		{dialect: platform.Postgres, caps: capability.Postgres18(), families: []ydbschema.ColumnFamily{coldBody}},
		{dialect: platform.CockroachDB, caps: capability.CockroachDB26(), families: []ydbschema.ColumnFamily{coldBody}},
		{dialect: platform.MySQL, caps: capability.MySQL84(), families: []ydbschema.ColumnFamily{coldBody}},
		{dialect: platform.SQLite, caps: capability.SQLite3(), families: []ydbschema.ColumnFamily{coldBody}},
		{dialect: platform.ClickHouse, caps: capability.ClickHouse24(), families: []ydbschema.ColumnFamily{coldBody}},
		{dialect: platform.SQLServer, caps: capability.SQLServer2022(), families: []ydbschema.ColumnFamily{coldBody}},
		{dialect: platform.Oracle, caps: capability.Oracle23(), families: []ydbschema.ColumnFamily{coldBody}},
		{dialect: platform.Postgres, caps: capability.Postgres18(), families: []ydbschema.ColumnFamily{{Name: "default"}}},
	}

	for _, test := range tests {
		t.Run(test.dialect+"/"+test.families[0].Name, func(t *testing.T) {
			c := qt.New(t)
			const refused = `.*facet "ptah.run/ydb/column-families" is not (registered for target|supported).*`

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(familySchema(test.families...), test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, refused)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)
			err = builtin.ValidateSchemaWithCapabilities(familySchema(test.families...), test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, refused)
			sql, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, familyNode(test.families...))
			c.Assert(err, qt.ErrorMatches, refused)
			c.Assert(sql, qt.Equals, "")
			sql, err = builtin.RenderSQLWithCapabilities(test.dialect, test.caps, familyChange())
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// A YDB target without the column_families key refuses the families by the
// key it lacks, on every path a declaration or a change reaches it by.
func TestRender_ColumnFamilies_RefusesWithoutTheKey(t *testing.T) {
	c := qt.New(t)
	caps := capability.YDB262().With(capability.ColumnFamilies, false)
	const declares = `.*table "t" declares column families, which requires target capability column_families, .*`

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(familySchema(coldBody), platform.YDB, caps)
	c.Assert(err, qt.ErrorMatches, declares)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(statements, qt.IsNil)
	c.Assert(builtin.ValidateSchemaWithCapabilities(familySchema(coldBody), platform.YDB, caps), qt.ErrorMatches, declares)
	sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, caps, familyNode(coldBody))
	c.Assert(err, qt.ErrorMatches, `.*the column families of table "t", which requires target capability column_families, .*`)
	c.Assert(sql, qt.Equals, "")
	sql, err = builtin.RenderSQLWithCapabilities(platform.YDB, caps, familyChange())
	c.Assert(err, qt.ErrorMatches, `.*changing the column families of table "t", which requires target capability column_families, .*`)
	c.Assert(sql, qt.Equals, "")
}

// TestRender_ColumnFamilies_HappyPath writes a declared table's families on
// YDB through the whole-schema render, and a table declaring only the default
// family stating no setting needs no key there.
func TestRender_ColumnFamilies_HappyPath(t *testing.T) {
	c := qt.New(t)

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(familySchema(ydbschema.ColumnFamily{Name: "default", Compression: "lz4"}),
		platform.YDB, capability.YDB251())
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE TABLE `t` (\n    `id` Int32 NOT NULL,\n    `body` Utf8,\n    PRIMARY KEY (`id`),\n    FAMILY `default` (COMPRESSION = 'lz4')\n);\n",
	})
	_, err = builtin.GetOrderedCreateStatementsWithCapabilities(familySchema(ydbschema.ColumnFamily{Name: "default"}), platform.YDB,
		capability.YDB262().With(capability.ColumnFamilies, false))
	c.Assert(err, qt.IsNil)
}
