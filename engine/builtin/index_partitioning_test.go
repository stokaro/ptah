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

// indexSettingsFacets is an index's YDB settings as the YDB owner's facet.
func indexSettingsFacets(settings ydbschema.IndexPartitioning) schemaext.Facets {
	return must.Must(schemaext.NewFacets(&ydbschema.DesiredIndexPartitioning{IndexPartitioning: settings}))
}

// TestRender_IndexPartitioning_FailurePath refuses an index's partitioning on
// every target without index_partitioning, through the whole-schema render, a
// single index node and a change of partitioning alike: built without it, the
// index would split as the server's defaults say, and nothing would report the
// difference.
func TestRender_IndexPartitioning_FailurePath(t *testing.T) {
	declared := ydbschema.IndexPartitioning{MinPartitions: 3}
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "int", Primary: true},
			{StructName: "T", Name: "a", Type: "int", Nullable: true},
		},
		Indexes: []schemamodel.Index{{StructName: "T", Name: "k_a", TableName: "t", Fields: []string{"a"},
			Facets: indexSettingsFacets(declared)}},
	}
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{dialect: platform.Postgres, caps: capability.Postgres18()},
		{dialect: platform.MySQL, caps: capability.MySQL84()},
		{dialect: platform.SQLite, caps: capability.SQLite3()},
		{dialect: platform.ClickHouse, caps: capability.ClickHouse24()},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			const refused = `.*facet "ptah.run/ydb/index-partitioning" is not (registered for target|supported).*`

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(schema, test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, refused)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)

			node := &ast.IndexNode{Name: "k_a", Table: "t", Columns: []string{"a"}, Facets: indexSettingsFacets(declared)}
			sql, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")

			sql, err = builtin.RenderSQLWithCapabilities(test.dialect, test.caps, indexSettingsChange(declared))
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestRender_IndexPartitioning_RefusesWithoutTheKey refuses an index's
// partitioning on a YDB target without index_partitioning, on every path a
// declaration or a change reaches it by.
func TestRender_IndexPartitioning_RefusesWithoutTheKey(t *testing.T) {
	c := qt.New(t)
	declared := ydbschema.IndexPartitioning{MinPartitions: 3}
	caps := capability.YDB262().With(capability.IndexPartitioning, false)
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "int", Primary: true},
			{StructName: "T", Name: "a", Type: "int", Nullable: true},
		},
		Indexes: []schemamodel.Index{{StructName: "T", Name: "k_a", TableName: "t", Fields: []string{"a"},
			Facets: indexSettingsFacets(declared)}},
	}
	const declares = `.*index "k_a" declares its partitioning, which requires target capability index_partitioning, .*`

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(schema, platform.YDB, caps)
	c.Assert(err, qt.ErrorMatches, declares)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(statements, qt.IsNil)

	node := &ast.IndexNode{Name: "k_a", Table: "t", Columns: []string{"a"}, Facets: indexSettingsFacets(declared)}
	sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, caps, node)
	c.Assert(err, qt.ErrorMatches, declares)
	c.Assert(sql, qt.Equals, "")

	sql, err = builtin.RenderSQLWithCapabilities(platform.YDB, caps, indexSettingsChange(declared))
	c.Assert(err, qt.ErrorMatches, `.*changing the partitioning of index "k_a" of table "t", which requires target capability index_partitioning, .*`)
	c.Assert(sql, qt.Equals, "")
}

// indexSettingsChange is the in-place change that gives index k_a of table t
// settings.
func indexSettingsChange(settings ydbschema.IndexPartitioning) *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{
		Payload: &ydbast.AlterIndexPartitioning{Index: "k_a", Change: ydbdiff.IndexPartitioning{
			After: &ydbschema.DesiredIndexPartitioning{IndexPartitioning: settings},
		}},
	}}}
}
