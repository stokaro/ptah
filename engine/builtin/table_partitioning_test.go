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

// settingsFacets is a table's YDB settings as the YDB owner's facet.
func settingsFacets(settings ydbschema.TablePartitioning) schemaext.Facets {
	return must.Must(schemaext.NewFacets(&ydbschema.DesiredTablePartitioning{TablePartitioning: settings}))
}

// partitionedSchema is a table t declaring settings.
func partitionedSchema(settings ydbschema.TablePartitioning) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}, Facets: settingsFacets(settings)}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "int", Primary: true}},
	}
}

// partitionedNode is table t as a new table's node, declaring settings.
func partitionedNode(settings ydbschema.TablePartitioning) *ast.CreateTableNode {
	return &ast.CreateTableNode{Name: "t", Facets: settingsFacets(settings),
		Columns: []*ast.ColumnNode{ast.NewColumn("id", "INTEGER").SetPrimary()}}
}

// settingsChange is the in-place change that gives table t settings.
func settingsChange(settings ydbschema.TablePartitioning) *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{
		Payload: &ydbast.AlterTablePartitioning{Change: ydbdiff.TablePartitioning{After: &ydbschema.DesiredTablePartitioning{TablePartitioning: settings}}},
	}}}
}

// TestRender_TablePartitioning_FailurePath refuses a row table's YDB settings
// on every other target, through the whole-schema render, the validation a
// plan runs before it compares, a new table's node and a change of one alike:
// built without them, the table would split, replicate and filter as the
// server's defaults say, and nothing would report the difference.
func TestRender_TablePartitioning_FailurePath(t *testing.T) {
	declared := ydbschema.TablePartitioning{MinPartitions: 3}
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{dialect: platform.Postgres, caps: capability.Postgres18()},
		{dialect: platform.MySQL, caps: capability.MySQL84()},
		{dialect: platform.SQLite, caps: capability.SQLite3()},
		{dialect: platform.ClickHouse, caps: capability.ClickHouse24()},
		{dialect: platform.SQLServer, caps: capability.SQLServer2022()},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			const refused = `.*facet "ptah.run/ydb/table-partitioning" is not (registered for target|supported).*`

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(partitionedSchema(declared), test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, refused)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)
			c.Assert(builtin.ValidateSchemaWithCapabilities(partitionedSchema(declared), test.dialect, test.caps), qt.ErrorMatches, refused)
			sql, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, partitionedNode(declared))
			c.Assert(err, qt.ErrorMatches, refused)
			c.Assert(sql, qt.Equals, "")
			sql, err = builtin.RenderSQLWithCapabilities(test.dialect, test.caps, settingsChange(declared))
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestRender_TablePartitioning_RefusesWithoutTheKey refuses each setting on a
// YDB target by the key it needs and no other, on every path a declaration or
// a change reaches it by, so a target gaining one key renders that setting
// and still refuses the rest.
func TestRender_TablePartitioning_RefusesWithoutTheKey(t *testing.T) {
	tests := []struct {
		name     string
		settings ydbschema.TablePartitioning
		key      capability.Capability
		what     string
	}{
		{name: "partitioning", settings: ydbschema.TablePartitioning{MinPartitions: 3}, key: capability.PartitioningOptions, what: "partitioning"},
		{name: "a starting layout", settings: ydbschema.TablePartitioning{UniformPartitions: 4}, key: capability.PartitioningOptions, what: "partitioning"},
		{name: "read replicas", settings: ydbschema.TablePartitioning{ReadReplicas: "PER_AZ:1"}, key: capability.ReadReplicas, what: "read replicas"},
		{name: "a key bloom filter", settings: ydbschema.TablePartitioning{KeyBloomFilter: new(true)}, key: capability.KeyBloomFilter, what: "key bloom filter"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.YDB262().With(test.key, false)
			declares := `.*table "t" declares its ` + test.what + `, which requires target capability ` + string(test.key) + `, .*`

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(partitionedSchema(test.settings), platform.YDB, caps)
			c.Assert(err, qt.ErrorMatches, declares)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)
			c.Assert(builtin.ValidateSchemaWithCapabilities(partitionedSchema(test.settings), platform.YDB, caps), qt.ErrorMatches, declares)
			sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, caps, partitionedNode(test.settings))
			c.Assert(err, qt.ErrorMatches, declares)
			c.Assert(sql, qt.Equals, "")
		})
	}
	c := qt.New(t)
	sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262().With(capability.PartitioningOptions, false),
		settingsChange(ydbschema.TablePartitioning{MinPartitions: 3}))
	c.Assert(err, qt.ErrorMatches, `.*changing the partitioning of table "t", which requires target capability partitioning_options, .*`)
	c.Assert(sql, qt.Equals, "")
}
