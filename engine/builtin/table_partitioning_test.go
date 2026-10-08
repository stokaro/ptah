package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// TestRender_TablePartitioning_FailurePath refuses a row table's YDB settings
// on every target without the key each needs, through the whole-schema
// render, a single CREATE TABLE and a change in place alike: built without
// them, the table would split, replicate and filter as the server's defaults
// say, and nothing would report the difference.
func TestRender_TablePartitioning_FailurePath(t *testing.T) {
	declared := &ast.YDBTablePartitioningSpec{MinPartitions: 3}
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}, YDBPartitioning: declared}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "int", Primary: true}},
	}
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{dialect: platform.Postgres, caps: capability.Postgres18()},
		{dialect: platform.MySQL, caps: capability.MySQL84()},
		{dialect: platform.SQLite, caps: capability.SQLite3()},
		{dialect: platform.ClickHouse, caps: capability.ClickHouse24()},
		{dialect: platform.SQLServer, caps: capability.SQLServer2022()},
		{dialect: platform.YDB, caps: capability.YDB262().With(capability.PartitioningOptions, false)},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(schema, test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, `table "t" declares its partitioning, which requires target capability partitioning_options, .*`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)

			// Validation renders no CREATE TABLE on most targets, so it refuses
			// the declaration itself.
			err = builtin.ValidateSchemaWithCapabilities(schema, test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, `table "t" declares its partitioning, which requires target capability partitioning_options, .*`)

			node := &ast.CreateTableNode{Name: "t", YDBPartitioning: declared, Columns: []*ast.ColumnNode{ast.NewColumn("id", "INTEGER").SetPrimary()}}
			sql, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, node)
			c.Assert(err, qt.ErrorMatches, `.*table "t" declares its partitioning, which requires target capability partitioning_options, .*`)
			c.Assert(sql, qt.Equals, "")

			change := &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{
				&ast.SetYDBTablePartitioningOperation{Partitioning: declared},
			}}
			sql, err = builtin.RenderSQLWithCapabilities(test.dialect, test.caps, change)
			c.Assert(err, qt.ErrorMatches, `.*changing the partitioning of table "t", which requires target capability partitioning_options, .*`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestRender_TablePartitioning_EachSettingNamesItsKey refuses each setting by
// the key it needs and no other, so a target gaining one key renders that
// setting and still refuses the rest.
func TestRender_TablePartitioning_EachSettingNamesItsKey(t *testing.T) {
	tests := []struct {
		name    string
		spec    *ast.YDBTablePartitioningSpec
		wantErr string
	}{
		{name: "a starting layout", spec: &ast.YDBTablePartitioningSpec{UniformPartitions: 4},
			wantErr: `table "t" declares its partitioning, which requires target capability partitioning_options, .*`},
		{name: "read replicas", spec: &ast.YDBTablePartitioningSpec{ReadReplicas: "PER_AZ:1"},
			wantErr: `table "t" declares its read replicas, which requires target capability read_replicas, .*`},
		{name: "a key bloom filter", spec: &ast.YDBTablePartitioningSpec{KeyBloomFilter: new(true)},
			wantErr: `table "t" declares its key bloom filter, which requires target capability key_bloom_filter, .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := &ast.CreateTableNode{Name: "t", YDBPartitioning: test.spec, Columns: []*ast.ColumnNode{ast.NewColumn("id", "INTEGER").SetPrimary()}}
			sql, err := builtin.RenderSQLWithCapabilities(platform.Postgres, capability.Postgres18(), node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
