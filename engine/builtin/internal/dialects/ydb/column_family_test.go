package ydb_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin/internal/dialects/ydb"
)

// withFamilies is a table t keyed on an Int64 id, with nullable columns a and b
// and a NOT NULL column c with a default, in the given column families, which
// the YDB owner's facet declares.
func withFamilies(families ...ydbschema.ColumnFamily) *ast.CreateTableNode {
	return &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{
		ast.NewColumn("id", "BIGINT").SetPrimary(),
		ast.NewColumn("a", "TEXT"),
		ast.NewColumn("b", "BYTEA"),
		ast.NewColumn("c", "INTEGER").SetNotNull().SetDefault("7"),
	}, Facets: must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{Families: families}))}
}

// changeFamilies is the in-place change of table t's families from before to
// after, in its extension envelope.
func changeFamilies(after, before []ydbschema.ColumnFamily) *ast.AlterTableNode {
	change := ydbdiff.ColumnFamilies{After: &ydbschema.DesiredColumnFamilies{Families: after}}
	if before != nil {
		change.Before = &ydbschema.ObservedColumnFamilies{Families: before}
	}
	return alter(&ast.ExtensionAlterOperation{Payload: &ydbast.AlterColumnFamilies{Change: change}})
}

// TestRender_ColumnFamilies_HappyPath pins how a table's column families are
// written: in CREATE TABLE, a column naming its family right after its type,
// the only place YDB 25.1 takes it, and each family's stated settings in a
// FAMILY entry; on a table that exists, one ALTER TABLE that adds families,
// sets the settings the declaration states and moves columns, and leaves
// alone what the declaration does not state. Each rendering was applied to local-ydb
// 26.2.1.14 one statement per query and read back through DescribeTable, and
// on 25.1.4.7 where it names no cache mode.
func TestRender_ColumnFamilies_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "a new table's families",
			caps: capability.YDB251(),
			node: withFamilies(
				ydbschema.ColumnFamily{Name: "default", Compression: "lz4"},
				ydbschema.ColumnFamily{Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"b", "c"}},
				ydbschema.ColumnFamily{Name: "empty"},
			),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `a` Utf8,\n" +
				"    `b` String FAMILY `cold`,\n" +
				"    `c` Int32 FAMILY `cold` NOT NULL DEFAULT 7,\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    FAMILY `cold` (DATA = 'hdd', COMPRESSION = 'lz4'),\n" +
				"    FAMILY `default` (COMPRESSION = 'lz4'),\n" +
				"    FAMILY `empty` ()\n" +
				");\n",
		},
		{
			name: "a cache mode",
			caps: capability.YDB262(),
			node: withFamilies(ydbschema.ColumnFamily{Name: "hot", CacheMode: "in_memory", Columns: []string{"a"}}),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `a` Utf8 FAMILY `hot`,\n" +
				"    `b` String,\n" +
				"    `c` Int32 NOT NULL DEFAULT 7,\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    FAMILY `hot` (CACHE_MODE = 'in_memory')\n" +
				");\n",
		},
		{
			name: "the default family stating YDB's own compression",
			caps: capability.YDB251(),
			node: withFamilies(ydbschema.ColumnFamily{Name: "default", Compression: "off"}),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `a` Utf8,\n" +
				"    `b` String,\n" +
				"    `c` Int32 NOT NULL DEFAULT 7,\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    FAMILY `default` (COMPRESSION = 'off')\n" +
				");\n",
		},
		{
			name: "the default family stating nothing",
			caps: capability.YDB251(),
			node: withFamilies(ydbschema.ColumnFamily{Name: "default"}),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `a` Utf8,\n" +
				"    `b` String,\n" +
				"    `c` Int32 NOT NULL DEFAULT 7,\n" +
				"    PRIMARY KEY (`id`)\n" +
				");\n",
		},
		{
			name: "families changed in place",
			caps: capability.YDB251(),
			node: changeFamilies(
				[]ydbschema.ColumnFamily{
					{Name: "cold", Compression: "off", Columns: []string{"a"}},
					{Name: "warm", Compression: "lz4", Columns: []string{"b"}},
				},
				[]ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"b"}}},
			),
			want: "ALTER TABLE `t` ADD FAMILY `warm` (COMPRESSION = 'lz4'), ALTER FAMILY `cold` SET COMPRESSION 'off', " +
				"ALTER COLUMN `a` SET FAMILY `cold`, ALTER COLUMN `b` SET FAMILY `warm`;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRender_ColumnFamilies_RefusesByCapability names the key a target lacks
// for what a statement writes, so a line that gains it is a preset change. A
// regular cache is refused too: 25.1 answers CACHE_MODE with `Unknown table
// setting: CACHE_MODE` whatever its value.
func TestRender_ColumnFamilies_RefusesByCapability(t *testing.T) {
	hot := ydbschema.ColumnFamily{Name: "hot", CacheMode: "in_memory"}
	tests := []struct {
		name    string
		caps    capability.Capabilities
		node    ast.Node
		wantKey capability.Capability
		wantErr string
	}{
		{
			name: "a cache mode on 25.3", caps: capability.YDB253(),
			node:    withFamilies(hot),
			wantKey: capability.ColumnFamilyCacheMode,
			wantErr: `the column family cache mode of table "t", which requires target capability column_family_cache_mode, .*`,
		},
		{
			name: "a regular cache on 25.1", caps: capability.YDB251(),
			node:    withFamilies(ydbschema.ColumnFamily{Name: "default", CacheMode: "regular"}),
			wantKey: capability.ColumnFamilyCacheMode,
			wantErr: `the column family cache mode of table "t", which requires target capability column_family_cache_mode, .*`,
		},
		{
			name: "a family added with a cache mode on 25.1", caps: capability.YDB251(),
			node:    changeFamilies([]ydbschema.ColumnFamily{hot}, nil),
			wantKey: capability.ColumnFamilyCacheMode,
			wantErr: `changing the column family cache mode of table "t", which requires target capability column_family_cache_mode, .*`,
		},
		{
			name: "families without the key", caps: capability.YDB262().With(capability.ColumnFamilies, false),
			node:    withFamilies(ydbschema.ColumnFamily{Name: "cold", Columns: []string{"a"}}),
			wantKey: capability.ColumnFamilies,
			wantErr: `the column families of table "t", which requires target capability column_families, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			refusal, ok := errors.AsType[*ptaherr.CapabilityError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(refusal.Feature, qt.Equals, string(test.wantKey))
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestRender_ColumnFamilies_FailurePath refuses what YDB refuses on every
// line, and a keep_in_memory no statement writes, with the server's reason,
// before any statement is written.
func TestRender_ColumnFamilies_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		node    ast.Node
		wantErr string
	}{
		{
			name:    "a key column in a family",
			node:    withFamilies(ydbschema.ColumnFamily{Name: "cold", Columns: []string{"id"}}),
			wantErr: "table \"t\": column family \"cold\" names key column \"id\", .*`Key column 'id' must belong to the default family`.*",
		},
		{
			name:    "a column the table does not declare",
			node:    withFamilies(ydbschema.ColumnFamily{Name: "cold", Columns: []string{"z"}}),
			wantErr: `table "t": column family "cold" names column "z", which the table does not declare`,
		},
		{
			name:    "keep_in_memory in a new table",
			node:    withFamilies(ydbschema.ColumnFamily{Name: "default", Compression: "lz4", KeepInMemory: true}),
			wantErr: `table "t": column family "default" keeps its columns in memory \(keep_in_memory\), and YQL has no .*`,
		},
		{
			name: "keep_in_memory stated for a table without it",
			node: changeFamilies([]ydbschema.ColumnFamily{{Name: "default", KeepInMemory: true}},
				[]ydbschema.ColumnFamily{{Name: "default", Compression: "off"}}),
			wantErr: `table "t": column family "default" keeps its columns in memory \(keep_in_memory\) on one side only, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(capability.YDB262()).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// A column two families name is a value no source writes, which the model
// itself refuses before any statement is written.
func TestRender_ColumnFamilies_RefusesAnInvalidDeclaration(t *testing.T) {
	c := qt.New(t)
	node := withFamilies(ydbschema.ColumnFamily{Name: "cold", Columns: []string{"a"}},
		ydbschema.ColumnFamily{Name: "warm", Columns: []string{"a"}})

	got, err := ydb.NewWithCapabilities(capability.YDB262()).Render(node)

	c.Assert(err, qt.ErrorMatches, `the column families of table "t": .*column "a" is in two column families, "cold" and "warm"`)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(got, qt.Equals, "")
}
