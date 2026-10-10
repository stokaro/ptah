package ydbrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbrender"
	"ptah.run/dialect/ydb/ydbschema"
)

func familyRegistry() renderer.Extensions {
	return must.Must(renderer.NewExtensions(ydbrender.ColumnFamiliesHandler()))
}

func familyOperation(before, after []ydbschema.ColumnFamily) *ydbast.AlterColumnFamilies {
	op := &ydbast.AlterColumnFamilies{Change: ydbdiff.ColumnFamilies{After: &ydbschema.DesiredColumnFamilies{Families: after}}}
	if before != nil {
		op.Change.Before = &ydbschema.ObservedColumnFamilies{Families: before}
	}
	return op
}

// TestColumnFamiliesHandler_RendersOneStatement pins the one ALTER TABLE a
// change lowers to: the families it adds, then the settings it sets, then the
// columns it moves, each applied to local-ydb 26.2 and 25.1 by the live tests.
func TestColumnFamiliesHandler_RendersOneStatement(t *testing.T) {
	tests := []struct {
		name          string
		table         string
		before, after []ydbschema.ColumnFamily
		want          string
	}{
		{
			name:   "a family added, a setting set and a column moved",
			table:  "docs",
			before: []ydbschema.ColumnFamily{{Name: "default", Compression: "off"}, {Name: "cold", Compression: "off", Columns: []string{"body"}}},
			after:  []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Compression: "lz4"}, {Name: "warm", CacheMode: "in_memory", Columns: []string{"body"}}},
			want: "ALTER TABLE `docs` ADD FAMILY `warm` (CACHE_MODE = 'in_memory'), ALTER FAMILY `cold` SET DATA 'hdd', " +
				"ALTER FAMILY `cold` SET COMPRESSION 'lz4', ALTER COLUMN `body` SET FAMILY `warm`;",
		},
		{
			name:   "a column back to the default family of a table under a schema",
			table:  "app.docs",
			before: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}},
			after:  []ydbschema.ColumnFamily{{Name: "cold"}},
			want:   "ALTER TABLE `app/docs` ALTER COLUMN `body` SET FAMILY `default`;",
		},
		{
			name:  "families on a table the read found without them",
			table: "docs",
			after: []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4"}, {Name: "cold", Columns: []string{"body"}}},
			want: "ALTER TABLE `docs` ADD FAMILY `cold` (), ALTER FAMILY `default` SET COMPRESSION 'lz4', " +
				"ALTER COLUMN `body` SET FAMILY `cold`;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := familyRegistry().Render(renderer.ExtensionContext{
				Target: "ydb", Capabilities: capability.YDB262(), Parent: &ast.AlterTableNode{Name: test.table},
			}, ast.AlterExtension, familyOperation(test.before, test.after))

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{test.want})
		})
	}
}

func TestColumnFamiliesHandler_FailurePath(t *testing.T) {
	parent := &ast.AlterTableNode{Name: "docs"}
	cold := []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}}
	tests := []struct {
		name    string
		ctx     renderer.ExtensionContext
		op      *ydbast.AlterColumnFamilies
		wantErr string
	}{
		{name: "another target", ctx: renderer.ExtensionContext{Target: "spanner", Capabilities: capability.SpannerPostgres(), Parent: parent},
			op: familyOperation(nil, cold), wantErr: `YDB column families cannot be rendered for "spanner"`},
		{name: "a target without column families", ctx: renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262().With(capability.ColumnFamilies, false), Parent: parent},
			op:      familyOperation(nil, cold),
			wantErr: `changing the column families of table "docs", which requires target capability column_families, unavailable on this ydb target`},
		{name: "a cache mode without its key", ctx: renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262().With(capability.ColumnFamilyCacheMode, false), Parent: parent},
			op:      familyOperation(nil, []ydbschema.ColumnFamily{{Name: "cold", CacheMode: "regular"}}),
			wantErr: `changing the column family cache mode of table "docs", which requires target capability column_family_cache_mode, unavailable on this ydb target`},
		{name: "a keep_in_memory no statement writes", ctx: renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262(), Parent: parent},
			op:      familyOperation([]ydbschema.ColumnFamily{{Name: "cold"}}, []ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4", KeepInMemory: true}}),
			wantErr: `table "docs": column family "cold" keeps its columns in memory \(keep_in_memory\) on one side only, .*`},
		{name: "no after", ctx: renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262(), Parent: parent},
			op: &ydbast.AlterColumnFamilies{}, wantErr: `the column families of table "docs": .*requires the families the table ends up holding`},
		{name: "a change that changes nothing", ctx: renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262(), Parent: parent},
			op: familyOperation(cold, cold), wantErr: `the column families of table "docs": .*operands contain no change`},
		{name: "no parent", ctx: renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262()},
			op: familyOperation(nil, cold), wantErr: `.*requires an ALTER TABLE parent.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := familyRegistry().Render(test.ctx, ast.AlterExtension, test.op)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestCreateTableFamilies_HappyPath pins what a CREATE TABLE writes: the
// family entries after the key, and the clause each column's definition
// carries, which is nothing for a column in the default family.
func TestCreateTableFamilies_HappyPath(t *testing.T) {
	c := qt.New(t)
	facets := must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{
		{Name: "default", Compression: "off"}, {Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"body"}},
	}}))

	families, err := ydbrender.CreateTableFamilies(capability.YDB262(), "docs", facets, []string{"id", "body", "blob"}, []string{"id"})
	none, noneErr := ydbrender.CreateTableFamilies(capability.Capabilities{}, "docs", schemaext.Facets{}, []string{"id"}, []string{"id"})

	c.Assert(err, qt.IsNil)
	c.Assert(families.Entries, qt.DeepEquals, []string{"FAMILY `cold` (DATA = 'hdd', COMPRESSION = 'lz4')", "FAMILY `default` (COMPRESSION = 'off')"})
	c.Assert(families.ColumnClause("body"), qt.Equals, " FAMILY `cold`")
	c.Assert(families.ColumnClause("blob"), qt.Equals, "")
	c.Assert(noneErr, qt.IsNil)
	c.Assert(none.Entries, qt.IsNil)
	c.Assert(none.ColumnClause("id"), qt.Equals, "")
}

// TestCreateTableFamilies_FailurePath refuses families a CREATE TABLE cannot
// write, read against the table's columns and key.
func TestCreateTableFamilies_FailurePath(t *testing.T) {
	declared := func(families ...ydbschema.ColumnFamily) schemaext.Facets {
		return must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{Families: families}))
	}
	tests := []struct {
		name    string
		caps    capability.Capabilities
		facets  schemaext.Facets
		wantErr string
	}{
		{name: "a column the table does not declare", caps: capability.YDB262(), facets: declared(ydbschema.ColumnFamily{Name: "cold", Columns: []string{"gone"}}),
			wantErr: `table "t": column family "cold" names column "gone", which the table does not declare`},
		{name: "a key column", caps: capability.YDB262(), facets: declared(ydbschema.ColumnFamily{Name: "cold", Columns: []string{"id"}}),
			wantErr: `table "t": column family "cold" names key column "id", .*`},
		{name: "a keep_in_memory", caps: capability.YDB262(), facets: declared(ydbschema.ColumnFamily{Name: "default", KeepInMemory: true}),
			wantErr: `table "t": column family "default" keeps its columns in memory \(keep_in_memory\), .*`},
		{name: "a target without column families", caps: capability.YDB262().With(capability.ColumnFamilies, false),
			facets:  declared(ydbschema.ColumnFamily{Name: "cold"}),
			wantErr: `the column families of table "t", which requires target capability column_families, unavailable on this ydb target`},
		{name: "an invalid declaration", caps: capability.YDB262(), facets: declared(ydbschema.ColumnFamily{Name: "cold"}, ydbschema.ColumnFamily{Name: "cold"}),
			wantErr: `the column families of table "t": desired model "ptah.run/ydb/column-families": invalid feature value: column family "cold" is listed twice`},
		{name: "an observation", caps: capability.YDB262(),
			facets:  must.Must(schemaext.NewFacets(&ydbschema.ObservedColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold"}}})),
			wantErr: `.*column-families.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			families, err := ydbrender.CreateTableFamilies(test.caps, "t", test.facets, []string{"id", "body"}, []string{"id"})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(families.Entries, qt.IsNil)
		})
	}
}

// TestValidateTableFacets_ChecksTheDeclaredFamilies pins that a CREATE TABLE
// carrying families accepts a valid declaration and refuses an invalid one or
// an observation.
func TestValidateTableFacets_ChecksTheDeclaredFamilies(t *testing.T) {
	c := qt.New(t)
	valid := must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold"}}},
		&ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P1D"}}))
	invalid := must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold", CacheMode: "ram"}}}))
	observed := must.Must(schemaext.NewFacets(&ydbschema.ObservedColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold"}}}))

	c.Assert(ydbrender.ValidateTableFacets(valid), qt.IsNil)
	c.Assert(ydbrender.ValidateTableFacets(invalid), qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(ydbrender.ValidateTableFacets(observed), qt.ErrorIs, schemaext.ErrInvalidValue)
}
