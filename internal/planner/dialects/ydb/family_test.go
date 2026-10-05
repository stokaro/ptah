package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// familiesChanged is a change of the column families of items, beside a
// column added into a family and a column dropped out of one, with the
// declaration holding desired.
func familiesChanged(desired, current []ast.YDBColumnFamilySpec) *difftypes.SchemaDiff {
	declaration := itemsDeclaration(field("note", "TEXT", true), field("body", "TEXT", true))
	declaration.Table.YDBColumnFamilies = desired
	return modified(difftypes.TableDiff{
		TableName:               "items",
		Desired:                 declaration,
		ColumnsAdded:            difftypes.ColumnChanges{field("note", "TEXT", true)},
		ColumnsRemoved:          difftypes.ColumnChanges{field("old", "TEXT", true)},
		YDBColumnFamiliesChange: &difftypes.YDBColumnFamiliesChange{Desired: desired, Current: current},
	})
}

// TestGenerateMigrationAST_ColumnFamilies_HappyPath pins where a change of a
// table's column families sits: after the column additions, so a column added
// into a family exists when it moves there; after the TTL; and before the
// column drops, leaving a dropped column where it is. A new table's families go
// inside its CREATE TABLE. Plans of this shape applied on local-ydb 26.2.1.14
// and 25.1.4.7 and read back as declared.
func TestGenerateMigrationAST_ColumnFamilies_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "a new family holding an added column and a kept one",
			diff: familiesChanged(
				[]ast.YDBColumnFamilySpec{{Name: "cold", Compression: "lz4", Columns: []string{"body", "note"}}},
				nil,
			),
			want: "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n" +
				"ALTER TABLE `items` ADD FAMILY `cold` (COMPRESSION = 'lz4'), ALTER COLUMN `body` SET FAMILY `cold`, " +
				"ALTER COLUMN `note` SET FAMILY `cold`;\n" +
				"ALTER TABLE `items` DROP COLUMN `old`;\n",
		},
		{
			name: "the default family's compression and a column moved back to it",
			diff: familiesChanged(
				[]ast.YDBColumnFamilySpec{{Name: "default", Compression: "lz4"}, {Name: "cold"}},
				[]ast.YDBColumnFamilySpec{{Name: "cold", Columns: []string{"body", "old"}}},
			),
			want: "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n" +
				"ALTER TABLE `items` ALTER FAMILY `default` SET COMPRESSION 'lz4', ALTER COLUMN `body` SET FAMILY `default`;\n" +
				"ALTER TABLE `items` DROP COLUMN `old`;\n",
		},
		{
			name: "settings and families the declaration leaves out",
			diff: familiesChanged(
				[]ast.YDBColumnFamilySpec{{Name: "cold", Columns: []string{"body"}}},
				[]ast.YDBColumnFamilySpec{
					{Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"body"}},
					{Name: "default", Compression: "lz4", KeepInMemory: true},
					{Name: "extra", Compression: "lz4"},
				},
			),
			want: "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n" +
				"ALTER TABLE `items` DROP COLUMN `old`;\n",
		},
		{
			name: "a column out of a family the declaration leaves out",
			diff: familiesChanged(nil, []ast.YDBColumnFamilySpec{{Name: "cold", Data: "hdd", Columns: []string{"body"}}}),
			want: "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n" +
				"ALTER TABLE `items` ALTER COLUMN `body` SET FAMILY `default`;\n" +
				"ALTER TABLE `items` DROP COLUMN `old`;\n",
		},
		{
			name: "only a dropped column's family differs",
			diff: familiesChanged(
				[]ast.YDBColumnFamilySpec{{Name: "cold"}},
				[]ast.YDBColumnFamilySpec{{Name: "cold", Columns: []string{"old"}}},
			),
			want: "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n" +
				"ALTER TABLE `items` DROP COLUMN `old`;\n",
		},
		{
			name: "after the TTL",
			diff: func() *difftypes.SchemaDiff {
				diff := familiesChanged([]ast.YDBColumnFamilySpec{{Name: "cold", Columns: []string{"note"}}}, nil)
				diff.TablesModified[0].Desired.Fields = append(diff.TablesModified[0].Desired.Fields, field("ts", "TIMESTAMP", true))
				diff.TablesModified[0].RowDeletionPolicyChange = &difftypes.RowDeletionPolicyChange{
					Desired: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P1D"},
				}
				return diff
			}(),
			want: "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n" +
				"ALTER TABLE `items` SET (TTL = Interval(\"P1D\") ON `ts`);\n" +
				"ALTER TABLE `items` ADD FAMILY `cold` (), ALTER COLUMN `note` SET FAMILY `cold`;\n" +
				"ALTER TABLE `items` DROP COLUMN `old`;\n",
		},
		{
			name: "a new table",
			diff: &difftypes.SchemaDiff{TablesAdded: []difftypes.TableCreation{{
				Name: "events",
				Table: schemamodel.Table{StructName: "E", Name: "events",
					YDBColumnFamilies: []ast.YDBColumnFamilySpec{{Name: "cold", Data: "hdd", Columns: []string{"body"}}}},
				Fields: []schemamodel.Field{
					{StructName: "E", Name: "id", Type: "BIGINT UNSIGNED", Primary: true},
					{StructName: "E", Name: "body", Type: "TEXT", Nullable: true},
				},
			}}},
			want: "CREATE TABLE `events` (\n" +
				"    `id` Uint64 NOT NULL,\n" +
				"    `body` Utf8 FAMILY `cold`,\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    FAMILY `cold` (DATA = 'hdd')\n" +
				");\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(render(c, capability.YDB251(), test.diff), qt.Equals, test.want)
		})
	}
}

// TestGenerateMigrationAST_ColumnFamilies_FailurePath refuses, before any node,
// a change the target cannot make: by the key what it writes needs, by YDB's
// reason for a declaration it refuses, and by the reason no statement writes
// keep_in_memory.
func TestGenerateMigrationAST_ColumnFamilies_FailurePath(t *testing.T) {
	hot := ast.YDBColumnFamilySpec{Name: "hot", CacheMode: "in_memory"}
	tests := []struct {
		name        string
		caps        capability.Capabilities
		diff        *difftypes.SchemaDiff
		wantFeature string
		wantErr     string
	}{
		{
			name:        "families without the key",
			caps:        capability.YDB262().With(capability.ColumnFamilies, false),
			diff:        familiesChanged([]ast.YDBColumnFamilySpec{{Name: "cold"}}, nil),
			wantFeature: "column_families",
			wantErr:     `changing the column families of table "items", which requires target capability column_families, .*`,
		},
		{
			name:        "a cache mode on a line without it",
			caps:        capability.YDB253(),
			diff:        familiesChanged([]ast.YDBColumnFamilySpec{hot}, nil),
			wantFeature: "column_family_cache_mode",
			wantErr:     `changing the column family cache mode of table "items", which requires target capability column_family_cache_mode, .*`,
		},
		{
			name:        "a key column in a family",
			caps:        capability.YDB262(),
			diff:        familiesChanged([]ast.YDBColumnFamilySpec{{Name: "cold", Columns: []string{"id"}}}, nil),
			wantFeature: `table "items"`,
			wantErr:     `table "items": column family "cold" names key column "id", .*`,
		},
		{
			name: "keep_in_memory stated for a family without it",
			caps: capability.YDB262(),
			diff: familiesChanged([]ast.YDBColumnFamilySpec{{Name: "default", KeepInMemory: true}},
				[]ast.YDBColumnFamilySpec{{Name: "default", Compression: "off"}}),
			wantFeature: `table "items"`,
			wantErr: `table "items": column family "default" keeps its columns in memory \(keep_in_memory\) on one side ` +
				`only, and YQL has no family setting for it .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(test.diff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			var refusal *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &refusal)
			c.Assert(refusal.Feature, qt.Equals, test.wantFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestGenerateMigrationAST_ColumnFamilies_NeedNoRebuild changes families in
// place even when a rebuild is allowed: a family and a storage pool the
// declaration leaves out stay, since a table profile can give them to every
// new table and a rebuild would only get them back.
func TestGenerateMigrationAST_ColumnFamilies_NeedNoRebuild(t *testing.T) {
	tests := []struct {
		name    string
		desired []ast.YDBColumnFamilySpec
		current []ast.YDBColumnFamilySpec
		want    string
	}{
		{
			name:    "a family left out",
			desired: []ast.YDBColumnFamilySpec{{Name: "warm", Columns: []string{"label"}}},
			current: []ast.YDBColumnFamilySpec{{Name: "cold", Columns: []string{"label"}}},
			want:    "ALTER TABLE `app/items` ADD FAMILY `warm` (), ALTER COLUMN `label` SET FAMILY `warm`;\n",
		},
		{
			name:    "a storage pool left out",
			desired: []ast.YDBColumnFamilySpec{{Name: "default", Compression: "lz4"}},
			current: []ast.YDBColumnFamilySpec{{Name: "default", Data: "hdd", Compression: "off"}},
			want:    "ALTER TABLE `app/items` ALTER FAMILY `default` SET COMPRESSION 'lz4';\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declaration := appItems(field("label", "TEXT", true))
			declaration.Table.YDBColumnFamilies = test.desired
			got := renderRebuild(c, capability.YDB262(), modified(difftypes.TableDiff{
				TableName: "app.items", Desired: declaration,
				YDBColumnFamiliesChange: &difftypes.YDBColumnFamiliesChange{Desired: test.desired, Current: test.current},
			}))
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestGenerateMigrationAST_ColumnFamilies_RebuildCarriesThem writes a rebuilt
// table's families into its new CREATE TABLE, so the families survive the
// swap. The comparison gives the declaration every family and setting the
// old table holds and the declaration does not state, here the default
// family's compression and a profile's family, and the new table states each.
func TestGenerateMigrationAST_ColumnFamilies_RebuildCarriesThem(t *testing.T) {
	c := qt.New(t)
	declaration := appItems(field("label", "TEXT", true), field("n", "BIGINT", true))
	declaration.Table.YDBColumnFamilies = []ast.YDBColumnFamilySpec{
		{Name: "cold", Compression: "lz4", Columns: []string{"n"}},
		{Name: "default", Compression: "lz4"},
		{Name: "extra", Compression: "off"},
	}
	got := renderRebuild(c, capability.YDB262(), modified(difftypes.TableDiff{
		TableName: "app.items", Desired: declaration,
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
	}))
	c.Assert(got, qt.Contains, "    `n` Int64 FAMILY `cold`,\n")
	c.Assert(got, qt.Contains, "    INDEX `items_label` GLOBAL SYNC ON (`label`),\n"+
		"    FAMILY `cold` (COMPRESSION = 'lz4'),\n"+
		"    FAMILY `default` (COMPRESSION = 'lz4'),\n"+
		"    FAMILY `extra` (COMPRESSION = 'off')\n);\n")
}

// TestGenerateMigrationAST_ColumnFamilies_RebuildRefusesWhatItCannotRead
// refuses a rebuild of a table whose families the read could not describe,
// and rebuilds a table whose current state came from a document that cannot
// spell a family: the format's limit says nothing about the table carrying
// one.
func TestGenerateMigrationAST_ColumnFamilies_RebuildRefusesWhatItCannotRead(t *testing.T) {
	c := qt.New(t)
	change := func() *difftypes.SchemaDiff {
		return modified(difftypes.TableDiff{
			TableName: "app.items", Desired: appItems(field("label", "TEXT", true), field("n", "BIGINT", true)),
			ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
		})
	}
	read := notDescribing(change(), coverage.Object{Kind: coverage.ColumnFamily, Name: "app.items",
		Reason: coverage.Unsupported, Provenance: coverage.Observed})
	format := notDescribing(change(), coverage.Object{Kind: coverage.ColumnFamily,
		Reason: coverage.Unsupported, Provenance: coverage.DerivedFromFact})

	nodes, err := ydb.NewWithCapabilities(capability.YDB262()).WithTableRebuild(true).GenerateMigrationAST(read)
	c.Assert(err, qt.ErrorMatches, `rebuilding table "app.items": the table carries column families with settings Ptah does not read, .*`)
	c.Assert(nodes, qt.IsNil)
	c.Assert(renderRebuild(c, capability.YDB262(), format), qt.Contains, "CREATE TABLE `app/__ptah_rebuild_items` (")
}

// TestGenerateMigrationAST_ColumnFamilies_RebuildRefusesWhatItCannotWrite
// refuses, before any node, a rebuild whose new table would carry a family
// setting the target has no key for, by the key, and one whose old table
// keeps a family's columns in memory, which no CREATE TABLE writes.
func TestGenerateMigrationAST_ColumnFamilies_RebuildRefusesWhatItCannotWrite(t *testing.T) {
	tests := []struct {
		name        string
		caps        capability.Capabilities
		families    []ast.YDBColumnFamilySpec
		wantFeature string
		wantErr     string
	}{
		{
			name:        "a cache mode on a line without it",
			caps:        capability.YDB253(),
			families:    []ast.YDBColumnFamilySpec{{Name: "hot", CacheMode: "in_memory", Columns: []string{"n"}}},
			wantFeature: "column_family_cache_mode",
			wantErr: `rebuilding table "app.items" with column family cache mode, which requires target capability ` +
				`column_family_cache_mode, .*`,
		},
		{
			name:        "keep_in_memory",
			caps:        capability.YDB262(),
			families:    []ast.YDBColumnFamilySpec{{Name: "default", Compression: "lz4", KeepInMemory: true}},
			wantFeature: `rebuilding table "app.items"`,
			wantErr: `rebuilding table "app.items": column family "default" keeps its columns in memory ` +
				`\(keep_in_memory\), and YQL has no family setting for it .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declaration := appItems(field("label", "TEXT", true), field("n", "BIGINT", true))
			declaration.Table.YDBColumnFamilies = test.families
			diff := modified(difftypes.TableDiff{
				TableName: "app.items", Desired: declaration,
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
			})

			nodes, err := ydb.NewWithCapabilities(test.caps).WithTableRebuild(true).GenerateMigrationAST(diff)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			var refusal *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &refusal)
			c.Assert(refusal.Feature, qt.Equals, test.wantFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
