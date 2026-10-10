package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// familyCoverage is the changefeed coverage every fixture carries, with the
// column families known as knowledge says and subjects override.
func familyCoverage(t *testing.T, representation schemaext.Representation, knowledge schemaext.Knowledge, subjects ...schemaext.SubjectCoverage) schemaext.Coverage {
	t.Helper()
	families := must.Must(ydbschema.ColumnFamiliesCoverage(representation, knowledge, subjects))
	return must.Must(feedCoverage(t, representation).Combine(families))
}

var known = schemaext.Knowledge{State: schemaext.Complete}

// unreadFamilies is diff with the read of its one modified table recording
// column families it could not describe.
func unreadFamilies(t *testing.T, diff *difftypes.SchemaDiff) *difftypes.SchemaDiff {
	t.Helper()
	table := diff.TablesModified[0].Desired.Table
	subject := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts(table.Schema, table.Name)
	diff.TablesModified[0].Current.FeatureCoverage = familyCoverage(t, schemaext.Observed, known, schemaext.SubjectCoverage{
		Kind: ydbschema.ColumnFamiliesKind, Subject: subject,
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the table's column families hold a setting Ptah does not read"},
	})
	return diff
}

// withFamilies is declaration declaring families, with complete knowledge of
// them, as a Go annotation or YAML source records it.
func withFamilies(t *testing.T, declaration schemacapture.TableDeclaration, families ...ydbschema.ColumnFamily) schemacapture.TableDeclaration {
	t.Helper()
	declaration.Table.Facets = must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{Families: families}))
	declaration.FeatureCoverage = familyCoverage(t, schemaext.Desired, known)
	return declaration
}

// familiesChanged is a change of the column families of items, beside a
// column added into a family and a column dropped out of one: after is the
// families the table holds once the change runs, as the comparison makes them
// effective, and before what it holds, nil for none. The read found columns
// id, body and old.
func familiesChanged(t *testing.T, after, before []ydbschema.ColumnFamily) *difftypes.SchemaDiff {
	declaration := withFamilies(t, itemsDeclaration(field("note", "TEXT", true), field("body", "TEXT", true)), after...)
	change := &ydbdiff.ColumnFamilies{After: &ydbschema.DesiredColumnFamilies{Families: after}}
	if before != nil {
		change.Before = &ydbschema.ObservedColumnFamilies{Families: before}
	}
	subject := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "items")
	diff := modified(t, difftypes.TableDiff{
		TableName:      "items",
		Desired:        declaration,
		ColumnsAdded:   difftypes.ColumnChanges{field("note", "TEXT", true)},
		ColumnsRemoved: difftypes.ColumnChanges{field("old", "TEXT", true)},
		FeatureChanges: []schemaext.ChangeRecord{{Subject: subject, Value: change}},
	})
	diff.TablesModified[0].Current.Table.Columns = []catalog.Column{{Name: "id"}, {Name: "body"}, {Name: "old"}}
	return diff
}

// TestGenerateMigrationAST_ColumnFamilies_HappyPath pins where a change of a
// table's column families sits: after the column additions, so a column added
// into a family exists when it moves there, and before the column drops,
// leaving a dropped column where it is. A new table's families go inside its
// CREATE TABLE. Plans of this shape applied on local-ydb 26.2.1.14 and
// 25.1.4.7 and read back as declared.
func TestGenerateMigrationAST_ColumnFamilies_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "a new family holding an added column and a kept one",
			diff: familiesChanged(t,
				[]ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4", Columns: []string{"body", "note"}}},
				nil,
			),
			want: "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n" +
				"ALTER TABLE `items` ADD FAMILY `cold` (COMPRESSION = 'lz4'), ALTER COLUMN `body` SET FAMILY `cold`, " +
				"ALTER COLUMN `note` SET FAMILY `cold`;\n" +
				"ALTER TABLE `items` DROP COLUMN `old`;\n",
		},
		{
			name: "the default family's compression and a column moved back to it",
			diff: familiesChanged(t,
				[]ydbschema.ColumnFamily{{Name: "default", Compression: "lz4"}, {Name: "cold"}},
				[]ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body", "old"}}},
			),
			want: "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n" +
				"ALTER TABLE `items` ALTER FAMILY `default` SET COMPRESSION 'lz4', ALTER COLUMN `body` SET FAMILY `default`;\n" +
				"ALTER TABLE `items` DROP COLUMN `old`;\n",
		},
		{
			name: "settings and families the declaration leaves out",
			diff: familiesChanged(t,
				[]ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}},
				[]ydbschema.ColumnFamily{
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
			diff: familiesChanged(t,
				[]ydbschema.ColumnFamily{{Name: "cold", Data: "hdd"}},
				[]ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Columns: []string{"body"}}}),
			want: "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n" +
				"ALTER TABLE `items` ALTER COLUMN `body` SET FAMILY `default`;\n" +
				"ALTER TABLE `items` DROP COLUMN `old`;\n",
		},
		{
			name: "only a dropped column's family differs",
			diff: familiesChanged(t,
				[]ydbschema.ColumnFamily{{Name: "cold"}},
				[]ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"old"}}},
			),
			want: "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n" +
				"ALTER TABLE `items` DROP COLUMN `old`;\n",
		},
		{
			name: "beside the TTL",
			diff: func() *difftypes.SchemaDiff {
				diff := familiesChanged(t, []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"note"}}}, nil)
				diff.TablesModified[0].Desired.Fields = append(diff.TablesModified[0].Desired.Fields, field("ts", "TIMESTAMP", true))
				families := diff.TablesModified[0].Desired.Table.Facets
				diff = addTTLChange(diff, &ydbschema.TTL{Column: "ts", Interval: "P1D"}, nil)
				diff.TablesModified[0].Desired.Table.Facets = must.Must(families.Merge(diff.TablesModified[0].Desired.Table.Facets))
				return diff
			}(),
			want: "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n" +
				"ALTER TABLE `items` ADD FAMILY `cold` (), ALTER COLUMN `note` SET FAMILY `cold`;\n" +
				"ALTER TABLE `items` SET (TTL = Interval(\"P1D\") ON `ts`);\n" +
				"ALTER TABLE `items` DROP COLUMN `old`;\n",
		},
		{
			name: "a new table",
			diff: &difftypes.SchemaDiff{TablesAdded: []difftypes.TableCreation{{
				Name: "events",
				Table: schemamodel.Table{StructName: "E", Name: "events",
					Facets: must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{
						Families: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Columns: []string{"body"}}},
					}))},
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
	hot := ydbschema.ColumnFamily{Name: "hot", CacheMode: "in_memory"}
	tests := []struct {
		name    string
		caps    capability.Capabilities
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{
			name:    "families without the key",
			caps:    capability.YDB262().With(capability.ColumnFamilies, false),
			diff:    familiesChanged(t, []ydbschema.ColumnFamily{{Name: "cold"}}, nil),
			wantErr: `changing the column families of table "items", which requires target capability column_families, .*`,
		},
		{
			name:    "a cache mode on a line without it",
			caps:    capability.YDB253(),
			diff:    familiesChanged(t, []ydbschema.ColumnFamily{hot}, nil),
			wantErr: `changing the column family cache mode of table "items", which requires target capability column_family_cache_mode, .*`,
		},
		{
			name:    "a key column in a family",
			caps:    capability.YDB262(),
			diff:    familiesChanged(t, []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"id"}}}, nil),
			wantErr: `table "items": column family "cold" names key column "id", .*`,
		},
		{
			name: "keep_in_memory stated for a family without it",
			caps: capability.YDB262(),
			diff: familiesChanged(t, []ydbschema.ColumnFamily{{Name: "default", KeepInMemory: true}},
				[]ydbschema.ColumnFamily{{Name: "default", Compression: "off"}}),
			wantErr: `table "items": column family "default" keeps its columns in memory \(keep_in_memory\) on one side ` +
				`only, and YQL has no family setting for it .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				test.diff,
			)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
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
		name          string
		after, before []ydbschema.ColumnFamily
		want          string
	}{
		{
			name:   "a family left out",
			after:  []ydbschema.ColumnFamily{{Name: "warm", Columns: []string{"label"}}, {Name: "cold"}},
			before: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"label"}}},
			want:   "ALTER TABLE `app/items` ADD FAMILY `warm` (), ALTER COLUMN `label` SET FAMILY `warm`;\n",
		},
		{
			name:   "a storage pool left out",
			after:  []ydbschema.ColumnFamily{{Name: "default", Data: "hdd", Compression: "lz4"}},
			before: []ydbschema.ColumnFamily{{Name: "default", Data: "hdd", Compression: "off"}},
			want:   "ALTER TABLE `app/items` ALTER FAMILY `default` SET COMPRESSION 'lz4';\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declaration := withFamilies(t, appItems(field("label", "TEXT", true)), test.after...)
			subject := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("app", "items")
			change := &ydbdiff.ColumnFamilies{After: &ydbschema.DesiredColumnFamilies{Families: test.after},
				Before: &ydbschema.ObservedColumnFamilies{Families: test.before}}
			got := renderRebuild(c, capability.YDB262(), modified(t, difftypes.TableDiff{
				TableName: "app.items", Desired: declaration,
				FeatureChanges: []schemaext.ChangeRecord{{Subject: subject, Value: change}},
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
	declaration := withFamilies(t, appItems(field("label", "TEXT", true), field("n", "BIGINT", true)),
		ydbschema.ColumnFamily{Name: "cold", Compression: "lz4", Columns: []string{"n"}},
		ydbschema.ColumnFamily{Name: "default", Compression: "lz4"},
		ydbschema.ColumnFamily{Name: "extra", Compression: "off"},
	)
	got := renderRebuild(c, capability.YDB262(), modified(t, difftypes.TableDiff{
		TableName: "app.items", Desired: declaration,
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
	}))
	c.Assert(got, qt.Contains, "    `n` Int64 FAMILY `cold`,\n")
	c.Assert(got, qt.Contains, "    INDEX `items_label` GLOBAL SYNC ON (`label`),\n"+
		"    FAMILY `cold` (COMPRESSION = 'lz4'),\n"+
		"    FAMILY `default` (COMPRESSION = 'lz4'),\n"+
		"    FAMILY `extra` (COMPRESSION = 'off')\n"+heldDefaults)
}

// A rebuild driven by a document that cannot spell a family, HCL or DBML,
// writes the families the comparison adopted from the old table, without a
// column the document leaves out: the plan drops that column, and the new
// table cannot name it. A declaration that states its families keeps that
// refusal.
func TestGenerateMigrationAST_ColumnFamilies_RebuildLeavesADroppedColumnOut(t *testing.T) {
	c := qt.New(t)
	adopted := appItems(field("label", "TEXT", true), field("n", "BIGINT", true))
	adopted.Table.Facets = must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{
		Families: []ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4", Columns: []string{"gone", "n"}}},
	}))
	change := func(declaration schemacapture.TableDeclaration) *difftypes.SchemaDiff {
		return modified(t, difftypes.TableDiff{
			TableName: "app.items", Desired: declaration,
			ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
		})
	}

	got := renderRebuild(c, capability.YDB262(), change(adopted))
	stated := withFamilies(t, adopted, ydbschema.ColumnFamily{Name: "cold", Compression: "lz4", Columns: []string{"gone", "n"}})
	_, err := ydb.NewWithCapabilities(capability.YDB262()).WithTableRebuild(true).GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()), change(stated))

	c.Assert(got, qt.Contains, "    `n` Int64 FAMILY `cold`,\n")
	c.Assert(got, qt.Contains, "    FAMILY `cold` (COMPRESSION = 'lz4')\n")
	c.Assert(got, qt.Not(qt.Contains), "gone")
	c.Assert(err, qt.ErrorMatches, `.*column family "cold" names column "gone", which the table does not declare.*`)
}

// TestGenerateMigrationAST_ColumnFamilies_RebuildRefusesWhatItCannotRead
// refuses a rebuild of a table whose families the read could not describe,
// and rebuilds a table whose current state came from a document that cannot
// spell a family: the format's silence says nothing about the table carrying
// one.
func TestGenerateMigrationAST_ColumnFamilies_RebuildRefusesWhatItCannotRead(t *testing.T) {
	c := qt.New(t)
	change := func() *difftypes.SchemaDiff {
		return modified(t, difftypes.TableDiff{
			TableName: "app.items", Desired: appItems(field("label", "TEXT", true), field("n", "BIGINT", true)),
			ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
		})
	}
	read := unreadFamilies(t, change())
	format := change()

	nodes, err := ydb.NewWithCapabilities(capability.YDB262()).WithTableRebuild(true).GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		read,
	)
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
		name     string
		caps     capability.Capabilities
		families []ydbschema.ColumnFamily
		wantErr  string
	}{
		{
			name:     "a cache mode on a line without it",
			caps:     capability.YDB253(),
			families: []ydbschema.ColumnFamily{{Name: "hot", CacheMode: "in_memory", Columns: []string{"n"}}},
			wantErr: `rebuilding table "app/items" with column family cache mode, which requires target capability ` +
				`column_family_cache_mode, .*`,
		},
		{
			name:     "keep_in_memory",
			caps:     capability.YDB262(),
			families: []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4", KeepInMemory: true}},
			wantErr: `rebuilding table "app/items": column family "default" keeps its columns in memory ` +
				`\(keep_in_memory\), and YQL has no family setting for it .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declaration := withFamilies(t, appItems(field("label", "TEXT", true), field("n", "BIGINT", true)), test.families...)
			diff := modified(t, difftypes.TableDiff{
				TableName: "app.items", Desired: declaration,
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
			})

			nodes, err := ydb.NewWithCapabilities(test.caps).WithTableRebuild(true).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				diff,
			)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
