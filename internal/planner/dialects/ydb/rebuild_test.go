package ydb_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// renderRebuild plans diff with rebuilds allowed and renders it.
func renderRebuild(c *qt.C, caps capability.Capabilities, diff *difftypes.SchemaDiff) string {
	c.Helper()
	nodes, err := ydb.NewWithCapabilities(caps).WithTableRebuild(true).GenerateMigrationAST(diff)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQLWithCapabilities("ydb", caps, nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// rebuildNote is the comment every rebuild of app/items opens with.
const rebuildNote = "-- Rebuild of table app/items: YDB cannot make this change in place, so the table is created " +
	"anew, the rows are copied, and the two tables are swapped.\n" +
	"-- The steps are not atomic. Rows written to app/items between the copy and the swap are lost, and YDB has " +
	"no lock to stop them: stop writing to the table until the last step has run.\n" +
	"-- The copy is one query. YDB refuses one that carries more than about 48 MiB on 25.1 or 64 MiB on 26.2; " +
	"nothing is then copied, and the old table keeps serving.\n"

// appItems is the desired table app/items: a key, a column and an index.
func appItems(fields ...schemamodel.Field) difftypes.TableDeclaration {
	return difftypes.TableDeclaration{
		Table:   schemamodel.Table{StructName: "S", Name: "items", Schema: "app"},
		Fields:  append([]schemamodel.Field{keyField("id")}, fields...),
		Indexes: []schemamodel.Index{{StructName: "S", Name: "items_label", Fields: []string{"label"}}},
	}
}

// TestGenerateMigrationAST_TableRebuild_HappyPath pins the rebuild a change YDB
// cannot make in place becomes when it is asked for: the note, the new table
// written from the declaration with its indexes inside it, the copy, the two
// renames, and the drop of the old table. The copy converts what changed:
// a new type through an unwrapped CAST, which fails the copy on a value that
// does not convert, and a column made NOT NULL through Unwrap, which fails it
// on a NULL row.
func TestGenerateMigrationAST_TableRebuild_HappyPath(t *testing.T) {
	label := field("label", "TEXT", true)
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "a type change of a nullable column",
			diff: modified(difftypes.TableDiff{
				TableName: "app.items", Desired: appItems(label, field("n", "BIGINT", true)),
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
			}),
			want: "INSERT INTO `app/__ptah_rebuild_items` (`id`, `label`, `n`) SELECT Unwrap(`id`, " +
				"'rebuilding table app.items: column id holds NULL, and the new table declares it NOT NULL'u) AS `id`, " +
				"`label` AS `label`, IF(`n` IS NULL, NULL, Unwrap(CAST(`n` AS Int64), " +
				"'rebuilding table app.items: column n holds a value that does not convert to its new type'u)) AS `n` " +
				"FROM `app/items`;\n",
		},
		{
			name: "a type change of a NOT NULL column",
			diff: modified(difftypes.TableDiff{
				TableName: "app.items", Desired: appItems(label, field("n", "BIGINT", false)),
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
			}),
			want: "INSERT INTO `app/__ptah_rebuild_items` (`id`, `label`, `n`) SELECT Unwrap(`id`, " +
				"'rebuilding table app.items: column id holds NULL, and the new table declares it NOT NULL'u) AS `id`, " +
				"`label` AS `label`, Unwrap(CAST(`n` AS Int64), " +
				"'rebuilding table app.items: column n holds NULL or a value that does not convert to its new type'u) AS `n` " +
				"FROM `app/items`;\n",
		},
		{
			name: "a column made NOT NULL, beside a column added with a default",
			diff: modified(difftypes.TableDiff{
				TableName: "app.items",
				Desired: appItems(field("label", "TEXT", false),
					schemamodel.Field{StructName: "S", Name: "qty", Type: "INTEGER", Default: "1", DefaultSet: true}),
				ColumnsAdded: difftypes.ColumnChanges{
					{StructName: "S", Name: "qty", Type: "INTEGER", Default: "1", DefaultSet: true},
				},
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "label", Changes: map[string]string{"nullable": "true -> false"}}},
			}),
			want: "INSERT INTO `app/__ptah_rebuild_items` (`id`, `label`) SELECT Unwrap(`id`, " +
				"'rebuilding table app.items: column id holds NULL, and the new table declares it NOT NULL'u) AS `id`, " +
				"Unwrap(`label`, 'rebuilding table app.items: column label holds NULL, and the new table declares it " +
				"NOT NULL'u) AS `label` FROM `app/items`;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := renderRebuild(c, capability.YDB262(), test.diff)

			c.Assert(got, qt.Contains, rebuildNote)
			c.Assert(got, qt.Contains, "CREATE TABLE `app/__ptah_rebuild_items` (")
			c.Assert(got, qt.Contains, "    INDEX `items_label` GLOBAL SYNC ON (`label`)\n);\n")
			c.Assert(got, qt.Contains, test.want+
				"ALTER TABLE `app/items` RENAME TO `app/__ptah_replaced_items`;\n"+
				"ALTER TABLE `app/__ptah_rebuild_items` RENAME TO `app/items`;\n"+
				"DROP TABLE `app/__ptah_replaced_items`;\n")
		})
	}
}

// A key change arrives as a change to the key constraint and no modification
// of the table at all, so the rebuild writes the table from the declaration
// the diff carries for the tables its constraint changes name. The new key is
// in the new table, and the key change itself plans no statement.
func TestGenerateMigrationAST_TableRebuild_KeyChange(t *testing.T) {
	c := qt.New(t)
	declaration := difftypes.TableDeclaration{
		Table: schemamodel.Table{StructName: "S", Name: "items", Schema: "app", PrimaryKey: []string{"id", "k"}},
		Fields: []schemamodel.Field{
			{StructName: "S", Name: "id", Type: "BIGINT"},
			{StructName: "S", Name: "k", Type: "BIGINT"},
		},
	}
	diff := &difftypes.SchemaDiff{
		ConstraintsAdded:        difftypes.ConstraintAdditions{{Name: "items_pkey", TableName: "app.items", Type: "PRIMARY KEY"}},
		ConstraintsRemoved:      difftypes.ConstraintRemovals{{Name: "items_pkey", TableName: "app.items", Type: "PRIMARY KEY"}},
		DeclaredConstraintHosts: []difftypes.TableDeclaration{declaration},
	}

	got := renderRebuild(c, capability.YDB251(), diff)

	c.Assert(got, qt.Equals, rebuildNote+
		"CREATE TABLE `app/__ptah_rebuild_items` (\n"+
		"    `id` Int64 NOT NULL,\n"+
		"    `k` Int64 NOT NULL,\n"+
		"    PRIMARY KEY (`id`, `k`)\n"+
		");\n"+
		"INSERT INTO `app/__ptah_rebuild_items` (`id`, `k`) SELECT "+
		"Unwrap(`id`, 'rebuilding table app.items: column id holds NULL, and the new table declares it NOT NULL'u) AS `id`, "+
		"Unwrap(`k`, 'rebuilding table app.items: column k holds NULL, and the new table declares it NOT NULL'u) AS `k` "+
		"FROM `app/items`;\n"+
		"ALTER TABLE `app/items` RENAME TO `app/__ptah_replaced_items`;\n"+
		"ALTER TABLE `app/__ptah_rebuild_items` RENAME TO `app/items`;\n"+
		"DROP TABLE `app/__ptah_replaced_items`;\n")
}

// A scratch name the declaration holds, or the plan drops, is passed over for
// the next free one.
func TestGenerateMigrationAST_TableRebuild_PicksAFreeScratchName(t *testing.T) {
	c := qt.New(t)
	diff := modified(difftypes.TableDiff{
		TableName: "app.items", Desired: appItems(field("label", "TEXT", true), field("n", "BIGINT", true)),
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
	})
	diff.DeclaredTables = []schemamodel.Table{{Name: "__ptah_rebuild_items", Schema: "app"}}
	diff.TablesRemoved = []string{"app.__ptah_replaced_items"}

	got := renderRebuild(c, capability.YDB262(), diff)

	c.Assert(got, qt.Contains, "ALTER TABLE `app/items` RENAME TO `app/__ptah_replaced_items_1`;\n"+
		"ALTER TABLE `app/__ptah_rebuild_items_1` RENAME TO `app/items`;\n"+
		"DROP TABLE `app/__ptah_replaced_items_1`;\n")
}

// Without the request, each change a rebuild could make stays refused by its
// capability key, and the refusal names the flag that asks for the rebuild.
func TestGenerateMigrationAST_TableRebuild_NotAskedFor(t *testing.T) {
	tests := []struct {
		name    string
		diff    *difftypes.SchemaDiff
		wantKey capability.Capability
		wantErr string
	}{
		{name: "a type change",
			diff: modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(),
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}}}),
			wantKey: capability.AlterColumnType,
			wantErr: `changing the type of column "n" of table "items" \(Int32 -> Int64\), which requires target capability ` +
				`alter_column_type, unavailable on this ydb target; YDB makes it by rebuilding the table, which Ptah ` +
				`plans when asked with --allow-table-rebuild`},
		{name: "a column made NOT NULL",
			diff: modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(),
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"nullable": "true -> false"}}}}),
			wantKey: capability.AlterColumnSetNotNull,
			wantErr: `making column "n" of table "items" NOT NULL, which requires target capability alter_column_set_not_null, ` +
				`unavailable on this ydb target; YDB makes it by rebuilding the table, which Ptah plans when asked with ` +
				`--allow-table-rebuild`},
		{name: "a key change",
			diff:    &difftypes.SchemaDiff{ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "pk", TableName: "items", Type: "PRIMARY KEY"}}},
			wantKey: capability.PrimaryKeyAlterable,
			wantErr: `dropping constraint pk, the primary key, which requires target capability primary_key_alterable, ` +
				`unavailable on this ydb target; YDB makes it by rebuilding the table, which Ptah plans when asked with ` +
				`--allow-table-rebuild`},
		{name: "a column added to the key",
			diff: modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(keyField("k")),
				ColumnsAdded: difftypes.ColumnChanges{keyField("k")}}),
			wantKey: capability.PrimaryKeyAlterable,
			wantErr: `adding column "k" to table "items" as part of the key, which requires target capability ` +
				`primary_key_alterable, .*; YDB makes it by rebuilding the table, which Ptah plans when asked with ` +
				`--allow-table-rebuild`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(test.diff)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			refusal, ok := err.(*ptaherr.CapabilityError)
			c.Assert(ok, qt.IsTrue)
			c.Assert(refusal.Feature, qt.Equals, string(test.wantKey))
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// A caller that reads the request from somewhere other than the native flag
// names its own spelling, and the refusal names that; with none it names the
// flag. One predicate decides which changes a rebuild makes, so only the words
// differ.
func TestGenerateMigrationAST_TableRebuild_NamesTheCallersRequest(t *testing.T) {
	tests := []struct {
		name    string
		request string
		want    string
	}{
		{name: "the native flag", request: "", want: "--allow-table-rebuild"},
		{name: "a variable", request: "PTAH_ALLOW_TABLE_REBUILD=1", want: "PTAH_ALLOW_TABLE_REBUILD=1"},
	}
	diff := modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(),
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}}})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := ydb.NewWithCapabilities(capability.YDB262()).WithTableRebuildRequest(test.request).
				GenerateMigrationAST(diff)

			c.Assert(err, qt.ErrorMatches, `changing the type of column "n" of table "items" \(Int32 -> Int64\), .*`+
				`; YDB makes it by rebuilding the table, which Ptah plans when asked with `+regexp.QuoteMeta(test.want))
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// Even when asked for, a rebuild that would damage the table is refused
// before anything is planned, and the refusal says why.
func TestGenerateMigrationAST_TableRebuild_FailurePath(t *testing.T) {
	typeChange := []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}}
	serialKey := schemamodel.Field{StructName: "S", Name: "id", Type: "SERIAL", Primary: true}
	tests := []struct {
		name    string
		caps    capability.Capabilities
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{
			name: "a Serial column",
			caps: capability.YDB262(),
			diff: modified(difftypes.TableDiff{TableName: "app.items",
				Desired: difftypes.TableDeclaration{
					Table:  schemamodel.Table{StructName: "S", Name: "items", Schema: "app"},
					Fields: []schemamodel.Field{serialKey, field("n", "BIGINT", true)},
				},
				ColumnsModified: typeChange}),
			wantErr: `rebuilding table "app.items": column "id" takes its values from a sequence\. The new table's ` +
				`sequence would start at 1 while the copied rows keep theirs, so the next insert would collide .*`,
		},
		{
			name: "a TTL run interval and a changefeed the read did not describe",
			caps: capability.YDB262(),
			diff: notDescribing(modified(difftypes.TableDiff{TableName: "app.items",
				Desired: appItems(field("label", "TEXT", true), field("n", "BIGINT", true)), ColumnsModified: typeChange}),
				coverage.Object{Kind: coverage.TTL, Name: "app.items"},
				coverage.Object{Kind: coverage.Changefeed, Name: "app.items/updates"},
				coverage.Object{Kind: coverage.TTL, Name: "app.other"}),
			wantErr: `rebuilding table "app.items": the table carries a TTL run interval or tiering policy, ` +
				`changefeeds with settings Ptah does not read, which Ptah does not model and ` +
				`so cannot write on the new table: recreating it would drop them\. .*`,
		},
		{
			name: "column families and table options the read did not describe",
			caps: capability.YDB262(),
			diff: notDescribing(modified(difftypes.TableDiff{TableName: "app.items",
				Desired: appItems(field("label", "TEXT", true), field("n", "BIGINT", true)), ColumnsModified: typeChange}),
				coverage.Object{Kind: coverage.ColumnFamily, Name: "app.items"},
				coverage.Object{Kind: coverage.TableOption}),
			wantErr: `rebuilding table "app.items": the table carries column families, partitioning, read replica or ` +
				`key bloom filter options, which Ptah does not model .*`,
		},
		{
			name: "a target that cannot rename a table",
			caps: capability.YDB262().With(capability.RenameTable, false),
			diff: modified(difftypes.TableDiff{TableName: "app.items",
				Desired: appItems(field("label", "TEXT", true), field("n", "BIGINT", true)), ColumnsModified: typeChange}),
			wantErr: `rebuilding table "app.items", which swaps the new table into place by renaming it, which requires ` +
				`target capability rename_table, .*`,
		},
		{
			name: "a NOT NULL column added without a default",
			caps: capability.YDB262(),
			diff: modified(difftypes.TableDiff{TableName: "app.items",
				Desired:      appItems(field("label", "TEXT", true), field("n", "BIGINT", true), field("m", "TEXT", false)),
				ColumnsAdded: difftypes.ColumnChanges{field("m", "TEXT", false)}, ColumnsModified: typeChange}),
			wantErr: `adding column "m" to table "app.items" in a rebuild: the copy has no value for a NOT NULL column ` +
				`without a default: give it a default or make it nullable`,
		},
		{
			name: "a key change with no declaration to write the table from",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{ConstraintsRemoved: difftypes.ConstraintRemovals{
				{Name: "pk", TableName: "app.items", Type: "PRIMARY KEY"}}},
			wantErr: `rebuilding table "app.items": the plan carries no declaration of the table to write the new one from`,
		},
		{
			name: "a comment on the rebuilt table",
			caps: capability.YDB262(),
			diff: modified(difftypes.TableDiff{TableName: "app.items",
				Desired:       appItems(field("label", "TEXT", true), field("n", "BIGINT", true)),
				CommentChange: &difftypes.CommentChange{Desired: "items"}, ColumnsModified: typeChange}),
			wantErr: `the comment on table "app.items": .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := ydb.NewWithCapabilities(test.caps).WithTableRebuild(true).GenerateMigrationAST(test.diff)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// notDescribing records objects as what the read of the database did not
// describe.
func notDescribing(diff *difftypes.SchemaDiff, objects ...coverage.Object) *difftypes.SchemaDiff {
	diff.CurrentNotDescribed = coverage.Set{}.With(objects...)
	return diff
}

// A change the target makes in place is not rebuilt because rebuilds are
// allowed: the plan stays the in-place statement.
func TestGenerateMigrationAST_TableRebuild_OnlyWhatNeedsIt(t *testing.T) {
	c := qt.New(t)
	diff := modified(difftypes.TableDiff{
		TableName: "items", Desired: itemsDeclaration(field("label", "TEXT", true)),
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: "label", Changes: map[string]string{"nullable": "false -> true"}, Desired: field("label", "TEXT", true),
		}},
	})

	c.Assert(renderRebuild(c, capability.YDB262(), diff), qt.Equals,
		"ALTER TABLE `items` ALTER COLUMN `label` DROP NOT NULL;\n")
}

// A key column is written NOT NULL however the declaration marks it, so the
// copy unwraps it even where the declaration calls it nullable.
func TestGenerateMigrationAST_TableRebuild_UnwrapsAKeyDeclaredNullable(t *testing.T) {
	c := qt.New(t)
	declaration := appItems(field("label", "TEXT", true), field("n", "BIGINT", true))
	declaration.Fields[0].Nullable = true
	diff := modified(difftypes.TableDiff{
		TableName: "app.items", Desired: declaration,
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
	})

	c.Assert(renderRebuild(c, capability.YDB262(), diff), qt.Contains, "SELECT Unwrap(`id`, "+
		"'rebuilding table app.items: column id holds NULL, and the new table declares it NOT NULL'u) AS `id`, ")
}

// A rebuilt table's index changes travel with the new table, which carries
// every declared index inside its CREATE TABLE: no index of it is dropped or
// added on its own, where the old table is gone or the index exists already.
func TestGenerateMigrationAST_TableRebuild_CarriesTheIndexChanges(t *testing.T) {
	c := qt.New(t)
	diff := modified(difftypes.TableDiff{
		TableName: "app.items", Desired: appItems(field("label", "TEXT", true), field("n", "BIGINT", true)),
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
	})
	diff.IndexesAdded = difftypes.IndexChanges{{TableName: "app.items",
		Index: schemamodel.Index{Name: "items_label", Fields: []string{"label"}, Unique: true}}}
	diff.IndexesRemoved = []difftypes.IndexRef{{Name: "items_old", TableName: "app.items"}}

	got := renderRebuild(c, capability.YDB262(), diff)

	c.Assert(got, qt.Not(qt.Contains), "DROP INDEX")
	c.Assert(got, qt.Not(qt.Contains), "ADD INDEX")
	c.Assert(got, qt.Contains, "    INDEX `items_label` GLOBAL SYNC ON (`label`)\n);\n")
}
