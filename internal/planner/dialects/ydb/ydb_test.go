package ydb_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// field builds a column of struct S.
func field(name, columnType string, nullable bool) schemamodel.Field {
	return schemamodel.Field{StructName: "S", Name: name, Type: columnType, Nullable: nullable}
}

// keyField builds the key column of struct S.
func keyField(name string) schemamodel.Field {
	return schemamodel.Field{StructName: "S", Name: name, Type: "BIGINT", Primary: true}
}

// itemsDeclaration is the desired table every modification below names.
func itemsDeclaration(fields ...schemamodel.Field) difftypes.TableDeclaration {
	return difftypes.TableDeclaration{
		Table:  schemamodel.Table{StructName: "S", Name: "items"},
		Fields: append([]schemamodel.Field{keyField("id")}, fields...),
	}
}

// render plans diff for caps and renders the plan with the same caps, one
// statement per line, the way an apply would see it.
func render(c *qt.C, caps capability.Capabilities, diff *difftypes.SchemaDiff) string {
	c.Helper()
	nodes, err := ydb.NewWithCapabilities(caps).GenerateMigrationAST(diff)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQLWithCapabilities("ydb", caps, nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// TestGenerateMigrationAST_Order_HappyPath pins the order a YDB plan takes,
// which is the order the server needs: a new table with its indexes inside
// it; every index drop before any column drop, because YDB refuses to drop an
// indexed or covered column (`Impossible drop column because table has an
// index with that column` and `... because table index covers that column` on
// local-ydb 26.2.1.14 and 25.1.4.7); the column additions before the index
// that names them, each in a statement of its own; and the dropped table
// last. A plan of this shape, with rows in the tables, applied on both lines
// one statement per query and read back as declared.
func TestGenerateMigrationAST_Order_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{
			Name:   "tags",
			Table:  schemamodel.Table{StructName: "T", Name: "tags"},
			Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}, {StructName: "T", Name: "label", Type: "TEXT", Nullable: true}},
		}},
		TablesRemoved: []string{"legacy"},
		TablesModified: []difftypes.TableDiff{{
			TableName:      "items",
			Desired:        itemsDeclaration(field("note", "TEXT", true), field("qty", "INTEGER", true)),
			ColumnsAdded:   difftypes.ColumnChanges{field("note", "TEXT", true)},
			ColumnsRemoved: difftypes.ColumnChanges{{Name: "old_code"}},
		}},
		IndexesAdded: difftypes.IndexChanges{
			{TableName: "tags", Index: schemamodel.Index{Name: "tags_label_uq", Fields: []string{"label"}, Unique: true}},
			{TableName: "items", Index: schemamodel.Index{Name: "items_note", Fields: []string{"note"}, Type: "async"}},
		},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "items_old_code", TableName: "items"},
			{Name: "legacy_ix", TableName: "legacy"},
		},
	}

	got := render(c, capability.YDB251(), diff)

	c.Assert(got, qt.Equals, "CREATE TABLE `tags` (\n"+
		"    `id` Int64 NOT NULL,\n"+
		"    `label` Utf8,\n"+
		"    PRIMARY KEY (`id`),\n"+
		"    INDEX `tags_label_uq` GLOBAL UNIQUE SYNC ON (`label`)\n"+
		");\n"+
		"ALTER TABLE `items` DROP INDEX `items_old_code`;\n"+
		"ALTER TABLE `items` ADD COLUMN `note` Utf8;\n"+
		"ALTER TABLE `items` DROP COLUMN `old_code`;\n"+
		"ALTER TABLE `items` ADD INDEX `items_note` GLOBAL ASYNC ON (`note`);\n"+
		"DROP TABLE `legacy`;\n")
}

// tagsAdded is a plan that creates table tags with one index.
func tagsAdded(index schemamodel.Index) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{
			Name:   "tags",
			Table:  schemamodel.Table{StructName: "T", Name: "tags"},
			Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}, {StructName: "T", Name: "label", Type: "TEXT", Nullable: true}},
		}},
		IndexesAdded: difftypes.IndexChanges{{TableName: "tags", Index: index}},
	}
}

// TestGenerateMigrationAST_NewTableIndexes_HappyPath pins where a new table's
// index goes, which capability.CreateIndexStatement decides. Every YDB line
// lacks the key, so the index is written inside the CREATE TABLE; a target that
// had the statement would get the index after the table, as YDB's own ADD
// INDEX, because no CREATE INDEX spelling has been measured on YDB to write.
func TestGenerateMigrationAST_NewTableIndexes_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		want string
	}{
		{
			name: "every YDB line declares it in the table",
			caps: capability.YDB262(),
			want: "CREATE TABLE `tags` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `label` Utf8,\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    INDEX `tags_label` GLOBAL SYNC ON (`label`)\n" +
				");\n",
		},
		{
			name: "a target with CREATE INDEX adds it after the table",
			caps: capability.YDB262().With(capability.CreateIndexStatement, true),
			want: "CREATE TABLE `tags` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `label` Utf8,\n" +
				"    PRIMARY KEY (`id`)\n" +
				");\n" +
				"ALTER TABLE `tags` ADD INDEX `tags_label` GLOBAL SYNC ON (`label`);\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := render(c, test.caps, tagsAdded(schemamodel.Index{Name: "tags_label", Fields: []string{"label"}}))
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestGenerateMigrationAST_ColumnChanges_HappyPath pins the in-place changes a
// line makes: DROP NOT NULL on every line, SET and DROP DEFAULT on 26.2, and a
// column added with a default from 26.1.
func TestGenerateMigrationAST_ColumnChanges_HappyPath(t *testing.T) {
	desiredQty := field("qty", "INTEGER", true)
	desiredQty.Default, desiredQty.DefaultSet = "5", true
	tests := []struct {
		name string
		caps capability.Capabilities
		diff difftypes.TableDiff
		want string
	}{
		{
			name: "a column made nullable on 25.1",
			caps: capability.YDB251(),
			diff: difftypes.TableDiff{
				TableName: "items", Desired: itemsDeclaration(field("label", "TEXT", true)),
				ColumnsModified: []difftypes.ColumnDiff{{
					ColumnName: "label", Changes: map[string]string{"nullable": "false -> true"}, Desired: field("label", "TEXT", true),
				}},
			},
			want: "ALTER TABLE `items` ALTER COLUMN `label` DROP NOT NULL;\n",
		},
		{
			name: "a default set on 26.2",
			caps: capability.YDB262(),
			diff: difftypes.TableDiff{
				TableName: "items", Desired: itemsDeclaration(desiredQty),
				ColumnsModified: []difftypes.ColumnDiff{{
					ColumnName: "qty", Changes: map[string]string{"default": " -> 5"}, Desired: desiredQty,
				}},
			},
			want: "ALTER TABLE `items` ALTER COLUMN `qty` SET DEFAULT 5;\n",
		},
		{
			name: "a column added with a default on 26.1",
			caps: capability.YDB261(),
			diff: difftypes.TableDiff{
				TableName: "items", Desired: itemsDeclaration(desiredQty),
				ColumnsAdded: difftypes.ColumnChanges{desiredQty},
			},
			want: "ALTER TABLE `items` ADD COLUMN `qty` Int32 DEFAULT 5;\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := render(c, test.caps, &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{test.diff}})
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestGenerateMigrationAST_RefusesByCapability_FailurePath pins every change a
// YDB plan refuses up front because the target lacks a key. Nothing is
// emitted, because YDB runs DDL outside any transaction and a statement that
// ran before the refused one would stay applied.
func TestGenerateMigrationAST_RefusesByCapability_FailurePath(t *testing.T) {
	notNullTwo := field("n", "INTEGER", false)
	notNullTwo.Default, notNullTwo.DefaultSet = "2", true
	expressionDefault := field("ts", "TIMESTAMP", true)
	expressionDefault.DefaultExpr = "CURRENT_TIMESTAMP"
	tests := []struct {
		name    string
		caps    capability.Capabilities
		diff    *difftypes.SchemaDiff
		wantKey capability.Capability
		wantErr string
	}{
		{name: "a key change", caps: capability.YDB262(),
			diff: modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(),
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "id", Changes: map[string]string{"primary_key": "false -> true"}}}}),
			wantKey: capability.PrimaryKeyAlterable, wantErr: `changing whether column "id" of table "items" is part of the key \(false -> true\), which requires target capability primary_key_alterable, .*`},
		{name: "a key constraint dropped", caps: capability.YDB262(),
			diff:    &difftypes.SchemaDiff{ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "pk", TableName: "items", Type: "PRIMARY KEY"}}},
			wantKey: capability.PrimaryKeyAlterable, wantErr: `dropping constraint pk, the primary key, which requires target capability primary_key_alterable, .*`},
		{name: "a desired table with no key", caps: capability.YDB262(),
			diff: modified(difftypes.TableDiff{TableName: "items",
				Desired: difftypes.TableDeclaration{Table: schemamodel.Table{StructName: "S", Name: "items"}, Fields: []schemamodel.Field{field("a", "TEXT", true)}}}),
			wantKey: capability.PrimaryKeyRequired, wantErr: `table "items" declares no primary key, which requires target capability primary_key_required, .*`},
		{name: "a type change", caps: capability.YDB262(),
			diff: modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(),
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "int32 -> int64"}}}}),
			wantKey: capability.AlterColumnType, wantErr: `changing the type of column "n" of table "items" \(int32 -> int64\), which requires target capability alter_column_type, .*`},
		{name: "a column made NOT NULL", caps: capability.YDB262(),
			diff: modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(),
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"nullable": "true -> false"}}}}),
			wantKey: capability.AlterColumnSetNotNull, wantErr: `making column "n" of table "items" NOT NULL, which requires target capability alter_column_set_not_null, .*`},
		{name: "a default changed on 26.1", caps: capability.YDB261(),
			diff: modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(),
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"default": "1 -> 2"}}}}),
			wantKey: capability.AlterColumnDefault, wantErr: `changing the default of column "n" of table "items" \(1 -> 2\), which requires target capability alter_column_default, .*`},
		{name: "an expression default", caps: capability.YDB262(),
			diff: modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(),
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "ts", Changes: map[string]string{"default_expr": " -> CURRENT_TIMESTAMP"}, Desired: expressionDefault}}}),
			wantKey: capability.ExpressionDefaults, wantErr: `column "ts" of table "items" defaults to the expression CURRENT_TIMESTAMP, which requires target capability expression_defaults, .*`},
		{name: "a column added with a default before 26.1", caps: capability.YDB253(),
			diff:    modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(notNullTwo), ColumnsAdded: difftypes.ColumnChanges{notNullTwo}}),
			wantKey: capability.AddColumnWithDefault, wantErr: `adding column "n" to table "items" with a default, which requires target capability add_column_with_default, .*`},
		{name: "a unique index added to a table that exists", caps: capability.YDB262(),
			diff:    &difftypes.SchemaDiff{IndexesAdded: difftypes.IndexChanges{{TableName: "items", Index: schemamodel.Index{Name: "u", Fields: []string{"a"}, Unique: true}}}},
			wantKey: capability.UniqueIndexOnExistingTable, wantErr: `adding unique index "u" to table "items", which exists already, which requires target capability unique_index_on_existing_table, .*`},
		// With the index outside the CREATE TABLE, a new table's unique index
		// is an ADD INDEX on a table that exists by then.
		{name: "a unique index on a new table added after it", caps: capability.YDB262().With(capability.CreateIndexStatement, true),
			diff:    tagsAdded(schemamodel.Index{Name: "tags_label_uq", Fields: []string{"label"}, Unique: true}),
			wantKey: capability.UniqueIndexOnExistingTable, wantErr: `adding unique index "tags_label_uq" to table "tags", which exists already, which requires target capability unique_index_on_existing_table, .*`},
		{name: "a UNIQUE constraint added", caps: capability.YDB262(),
			diff:    &difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "uq", TableName: "items", Type: "UNIQUE"}}},
			wantKey: capability.UniqueConstraints, wantErr: `adding constraint uq \(declare a unique index instead\), which requires target capability unique_constraints, .*`},
		{name: "an enum", caps: capability.YDB262(),
			diff:    &difftypes.SchemaDiff{EnumsAdded: difftypes.EnumChanges{{Name: "mood"}}},
			wantKey: capability.EnumCustomType, wantErr: `the plan changes an enum type, which requires target capability enum_custom_type, .*`},
		{name: "a sequence", caps: capability.YDB262(),
			diff:    &difftypes.SchemaDiff{SequencesAdded: difftypes.SequenceChanges{{Name: "s"}}},
			wantKey: capability.Sequences, wantErr: `the plan changes a sequence, which requires target capability sequences, .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(test.diff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			var capabilityErr *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &capabilityErr)
			c.Assert(capabilityErr.Feature, qt.Equals, string(test.wantKey))
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestGenerateMigrationAST_RefusesWhatYDBCannotDo_FailurePath pins the
// refusals no key decides, and the families a later phase implements.
func TestGenerateMigrationAST_RefusesWhatYDBCannotDo_FailurePath(t *testing.T) {
	serial := field("s", "SERIAL", false)
	tests := []struct {
		name    string
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{name: "a NOT NULL column added without a default",
			diff:    modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(field("n", "INTEGER", false)), ColumnsAdded: difftypes.ColumnChanges{field("n", "INTEGER", false)}}),
			wantErr: `adding column "n" to table "items": YDB adds a NOT NULL column only with a default .*`},
		{name: "a serial added to a table that exists",
			diff:    modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(serial), ColumnsAdded: difftypes.ColumnChanges{serial}}),
			wantErr: `adding column "s" to table "items": YDB adds no Serial column to an existing table .*`},
		{name: "an index over a Double column of a table the plan changes",
			diff:    withIndex(field("score", "DOUBLE", true), schemamodel.Index{Name: "items_score", Fields: []string{"score"}}),
			wantErr: `adding index "items_score" to table "items": column "score" is Double, which YDB refuses as an index key`},
		{name: "an index over the key of a table the plan changes",
			diff:    withIndex(field("score", "DOUBLE", true), schemamodel.Index{Name: "items_id", Fields: []string{"id"}}),
			wantErr: `adding index "items_id" to table "items": its columns are the table's key, .*`},
		{name: "an index covering the key of a table the plan changes",
			diff: withIndex(field("note", "TEXT", true),
				schemamodel.Index{Name: "items_note", Fields: []string{"note"}, IncludeColumns: []string{"id"}}),
			wantErr: `adding index "items_note" to table "items": it covers key column "id", .*`},
		{name: "a table constraint",
			diff: modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(),
				ConstraintsAdded: []string{"items_n_check"}}),
			wantErr: `table "items": YDB has no constraint but the key, and the key never changes`},
		{name: "a change the planner does not know",
			diff: modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(),
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"collation": "a -> b"}}}}),
			wantErr: `column "n" of table "items": YDB cannot change collation of a column in place \(a -> b\)`},
		{name: "a column comment",
			diff: modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(),
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", CommentChange: &difftypes.CommentChange{}}}}),
			wantErr: `the comment on column "n" of table "items": storing a comment on a YDB object is not implemented yet .*`},
		{name: "a view",
			diff:    &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{{Name: "v"}}},
			wantErr: `the plan changes a view: managing YDB views is not implemented yet .*`},
		{name: "a role",
			diff:    &difftypes.SchemaDiff{RolesAdded: difftypes.RoleChanges{{Name: "r"}}},
			wantErr: `the plan changes a role or a privilege: managing YDB users, groups and permissions is not implemented yet .*`},
		{name: "a database",
			diff:    &difftypes.SchemaDiff{SchemasRemoved: []string{"app"}},
			wantErr: `.*the ydb planner plans none`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.New().GenerateMigrationAST(test.diff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestGenerateMigrationAST_EveryNodeRendersAlone pins that a plan is a list of
// whole statements: each node renders on its own into statements that end in a
// semicolon, so the executor can run them one per query.
func TestGenerateMigrationAST_EveryNodeRendersAlone(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName: "items", Desired: itemsDeclaration(field("a", "TEXT", true), field("b", "TEXT", true)),
			ColumnsAdded: difftypes.ColumnChanges{field("a", "TEXT", true), field("b", "TEXT", true)},
		}},
		IndexesAdded: difftypes.IndexChanges{
			{TableName: "items", Index: schemamodel.Index{Name: "ia", Fields: []string{"a"}}},
			{TableName: "items", Index: schemamodel.Index{Name: "ib", Fields: []string{"b"}}},
		},
	}

	nodes, err := ydb.New().GenerateMigrationAST(diff)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 4)
	for _, node := range nodes {
		sql, err := renderer.RenderSQL("ydb", node)
		c.Assert(err, qt.IsNil)
		c.Assert(strings.Count(sql, ";\n"), qt.Equals, 1, qt.Commentf("%T renders %q", node, sql))
	}
	_, isIndex := nodes[3].(*ast.IndexNode)
	c.Assert(isIndex, qt.IsTrue)
}

// modified wraps one table modification in a diff.
func modified(tableDiff difftypes.TableDiff) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{tableDiff}}
}

// withIndex is a plan that adds column to items and an index to the same
// table, so the plan carries the table's declaration.
func withIndex(column schemamodel.Field, index schemamodel.Index) *difftypes.SchemaDiff {
	diff := modified(difftypes.TableDiff{TableName: "items", Desired: itemsDeclaration(column),
		ColumnsAdded: difftypes.ColumnChanges{column}})
	diff.IndexesAdded = difftypes.IndexChanges{{TableName: "items", Index: index}}
	return diff
}

// TestGenerateMigrationAST_DropsATableWithItsKey pins the removal a read hands
// the planner with every dropped table: the reader reports a table's key as a
// constraint, so dropping the table removes the key too. DROP TABLE takes the
// key with it, and refusing the removal as a key change would refuse every
// plan that drops a table. A key removal on a table the plan keeps is still a
// key change; see TestGenerateMigrationAST_RefusesByCapability_FailurePath.
func TestGenerateMigrationAST_DropsATableWithItsKey(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesRemoved: []string{"app.obsolete"},
		ConstraintsRemoved: difftypes.ConstraintRemovals{
			{Name: "obsolete_pkey", TableName: "app.obsolete", Type: "PRIMARY KEY"},
		},
	}

	c.Assert(render(c, capability.YDB262(), diff), qt.Equals, "DROP TABLE `app/obsolete`;\n")
}
