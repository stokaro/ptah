package generator

// White-box testing required: the subject is the unexported reverse plan
// builder (reverseSchemaDiffWithSchema -> dropReverseConstraintsRestoredByTableCreation)
// as it is rendered by the unexported generateDownMigrationSQL. The exported
// migration-file API reaches the same code only through a filesystem and a live
// connection, which would put the failure somewhere other than the rule under
// test.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestGenerateDownMigration_DropTable_RollbackSaysEachConstraintOnce covers the
// rollback of a migration that DROPS a table, on the two dialect families whose
// planner emits ALTER TABLE ... ADD CONSTRAINT.
//
// The reverse re-creates the table, and the re-created body already carries the
// primary key inline and has its field-level foreign key re-added by the
// planner's new-table pass. Before stokaro/ptah#1013 the swapped constraint
// lists said both a second time, and the rollback was refused at the second
// statement — measured on PostgreSQL 17.10
// (`multiple primary keys for table "gadgets" are not allowed`, exit 3) and on
// MySQL 9.7 (`ERROR 1068 (42000): Multiple primary key defined`, exit 1),
// through `ptah migrations generate` and through the Atlas-compatible
// `migrate diff` alike.
//
// Both halves of every row are the assertion, and they fail to different
// mutants. The "exactly once" counts fail when the rule is removed. The CHECK
// and UNIQUE lines fail to the cheaper rule that drops EVERY constraint
// addition whose host table is re-created: that one also produces a rollback
// that applies, and silently restores a table without its CHECK — which is why
// this test asserts what the rollback KEEPS as loudly as what it stops
// repeating.
func TestGenerateDownMigration_DropTable_RollbackSaysEachConstraintOnce(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		// inlinePrimaryKey is the primary key as the re-created table's own
		// CREATE TABLE spells it, which differs per renderer. The rollback must
		// contain this and no separate ADD PRIMARY KEY.
		inlinePrimaryKey string
		// wantOnce are the statements the rollback must carry exactly one of.
		wantOnce []string
		// wantNone are the statements a correct rollback never repeats.
		wantNone []string
	}{
		{
			name:             "postgres",
			dialect:          "postgres",
			inlinePrimaryKey: "id integer PRIMARY KEY NOT NULL",
			wantOnce: []string{
				"ALTER TABLE gadgets ADD CONSTRAINT gadgets_widget_fk FOREIGN KEY (widget_id)",
				"ALTER TABLE gadgets ADD CONSTRAINT gadgets_qty_ck CHECK",
				"ALTER TABLE gadgets ADD CONSTRAINT gadgets_code_uq UNIQUE",
			},
			wantNone: []string{"ADD PRIMARY KEY"},
		},
		{
			name:             "mysql",
			dialect:          "mysql",
			inlinePrimaryKey: "id int PRIMARY KEY",
			wantOnce: []string{
				"ALTER TABLE gadgets ADD CONSTRAINT gadgets_widget_fk FOREIGN KEY (widget_id)",
				"ALTER TABLE gadgets ADD CONSTRAINT gadgets_qty_ck CHECK",
				"ALTER TABLE gadgets ADD CONSTRAINT gadgets_code_uq UNIQUE",
			},
			wantNone: []string{"ADD PRIMARY KEY"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			target, prior := droppedTableFixtures(tt.dialect)

			upDiff := schemadiff.CompareWithDialect(target, prior, tt.dialect)
			c.Assert(upDiff.TablesRemoved, qt.DeepEquals, []string{"gadgets"})

			down, err := generateDownMigrationSQL(upDiff, target, prior, tt.dialect)
			c.Assert(err, qt.IsNil)
			down = legacyRenderedSQL(down)

			c.Assert(strings.Count(down, "CREATE TABLE gadgets"), qt.Equals, 1,
				qt.Commentf("the rollback must put the dropped table back:\n%s", down))
			c.Assert(strings.Count(down, tt.inlinePrimaryKey), qt.Equals, 1,
				qt.Commentf("the re-created table carries its own primary key:\n%s", down))
			for _, statement := range tt.wantOnce {
				c.Assert(strings.Count(down, statement), qt.Equals, 1,
					qt.Commentf("want exactly one %q in:\n%s", statement, down))
			}
			for _, statement := range tt.wantNone {
				c.Assert(strings.Count(down, statement), qt.Equals, 0,
					qt.Commentf("want no %q in:\n%s", statement, down))
			}
		})
	}
}

// TestGenerateDownMigration_DropTable_KeepsWhatTableCreationCannotRestore is the
// other side of the same rule, on the two references the re-created table does
// NOT bring back on its own.
//
// A self-referencing foreign key is routed through a list the introspected
// schema never populates, and a multi-column foreign key cannot be expressed as
// a field-level reference at all, so for both of them the ALTER is the only
// emission there is. Dropping them along with the rest would turn an
// unexecutable rollback into one that applies and restores a table missing its
// references, which is the worse of the two failures.
func TestGenerateDownMigration_DropTable_KeepsWhatTableCreationCannotRestore(t *testing.T) {
	c := qt.New(t)
	target, prior := selfAndCompositeForeignKeyFixtures()

	upDiff := schemadiff.CompareWithDialect(target, prior, "postgres")
	c.Assert(upDiff.TablesRemoved, qt.Contains, "nodes")
	c.Assert(upDiff.TablesRemoved, qt.Contains, "pairs")

	down, err := generateDownMigrationSQL(upDiff, target, prior, "postgres")
	c.Assert(err, qt.IsNil)
	down = legacyRenderedSQL(down)

	kept := []string{
		"ALTER TABLE nodes ADD CONSTRAINT nodes_parent_fk FOREIGN KEY (parent_id)",
		"ALTER TABLE pairs ADD CONSTRAINT pairs_owner_fk FOREIGN KEY (owner_a, owner_b)",
	}
	for _, statement := range kept {
		c.Assert(strings.Count(down, statement), qt.Equals, 1,
			qt.Commentf("want exactly one %q in:\n%s", statement, down))
	}
	c.Assert(strings.Count(down, "PRIMARY KEY (a, b)"), qt.Equals, 1,
		qt.Commentf("the composite primary key is restored inline, once:\n%s", down))
	c.Assert(strings.Count(down, "ADD PRIMARY KEY"), qt.Equals, 0,
		qt.Commentf("want no separate ADD PRIMARY KEY in:\n%s", down))
}

// droppedTableFixtures returns the target schema that no longer declares
// `gadgets` and the introspected pre-change database that still holds it, with
// one constraint of every kind the reverse has to decide about: a primary key
// and a single-column foreign key (both restored by the re-created table), and
// a CHECK and a named UNIQUE (neither of which is).
func droppedTableFixtures(dialect string) (*goschema.Database, *dbschematypes.DBSchema) {
	target := &goschema.Database{
		Tables: []goschema.Table{{Name: "widgets", StructName: "Widget"}},
		Fields: []goschema.Field{
			{Name: "id", Type: droppedTableIntType(dialect), StructName: "Widget", Primary: true},
		},
	}
	check := "qty > 0"
	prior := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "widgets", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: droppedTableIntType(dialect), IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "gadgets", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: droppedTableIntType(dialect), IsNullable: "NO", IsPrimaryKey: true},
				{Name: "code", DataType: "text", IsNullable: "NO"},
				{Name: "qty", DataType: droppedTableIntType(dialect), IsNullable: "NO"},
				{Name: "widget_id", DataType: droppedTableIntType(dialect), IsNullable: "YES"},
			}},
		},
		Constraints: []dbschematypes.DBConstraint{
			{Name: "gadgets_pk", TableName: "gadgets", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}},
			{Name: "gadgets_code_uq", TableName: "gadgets", Type: "UNIQUE", ColumnName: "code", ColumnNames: []string{"code"}},
			{Name: "gadgets_qty_ck", TableName: "gadgets", Type: "CHECK", ColumnName: "qty", CheckClause: &check},
			{
				Name: "gadgets_widget_fk", TableName: "gadgets", Type: "FOREIGN KEY",
				ColumnName: "widget_id", ColumnNames: []string{"widget_id"},
				ForeignTable: new("widgets"), ForeignColumn: new("id"), ForeignColumns: []string{"id"},
				DeleteRule: new("NO ACTION"), UpdateRule: new("NO ACTION"),
			},
		},
	}
	return target, prior
}

// droppedTableIntType names the integer type each dialect's introspection
// reports, so the fixture is the shape the comparator really receives rather
// than a spelling only this test uses.
func droppedTableIntType(dialect string) string {
	return map[string]string{"postgres": "integer", "mysql": "int"}[dialect]
}

// selfAndCompositeForeignKeyFixtures returns a target schema holding neither
// table and a pre-change database holding two the re-created CREATE TABLE
// cannot fully restore: `nodes` with a self-referencing foreign key, and
// `pairs` with a composite primary key and a two-column foreign key.
func selfAndCompositeForeignKeyFixtures() (*goschema.Database, *dbschematypes.DBSchema) {
	target := &goschema.Database{
		Tables: []goschema.Table{{Name: "widgets", StructName: "Widget"}},
		Fields: []goschema.Field{
			{Name: "id", Type: "integer", StructName: "Widget", Primary: true},
		},
	}
	prior := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "widgets", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "nodes", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "parent_id", DataType: "integer", IsNullable: "YES"},
			}},
			{Name: "pairs", Columns: []dbschematypes.DBColumn{
				{Name: "a", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "b", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "owner_a", DataType: "integer", IsNullable: "YES"},
				{Name: "owner_b", DataType: "integer", IsNullable: "YES"},
			}},
		},
		Constraints: []dbschematypes.DBConstraint{
			{Name: "nodes_pk", TableName: "nodes", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}},
			{
				Name: "nodes_parent_fk", TableName: "nodes", Type: "FOREIGN KEY",
				ColumnName: "parent_id", ColumnNames: []string{"parent_id"},
				ForeignTable: new("nodes"), ForeignColumn: new("id"), ForeignColumns: []string{"id"},
				DeleteRule: new("NO ACTION"), UpdateRule: new("NO ACTION"),
			},
			{Name: "pairs_pk", TableName: "pairs", Type: "PRIMARY KEY", ColumnName: "a", ColumnNames: []string{"a", "b"}},
			{
				Name: "pairs_owner_fk", TableName: "pairs", Type: "FOREIGN KEY",
				ColumnName: "owner_a", ColumnNames: []string{"owner_a", "owner_b"},
				ForeignTable: new("pairs"), ForeignColumn: new("a"), ForeignColumns: []string{"a", "b"},
				DeleteRule: new("NO ACTION"), UpdateRule: new("NO ACTION"),
			},
		},
	}
	return target, prior
}
