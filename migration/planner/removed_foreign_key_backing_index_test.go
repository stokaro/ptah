package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// heldBackedForeignKey is a read of child, whose foreign key child_fk stands on the
// index the server built for it.
func heldBackedForeignKey() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "parent", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", ColumnType: "bigint", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "child", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", ColumnType: "bigint", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "pid", DataType: "bigint", ColumnType: "bigint", IsNullable: "YES"},
			}},
		},
		Indexes: []catalog.Index{{Name: "child_fk", TableName: "child", Columns: []string{"pid"}}},
		Constraints: []catalog.Constraint{{
			Name: "child_fk", TableName: "child", Type: "FOREIGN KEY", ColumnName: "pid", ColumnNames: []string{"pid"},
			ForeignTable: new("parent"), ForeignColumn: new("id"),
		}},
	}
}

// declaredWithoutForeignKey declares parent and child with change applied.
func declaredWithoutForeignKey(change func(*schemamodel.Database)) *schemamodel.Database {
	declared := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Parent", Name: "parent"}, {StructName: "Child", Name: "child"}},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "id", Type: "bigint", Primary: true},
			{StructName: "Child", Name: "id", Type: "bigint", Primary: true},
			{StructName: "Child", Name: "pid", Type: "bigint", Nullable: true},
		},
	}
	change(declared)
	return declared
}

// backedForeignKeyPlan plans declared over heldBackedForeignKey on dialect.
func backedForeignKeyPlan(c *qt.C, dialect string, declared *schemamodel.Database) []string {
	c.Helper()
	return plannedOverHeld(c, dialect, declared, heldBackedForeignKey())
}

// plannedOverHeld plans declared over held on dialect in the exact style.
func plannedOverHeld(c *qt.C, dialect string, declared *schemamodel.Database, held *catalog.Database) []string {
	c.Helper()
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDialect(c.Context(), declared, held, dialect, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, dialect,
		planext.Options{Capabilities: capability.ForDialect(dialect), Statements: planext.StatementStyleExact})
	c.Assert(err, qt.IsNil)
	return statements
}

// TestRemovedForeignKeyBackingIndex_HappyPath pins the index that leaves with its key
// on the servers that keep it, and the plans that have to leave it alone.
func TestRemovedForeignKeyBackingIndex_HappyPath(t *testing.T) {
	const dropKey = "ALTER TABLE `child` DROP FOREIGN KEY `child_fk`"
	tests := []struct {
		name   string
		change func(*schemamodel.Database)
		want   []string
	}{
		{
			name:   "the index leaves with the key",
			change: func(*schemamodel.Database) {},
			want:   []string{dropKey, "DROP INDEX `child_fk` ON `child`"},
		},
		{
			name: "the declaration holds the index",
			change: func(d *schemamodel.Database) {
				d.Indexes = []schemamodel.Index{{StructName: "Child", Name: "child_fk", Fields: []string{"pid"}}}
			},
			want: []string{dropKey},
		},
		{
			name: "a foreign key the plan adds takes the index over",
			change: func(d *schemamodel.Database) {
				d.Constraints = []schemamodel.Constraint{{
					StructName: "Child", Name: "new_fk", Type: "FOREIGN KEY", Table: "child", Columns: []string{"pid"},
					ForeignTable: "parent", ForeignColumns: []string{"id"},
				}}
			},
			want: []string{
				"ALTER TABLE `child` ADD CONSTRAINT `new_fk` FOREIGN KEY (`pid`) REFERENCES `parent`(`id`)",
				dropKey,
			},
		},
		{
			name: "the plan changes the key under its name",
			change: func(d *schemamodel.Database) {
				d.Constraints = []schemamodel.Constraint{{
					StructName: "Child", Name: "child_fk", Type: "FOREIGN KEY", Table: "child", Columns: []string{"pid"},
					ForeignTable: "parent", ForeignColumns: []string{"id"}, OnDelete: "CASCADE",
				}}
			},
			want: []string{
				dropKey,
				"ALTER TABLE `child` ADD CONSTRAINT `child_fk` FOREIGN KEY (`pid`) REFERENCES `parent`(`id`) ON DELETE CASCADE",
			},
		},
		{
			name: "the index leaves with its column",
			change: func(d *schemamodel.Database) {
				d.Fields = d.Fields[:2]
			},
			want: []string{dropKey, "ALTER TABLE `child` DROP COLUMN `pid`"},
		},
		{
			name: "the index leaves with its table",
			change: func(d *schemamodel.Database) {
				d.Tables = d.Tables[:1]
				d.Fields = d.Fields[:1]
			},
			want: []string{dropKey, "DROP TABLE `child`"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(backedForeignKeyPlan(c, platform.MySQL, declaredWithoutForeignKey(test.change)), qt.DeepEquals, test.want)
		})
	}
}

// TestRemovedForeignKeyBackingIndex_AServerThatDropsItWithTheKey is the control:
// Spanner reports a backing index too and drops it with the key, so a drop of its own
// would answer that it does not exist.
func TestRemovedForeignKeyBackingIndex_AServerThatDropsItWithTheKey(t *testing.T) {
	c := qt.New(t)
	held := heldBackedForeignKey()
	held.Indexes[0].Name = "IDX_child_pid_FBF4366D73F2084A"

	got := plannedOverHeld(c, platform.Spanner, declaredWithoutForeignKey(func(*schemamodel.Database) {}), held)

	c.Assert(strings.Join(got, "\n"), qt.Not(qt.Contains), "DROP INDEX")
}

// TestRemovedForeignKeyBackingIndex_AnIndexThePlanAddsTakesItOver pins the statement
// that drops a removed key's index when the plan adds an index over the key's columns.
// The new index makes the server drop the index it built for the key by itself, also
// after DROP FOREIGN KEY, so a DROP INDEX after the addition answered `Error 1091: Can't
// DROP` on MySQL 8.4 and 26.10 and MariaDB 12.3. The statement that adds the new index
// drops the old one, after the key.
func TestRemovedForeignKeyBackingIndex_AnIndexThePlanAddsTakesItOver(t *testing.T) {
	const dropKey = "ALTER TABLE `child` DROP FOREIGN KEY `child_fk`"
	unnamed := heldBackedForeignKey()
	unnamed.Constraints[0].Name = "child_ibfk_1"
	unnamed.Indexes[0].Name = "pid"
	withIndex := func(name string, fields ...string) func(*schemamodel.Database) {
		return func(d *schemamodel.Database) {
			d.Fields = append(d.Fields, schemamodel.Field{StructName: "Child", Name: "qid", Type: "bigint", Nullable: true})
			d.Indexes = append(d.Indexes, schemamodel.Index{StructName: "Child", Name: name, Fields: fields})
		}
	}
	movedWithIndex := declaredMovedForeignKey("child_fk")
	movedWithIndex.Indexes = []schemamodel.Index{{StructName: "Child", Name: "idx_pid", Fields: []string{"pid"}}}
	tests := []struct {
		name     string
		held     *catalog.Database
		declared *schemamodel.Database
		want     []string
	}{
		{
			name:     "an index over the key's column",
			held:     heldMovedForeignKey(),
			declared: declaredWithoutForeignKey(withIndex("idx_pid", "pid")),
			want:     []string{dropKey, "ALTER TABLE `child` DROP INDEX `child_fk`, ADD INDEX `idx_pid` (`pid`)"},
		},
		{
			name:     "a wider index",
			held:     heldMovedForeignKey(),
			declared: declaredWithoutForeignKey(withIndex("w", "pid", "qid")),
			want:     []string{dropKey, "ALTER TABLE `child` DROP INDEX `child_fk`, ADD INDEX `w` (`pid`, `qid`)"},
		},
		{
			name: "an unnamed key, whose index the server named after the column",
			held: unnamed,
			declared: declaredWithoutForeignKey(func(d *schemamodel.Database) {
				d.Indexes = []schemamodel.Index{{StructName: "Child", Name: "idx_pid", Fields: []string{"pid"}}}
			}),
			want: []string{
				"ALTER TABLE `child` DROP FOREIGN KEY `child_ibfk_1`",
				"ALTER TABLE `child` DROP INDEX `pid`, ADD INDEX `idx_pid` (`pid`)",
			},
		},
		{
			name:     "the key moves to another column under its name",
			held:     heldMovedForeignKey(),
			declared: movedWithIndex,
			want: []string{
				dropKey,
				"ALTER TABLE `child` DROP INDEX `child_fk`, ADD INDEX `idx_pid` (`pid`)",
				"ALTER TABLE `child` ADD CONSTRAINT `child_fk` FOREIGN KEY (`qid`) REFERENCES `parent`(`id`)",
			},
		},
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+" "+test.name, func(t *testing.T) {
				c := qt.New(t)

				got := plannedOverHeld(c, dialect, test.declared, test.held)

				c.Assert(got, qt.DeepEquals, test.want)
			})
		}
	}
}

// TestRemovedForeignKeyBackingIndex_AnIndexThePlanAddsLeavesItAlone pins the plans in
// which an added index takes nothing over: one over another column, and one the server
// cannot stand the key on, a descending index on MySQL. The server keeps its own index
// until the key leaves, and the removals drop it.
func TestRemovedForeignKeyBackingIndex_AnIndexThePlanAddsLeavesItAlone(t *testing.T) {
	tests := []struct {
		name  string
		index schemamodel.Index
		want  string
	}{
		{
			name:  "an index over another column",
			index: schemamodel.Index{StructName: "Child", Name: "idx_qid", Fields: []string{"qid"}},
			want:  "CREATE INDEX `idx_qid` ON `child` (`qid`)",
		},
		{
			name:  "a descending index",
			index: schemamodel.Index{StructName: "Child", Name: "idx_pid", Fields: []string{"pid"}, Parts: []schemamodel.IndexPart{{Name: "pid", Desc: true}}},
			want:  "CREATE INDEX `idx_pid` ON `child` (`pid` DESC)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := declaredWithoutForeignKey(func(d *schemamodel.Database) {
				d.Fields = append(d.Fields, schemamodel.Field{StructName: "Child", Name: "qid", Type: "bigint", Nullable: true})
				d.Indexes = []schemamodel.Index{test.index}
			})

			got := plannedOverHeld(c, platform.MySQL, declared, heldMovedForeignKey())

			c.Assert(got, qt.DeepEquals, []string{
				test.want,
				"ALTER TABLE `child` DROP FOREIGN KEY `child_fk`",
				"DROP INDEX `child_fk` ON `child`",
			})
		})
	}
}
