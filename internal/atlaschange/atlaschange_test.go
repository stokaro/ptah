package atlaschange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlaschange"
	"ptah.run/internal/planner/sqliterebuild"
	"ptah.run/internal/sqliteforeignkeys"
)

// The rows name a statement the way the pinned community binary v1.3.0 names
// it in a golang-migrate file, and the forward change a rollback statement of
// that kind undoes. The wording of tables, columns, indexes, enum types and
// schemas is the binary's own; the objects it does not plan follow the same
// pattern.
func TestOfAndUndone_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		node   ast.Node
		of     string
		undone string
	}{
		{
			name:   "a new table, named without its schema",
			node:   ast.NewCreateTable("public.owners"),
			of:     `create "owners" table`,
			undone: `drop "owners" table`,
		},
		{
			name:   "a dropped table",
			node:   ast.NewDropTable("items"),
			of:     `drop "items" table`,
			undone: `create "items" table`,
		},
		{
			name:   "two tables one statement drops",
			node:   ast.NewDropTable("public.b").SetNames([]string{"public.b", "public.a"}),
			of:     `drop "b" table, drop "a" table`,
			undone: `create "b" table, create "a" table`,
		},
		{
			name: "an added column",
			node: &ast.AlterTableNode{Name: "owners", Operations: []ast.AlterOperation{
				&ast.AddColumnOperation{Column: ast.NewColumn("email", "varchar")},
			}},
			of:     `modify "owners" table`,
			undone: `modify "owners" table`,
		},
		{
			name: "a dropped constraint",
			node: &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{
				&ast.DropConstraintOperation{ConstraintName: "items_note_len"},
			}},
			of:     `modify "items" table`,
			undone: `modify "items" table`,
		},
		{
			name: "a renamed column",
			node: &ast.AlterTableNode{Name: "owners", Operations: []ast.AlterOperation{
				&ast.RenameColumnOperation{OldName: "mail", NewName: "email"},
			}},
			of:     `rename a column from "mail" to "email"`,
			undone: `rename a column from "email" to "mail"`,
		},
		{
			name: "a renamed table",
			node: &ast.AlterTableNode{Name: "owner", Operations: []ast.AlterOperation{
				&ast.RenameTableOperation{NewName: "owners"},
			}},
			of:     `rename a table from "owner" to "owners"`,
			undone: `rename a table from "owners" to "owner"`,
		},
		{
			name: "a table comment",
			node: &ast.AlterTableNode{Name: "owners", Operations: []ast.AlterOperation{
				&ast.SetCommentOperation{Comment: "people"},
			}},
			of:     `set comment to table: "owners"`,
			undone: `set comment to table: "owners"`,
		},
		{
			name: "a column comment",
			node: &ast.AlterTableNode{Name: "owners", Operations: []ast.AlterOperation{
				&ast.SetCommentOperation{Column: "email", Comment: "where to write"},
			}},
			of:     `set comment to column: "email" on table: "owners"`,
			undone: `set comment to column: "email" on table: "owners"`,
		},
		{
			name:   "a new index",
			node:   ast.NewIndex("items_owner_idx", "items", "owner_id"),
			of:     `create index "items_owner_idx" to table: "items"`,
			undone: `drop index "items_owner_idx" from table: "items"`,
		},
		{
			name:   "a dropped index",
			node:   ast.NewDropIndex("items_owner_idx").SetTable("items"),
			of:     `drop index "items_owner_idx" from table: "items"`,
			undone: `create index "items_owner_idx" to table: "items"`,
		},
		{
			name:   "a new enum type",
			node:   ast.NewEnum("status", "active", "inactive"),
			of:     `create enum type "status"`,
			undone: `drop enum type "status"`,
		},
		{
			name:   "an enum value",
			node:   &ast.AlterTypeNode{Name: "status", Operations: []ast.TypeOperation{ast.NewAddEnumValueOperation("archived")}},
			of:     `add value to enum type: "status"`,
			undone: `add value to enum type: "status"`,
		},
		{
			name:   "a new schema",
			node:   &ast.CreateSchemaNode{Name: "billing", IfNotExists: true},
			of:     `Add new schema named "billing"`,
			undone: `Drop schema named "billing"`,
		},
		{
			name:   "a new view",
			node:   ast.NewCreateView("charges").SetBody("SELECT 1"),
			of:     `create "charges" view`,
			undone: `drop "charges" view`,
		},
		{
			name:   "a view replaced in place",
			node:   ast.NewCreateView("charges").SetBody("SELECT 1").SetReplace(),
			of:     `modify "charges" view`,
			undone: `modify "charges" view`,
		},
		{
			name:   "a dropped view",
			node:   ast.NewDropView("charges"),
			of:     `drop "charges" view`,
			undone: `create "charges" view`,
		},
		{
			name:   "a dropped materialized view",
			node:   ast.NewDropMaterializedView("totals"),
			of:     `drop "totals" materialized view`,
			undone: `create "totals" materialized view`,
		},
		{
			name:   "a dropped procedure",
			node:   ast.NewDropFunction("archive").SetKind(schemamodel.FunctionKindProcedure),
			of:     `drop "archive" procedure`,
			undone: `create "archive" procedure`,
		},
		{
			name:   "a dropped trigger",
			node:   ast.NewDropTrigger("touch", "items"),
			of:     `drop "touch" trigger`,
			undone: `create "touch" trigger`,
		},
		{
			name:   "an index comment",
			node:   ast.NewObjectComment(ast.CommentedIndex, "items_owner_idx", "by owner").SetTable("items"),
			of:     `set comment to index: "items_owner_idx" on table: "items"`,
			undone: `set comment to index: "items_owner_idx" on table: "items"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			of, ok := nameOf(tt.node)
			c.Assert(ok, qt.IsTrue)
			c.Assert(of, qt.Equals, tt.of)
			undone, ok := nameUndone(tt.node)
			c.Assert(ok, qt.IsTrue)
			c.Assert(undone, qt.Equals, tt.undone)
		})
	}
}

// A node that makes no change Atlas would name gets no comment: a planner's
// own note, and SQL Ptah writes verbatim.
func TestOfAndUndone_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
	}{
		{name: "a planner's note", node: ast.NewComment("SKIP: DROP TABLE of t omitted by diff policy (skip: drop_table)")},
		{name: "raw SQL", node: ast.NewRawSQL("DO $$ BEGIN END $$;")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			of, ok := nameOf(tt.node)
			c.Assert(ok, qt.IsFalse)
			c.Assert(of, qt.Equals, "")
			undone, ok := nameUndone(tt.node)
			c.Assert(ok, qt.IsFalse)
			c.Assert(undone, qt.Equals, "")
		})
	}
}

// TestRollback_HappyPath pins the namer of a rollback's statements. A view the
// forward plan replaces in place and the rollback drops and creates again
// undoes `modify "v" view` with both statements. A view the forward plan
// drops and creates itself keeps the comment each statement undoes, and a
// statement on another object keeps [atlaschange.Undone]'s answer.
func TestRollback_HappyPath(t *testing.T) {
	replaced := []ast.Node{ast.NewCreateView("v").SetReplace()}
	rebuilt := []ast.Node{ast.NewDropView("v"), ast.NewCreateView("v")}
	tests := []struct {
		name    string
		forward []ast.Node
		node    ast.Node
		want    string
	}{
		{name: "a drop of a view the forward plan replaced", forward: replaced, node: ast.NewDropView("v"), want: `modify "v" view`},
		{name: "a create of a view the forward plan replaced", forward: replaced, node: ast.NewCreateView("public.v"), want: `modify "v" view`},
		{name: "a drop of a view the forward plan rebuilt", forward: rebuilt, node: ast.NewDropView("v"), want: `create "v" view`},
		{name: "a create of a view the forward plan rebuilt", forward: rebuilt, node: ast.NewCreateView("v"), want: `drop "v" view`},
		{name: "a drop of another view", forward: replaced, node: ast.NewDropView("w"), want: `create "w" view`},
		{name: "a drop of a view the forward plan replaced, spelled in another case", forward: replaced,
			node: ast.NewDropView("V"), want: `modify "v" view`},
		{name: "a drop of a view of the same name in another schema",
			forward: []ast.Node{ast.NewCreateView("app.v").SetReplace()}, node: ast.NewDropView("reporting.v"), want: `create "v" view`},
		{name: "a drop of a view the forward plan replaced beside one of the same name it rebuilt in another schema",
			forward: []ast.Node{ast.NewCreateView("app.v").SetReplace(), ast.NewDropView("reporting.v"), ast.NewCreateView("reporting.v")},
			node:    ast.NewDropView("app.v"), want: `modify "v" view`},
		{name: "a drop of a trigger the forward plan replaced on another table",
			forward: []ast.Node{ast.NewCreateTrigger("audit", "orders").SetReplace()}, node: ast.NewDropTrigger("audit", "items"),
			want: `create "audit" trigger`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := nameRollback(test.forward, test.node)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// nameOf is the comment the PostgreSQL wording writes above node in a forward
// plan, and whether it writes one.
func nameOf(node ast.Node) (string, bool) {
	name := atlaschange.Words{}.Forward([]ast.Node{node})[0]
	return name, name != ""
}

// nameUndone is the comment of the forward change node undoes in a rollback
// planned against nothing, and whether it names one.
func nameUndone(node ast.Node) (string, bool) {
	name := atlaschange.Words{}.Rollback(nil, []ast.Node{node})[0]
	return name, name != ""
}

// nameRollback is the comment of the forward change node undoes in a rollback
// planned against forward, and whether it names one.
func nameRollback(forward []ast.Node, node ast.Node) (string, bool) {
	name := atlaschange.Words{}.Rollback(forward, []ast.Node{node})[0]
	return name, name != ""
}

// rebuildPlan is the plan the SQLite planner writes for one table it rebuilds:
// the statements between the two pragmas are the rebuild's own.
func rebuildPlan() []ast.Node {
	rebuild := sqliterebuild.CopyStatement{New: "new_t", Old: "t"}
	return []ast.Node{
		ast.NewRawSQL(sqliteforeignkeys.DisableStatement),
		ast.NewCreateTable("new_t"),
		ast.NewRawSQL(rebuild.CopyRows(`"id"`, `"id"`)),
		ast.NewDropTable("t"),
		ast.NewRawSQL(rebuild.Rename()),
		ast.NewIndex("i", "t", "a"),
		ast.NewDropTable("gone"),
		ast.NewRawSQL(sqliteforeignkeys.EnableStatement),
	}
}

// TestForward_NamesAnEnginesChangesInItsWords_HappyPath pins the wording that
// differs per engine. The rows are what the pinned community binary v1.3.0
// writes on each engine it has a driver for; `integration/atlasreference`
// compares these words with the binary's own.
func TestForward_NamesAnEnginesChangesInItsWords_HappyPath(t *testing.T) {
	added := &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: ast.NewColumn("c", "INT")}}}
	tests := []struct {
		name    string
		dialect string
		node    ast.Node
		want    string
	}{
		{name: "a new schema on PostgreSQL", dialect: platform.Postgres, node: ast.NewCreateSchema("s"), want: `Add new schema named "s"`},
		{name: "a new schema on MySQL", dialect: platform.MySQL, node: ast.NewCreateSchema("s"), want: `add new schema named "s"`},
		{name: "a new schema on MariaDB", dialect: platform.MariaDB, node: ast.NewCreateSchema("s"), want: `add new schema named "s"`},
		{name: "a dropped database on MySQL", dialect: platform.MySQL, node: ast.NewRawSQL("DROP DATABASE `s`"), want: `drop schema named "s"`},
		{name: "an added column on SQLite", dialect: platform.SQLite, node: added, want: `add column "c" to table: "t"`},
		{name: "an added column on libSQL", dialect: "libsql", node: added, want: `add column "c" to table: "t"`},
		{name: "an added column on PostgreSQL", dialect: platform.Postgres, node: added, want: `modify "t" table`},
		{name: "an added column on MySQL", dialect: platform.MySQL, node: added, want: `modify "t" table`},
		{name: "an added column on an engine the binary has no driver for", dialect: platform.SQLServer, node: added, want: `modify "t" table`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlaschange.For(test.dialect).Forward([]ast.Node{test.node})

			c.Assert(got, qt.DeepEquals, []string{test.want})
		})
	}
}

// A statement that is the same text on another engine, or that has the shape of
// a rebuild's step without the copy that says a table is being rebuilt, is not
// named as one.
func TestForward_DoesNotNameAnotherEnginesStatement_FailurePath(t *testing.T) {
	copyRows := sqliterebuild.CopyStatement{New: "new_t", Old: "t"}.CopyRows(`"id"`, `"id"`)
	tests := []struct {
		name    string
		dialect string
		node    ast.Node
	}{
		{name: "a dropped database on PostgreSQL", dialect: platform.Postgres, node: ast.NewRawSQL("DROP DATABASE `s`")},
		{name: "a dropped database in other quotes", dialect: platform.MySQL, node: ast.NewRawSQL(`DROP DATABASE "s"`)},
		{name: "a pragma on PostgreSQL", dialect: platform.Postgres, node: ast.NewRawSQL(sqliteforeignkeys.DisableStatement)},
		{name: "a copy on MySQL", dialect: platform.MySQL, node: ast.NewRawSQL(copyRows)},
		{name: "another statement on SQLite", dialect: platform.SQLite, node: ast.NewRawSQL("DELETE FROM t;")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlaschange.For(test.dialect).Forward([]ast.Node{test.node})

			c.Assert(got, qt.DeepEquals, []string{""})
		})
	}
}

// TestForward_NamesEachStepOfATableRebuild_HappyPath pins the comments around
// a SQLite table rebuild: the steps the copy of the rows identifies carry the
// rebuild's words, and a drop of another table keeps its own.
func TestForward_NamesEachStepOfATableRebuild_HappyPath(t *testing.T) {
	c := qt.New(t)

	got := atlaschange.For(platform.SQLite).Forward(rebuildPlan())

	c.Assert(got, qt.DeepEquals, []string{
		"disable the enforcement of foreign-keys constraints",
		`create "new_t" table`,
		`copy rows from old table "t" to new temporary table "new_t"`,
		`drop "t" table after copying rows`,
		`rename temporary table "new_t" to "t"`,
		`create index "i" to table: "t"`,
		`drop "gone" table`,
		"enable back the enforcement of foreign-keys constraints",
	})
}

// TestRollback_ARebuildKeepsItsWords_HappyPath pins a rollback that rebuilds a
// table. A rebuild undoes no statement of the forward plan one by one, so its
// steps keep their words, and a statement outside it still reads the change it
// undoes. On SQLite an added column's rollback undoes `add column`, and a column
// the forward plan dropped comes back undoing the drop.
func TestRollback_ARebuildKeepsItsWords_HappyPath(t *testing.T) {
	c := qt.New(t)
	forward := []ast.Node{&ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: ast.NewColumn("c", "INT")}}}}
	plan := append(rebuildPlan(),
		&ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: "c"}}},
		&ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: ast.NewColumn("d", "INT")}}},
	)

	got := atlaschange.For(platform.SQLite).Rollback(forward, plan)

	c.Assert(got, qt.DeepEquals, []string{
		"disable the enforcement of foreign-keys constraints",
		`create "new_t" table`,
		`copy rows from old table "t" to new temporary table "new_t"`,
		`drop "t" table after copying rows`,
		`rename temporary table "new_t" to "t"`,
		`drop index "i" from table: "t"`,
		`create "gone" table`,
		"enable back the enforcement of foreign-keys constraints",
		`add column "c" to table: "t"`,
		`drop column "d" from table: "t"`,
	})
}
