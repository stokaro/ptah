package atlaschange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlaschange"
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
			of, ok := atlaschange.Of(tt.node)
			c.Assert(ok, qt.IsTrue)
			c.Assert(of, qt.Equals, tt.of)
			undone, ok := atlaschange.Undone(tt.node)
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
			of, ok := atlaschange.Of(tt.node)
			c.Assert(ok, qt.IsFalse)
			c.Assert(of, qt.Equals, "")
			undone, ok := atlaschange.Undone(tt.node)
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

			got, ok := atlaschange.Rollback(test.forward)(test.node)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
