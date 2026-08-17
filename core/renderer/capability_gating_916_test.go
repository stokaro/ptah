package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
)

// renameColumn916 is the one node every arm below renders, so the only thing
// that differs between them is the capability set the renderer was built with.
func renameColumn916() ast.Node {
	return &ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{&ast.RenameColumnOperation{
			OldName: "email",
			NewName: "email_address",
		}},
	}
}

// TestSQLiteRenderer_RenameColumnFollowsTheCapabilitySet pins both arms of the
// gate stokaro/ptah#916 put on ALTER TABLE ... RENAME COLUMN. SQLite gained the
// clause in 3.25; below that a rename is a table rebuild, which is a different
// plan than this node describes, so the statement is replaced by a diagnostic
// naming what was dropped rather than by DDL the target refuses.
func TestSQLiteRenderer_RenameColumnFollowsTheCapabilitySet(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		want string
	}{
		{
			name: "a target with the clause emits it",
			caps: capability.SQLite3(),
			want: "ALTER TABLE \"users\" RENAME COLUMN \"email\" TO \"email_address\";\n",
		},
		{
			name: "a target below 3.25 says what it could not do",
			caps: capability.SQLite324(),
			want: "-- SQLITE: ALTER TABLE ... RENAME COLUMN \"email\" is not supported\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			r, err := renderer.NewRendererWithCapabilities("sqlite", test.caps)
			c.Assert(err, qt.IsNil)

			sql, err := r.Render(renameColumn916())

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// TestSQLiteRenderer_DefaultConstructorEmitsTheClause is the control for the
// table above. Without it a New() that handed the renderer an empty set would
// render the diagnostic on every offline path, and the 3.24 row would notice
// only by passing.
func TestSQLiteRenderer_DefaultConstructorEmitsTheClause(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQL("sqlite", renameColumn916())

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "RENAME COLUMN")
}

// TestSQLServerRenderer_RenameColumnIsAlwaysSpRename is the non-interference
// control for the key above: RenameColumnClause is false on every SQL Server
// line measured (15.0.4480.2, 16.0.4265.3, 17.0.4075.5 all answer "Incorrect
// syntax near 'RENAME'"), and sp_rename is what that false records -- so
// neither arm of the SQLite gate may reach this dialect.
func TestSQLServerRenderer_RenameColumnIsAlwaysSpRename(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQL("sqlserver", renameColumn916())

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "EXEC sp_rename")
	c.Assert(sql, qt.Not(qt.Contains), "RENAME COLUMN")
}

// TestSQLServerRenderer_DropGuardsFollowTheCapabilitySet pins both arms of the
// two IF EXISTS guards stokaro/ptah#916 corrected. Both are ACCEPTED on every
// supported SQL Server line, so the shipped preset emits them; a set that
// declines them renders the bare statement rather than syntax the target would
// refuse.
func TestSQLServerRenderer_DropGuardsFollowTheCapabilitySet(t *testing.T) {
	dropConstraint := &ast.AlterTableNode{
		Name: "dbo.things",
		Operations: []ast.AlterOperation{&ast.DropConstraintOperation{
			ConstraintName: "chk_qty",
			IfExists:       true,
		}},
	}
	dropIndex := ast.NewDropIndex("idx_things_qty").SetTable("dbo.things").SetIfExists()

	tests := []struct {
		name           string
		caps           capability.Capabilities
		wantConstraint string
		wantIndex      string
	}{
		{
			name:           "the shipped preset guards both drops",
			caps:           capability.SQLServer2022(),
			wantConstraint: "ALTER TABLE [dbo].[things] DROP CONSTRAINT IF EXISTS [chk_qty];",
			wantIndex:      "DROP INDEX IF EXISTS [idx_things_qty] ON [dbo].[things];",
		},
		{
			name: "a set declining the guards drops them",
			caps: capability.SQLServer2022().
				With(capability.DropConstraintIfExists, false).
				With(capability.DropIndexIfExists, false),
			wantConstraint: "ALTER TABLE [dbo].[things] DROP CONSTRAINT [chk_qty];",
			wantIndex:      "DROP INDEX [idx_things_qty] ON [dbo].[things];",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			r, err := renderer.NewRendererWithCapabilities("sqlserver", test.caps)
			c.Assert(err, qt.IsNil)

			constraintSQL, err := r.Render(dropConstraint)
			c.Assert(err, qt.IsNil)
			c.Assert(constraintSQL, qt.Contains, test.wantConstraint)

			indexSQL, err := r.Render(dropIndex)
			c.Assert(err, qt.IsNil)
			c.Assert(indexSQL, qt.Contains, test.wantIndex)
		})
	}
}
