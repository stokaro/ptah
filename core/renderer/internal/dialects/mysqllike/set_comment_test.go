package mysqllike_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/internal/bufwriter"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/mysqllike"
)

// A table's comment is a table option on MySQL and MariaDB, so changing one is
// an ALTER TABLE rather than a statement of its own -- stokaro/ptah#2168.
func TestVisitAlterTable_SetTableComment(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		want    string
	}{
		{
			name:    "a comment set",
			comment: "customers of record",
			want:    "ALTER TABLE `users` COMMENT='customers of record';",
		},
		{
			// The empty string is how these engines spell "no comment" -- the
			// catalog reports an uncommented table as '' rather than NULL, so
			// clearing one is COMMENT='' and the reader brings back the same
			// absence. PostgreSQL needs IS NULL for the same effect, because
			// its catalog reports NULL.
			name:    "a comment removed",
			comment: "",
			want:    "ALTER TABLE `users` COMMENT='';",
		},
		{
			// An apostrophe is the character a comment is most likely to hold,
			// and an unescaped one ends the literal and breaks the statement.
			name:    "a comment holding an apostrophe",
			comment: "the buyer's account",
			want:    "ALTER TABLE `users` COMMENT='the buyer''s account';",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			writer := &bufwriter.Writer{}
			renderer := mysqllike.NewWithCapabilities("mysql", writer, capability.ForDialect("mysql"))
			alter := &ast.AlterTableNode{
				Name:       "users",
				Operations: []ast.AlterOperation{&ast.SetCommentOperation{Comment: tt.comment}},
			}

			c.Assert(renderer.VisitAlterTable(alter), qt.IsNil)

			c.Assert(renderer.GetOutput(), qt.Contains, tt.want)
		})
	}
}

// A column comment never arrives as an operation of its own here, and the
// renderer says so instead of inventing a statement.
//
// These engines have no way to change one attribute of a column: MODIFY COLUMN
// restates the whole definition, and that definition already carries the
// comment. An operation that reached here would be a planner bug, and rendering
// something plausible would hide it behind valid-looking SQL.
func TestVisitAlterTable_SetColumnCommentIsRefused(t *testing.T) {
	c := qt.New(t)
	writer := &bufwriter.Writer{}
	renderer := mysqllike.NewWithCapabilities("mysql", writer, capability.ForDialect("mysql"))
	alter := &ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.SetCommentOperation{Column: "email", Comment: "primary contact"},
		},
	}

	err := renderer.VisitAlterTable(alter)

	c.Assert(err, qt.ErrorMatches, `.*MODIFY COLUMN.*"email".*"users".*`)
	c.Assert(renderer.GetOutput(), qt.Not(qt.Contains), "COMMENT")
}
