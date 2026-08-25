package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer"
)

// A comment transition is a statement of its own on PostgreSQL, outside ALTER
// TABLE -- stokaro/ptah#2168.
func TestVisitAlterTable_SetComment(t *testing.T) {
	tests := []struct {
		name      string
		operation *ast.SetCommentOperation
		want      string
	}{
		{
			name:      "a table's comment",
			operation: &ast.SetCommentOperation{Comment: "customers of record"},
			want:      `COMMENT ON TABLE "public"."users" IS 'customers of record';`,
		},
		{
			name:      "a column's comment",
			operation: &ast.SetCommentOperation{Column: "email", Comment: "primary contact"},
			want:      `COMMENT ON COLUMN "public"."users"."email" IS 'primary contact';`,
		},
		{
			// An empty comment becomes NULL, not ''. PostgreSQL stores them
			// alike, but the catalog reports NULL for an object with no
			// comment, so writing '' would leave something the reader brings
			// back as absent and the comparison plans again on every run.
			name:      "a table's comment removed",
			operation: &ast.SetCommentOperation{},
			want:      `COMMENT ON TABLE "public"."users" IS NULL;`,
		},
		{
			name:      "a column's comment removed",
			operation: &ast.SetCommentOperation{Column: "email"},
			want:      `COMMENT ON COLUMN "public"."users"."email" IS NULL;`,
		},
		{
			// An apostrophe is the character a comment is most likely to hold,
			// and an unescaped one ends the literal and breaks the statement.
			name:      "a comment holding an apostrophe",
			operation: &ast.SetCommentOperation{Comment: "the buyer's account"},
			want:      `COMMENT ON TABLE "public"."users" IS 'the buyer''s account';`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			alter := &ast.AlterTableNode{
				Name:       "public.users",
				Operations: []ast.AlterOperation{tt.operation},
			}

			sql, err := renderer.RenderSQL("postgres", alter)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, tt.want)
		})
	}
}
