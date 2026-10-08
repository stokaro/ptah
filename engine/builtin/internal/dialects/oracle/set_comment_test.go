package oracle_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// A comment transition is a statement of its own on Oracle, the way PostgreSQL
// writes one -- stokaro/ptah#2168.
//
// Oracle has no inline comment clause anywhere, so a column's comment cannot
// travel with the MODIFY that carries its other changes. That is the difference
// from MySQL and MariaDB, which have one and where a separate statement does
// not exist.
func TestAlterTable_SetComment(t *testing.T) {
	tests := []struct {
		name      string
		operation *ast.SetCommentOperation
		want      string
	}{
		{
			name:      "a table's comment",
			operation: &ast.SetCommentOperation{Comment: "customers of record"},
			want:      "COMMENT ON TABLE USERS IS 'customers of record';",
		},
		{
			name:      "a column's comment",
			operation: &ast.SetCommentOperation{Column: "EMAIL", Comment: "primary contact"},
			want:      "COMMENT ON COLUMN USERS.EMAIL IS 'primary contact';",
		},
		{
			// Oracle has no empty string: '' IS NULL there, so the empty
			// literal clears the comment and the catalog reports NULL
			// afterwards -- which is the absence the reader brings back.
			// `IS NULL` is a syntax error on this statement, which is why the
			// PostgreSQL spelling cannot be reused.
			name:      "a table's comment removed",
			operation: &ast.SetCommentOperation{},
			want:      "COMMENT ON TABLE USERS IS '';",
		},
		{
			name:      "a column's comment removed",
			operation: &ast.SetCommentOperation{Column: "EMAIL"},
			want:      "COMMENT ON COLUMN USERS.EMAIL IS '';",
		},
		{
			// An apostrophe is the character a comment is most likely to hold,
			// and an unescaped one ends the literal and breaks the statement.
			name:      "a comment holding an apostrophe",
			operation: &ast.SetCommentOperation{Comment: "the buyer's account"},
			want:      "COMMENT ON TABLE USERS IS 'the buyer''s account';",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			sql := render(c, capability.ForDialect(platform.Oracle), &ast.AlterTableNode{
				Name:       "USERS",
				Operations: []ast.AlterOperation{tt.operation},
			})

			c.Assert(sql, qt.Contains, tt.want)
		})
	}
}
