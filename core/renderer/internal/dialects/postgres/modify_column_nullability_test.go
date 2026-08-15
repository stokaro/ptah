package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
)

// TestPostgres_ModifyColumn_KeyColumnKeepsNotNull pins the renderer itself,
// below the preparation and validation layers that core/renderer wraps around
// it, because this is where the regression lived.
//
// The CREATE TABLE path writes PRIMARY KEY and NOT NULL together and never
// reads ColumnNode.Nullable for a key column. This ALTER path did read it, and
// when SetPrimary() stopped clearing the flag so that SQLite could answer the
// question for itself (stokaro/ptah#1235), it began planning
// `ALTER COLUMN "id" DROP NOT NULL` for a key column. PostgreSQL refuses that
// outright -- `column "id" is in a primary key`, SQLSTATE 42P16 -- so the whole
// apply failed rather than merely reading oddly. Measured live on PostgreSQL
// 17.10 through both `ptah-compat schema apply` and `ptah schema apply`.
func TestPostgres_ModifyColumn_KeyColumnKeepsNotNull(t *testing.T) {
	tests := []struct {
		name   string
		column *ast.ColumnNode
		want   string
		absent string
	}{
		{
			name:   "nullable key column is still set not null",
			column: ast.NewColumn("id", "BIGINT").SetPrimary(),
			want:   "ALTER TABLE users ALTER COLUMN id SET NOT NULL;",
			absent: "DROP NOT NULL",
		},
		{
			name:   "key column declared not null is unchanged",
			column: ast.NewColumn("id", "BIGINT").SetPrimary().SetNotNull(),
			want:   "ALTER TABLE users ALTER COLUMN id SET NOT NULL;",
			absent: "DROP NOT NULL",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			alter := &ast.AlterTableNode{
				Name:       "users",
				Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{Column: test.column}},
			}
			out := legacyPostgresSQL(renderPG(t, alter))
			c.Assert(out, qt.Contains, test.want)
			c.Assert(out, qt.Not(qt.Contains), test.absent)
		})
	}
}

// TestPostgres_ModifyColumn_OrdinaryColumnStillDropsNotNull is the other half of
// the guard. Without it, "never emit DROP NOT NULL" would be satisfied by a
// renderer that emits SET NOT NULL for every column, which would silently
// re-introduce a constraint the author removed.
func TestPostgres_ModifyColumn_OrdinaryColumnStillDropsNotNull(t *testing.T) {
	tests := []struct {
		name   string
		column *ast.ColumnNode
		want   string
		absent string
	}{
		{
			name:   "nullable non-key column drops not null",
			column: ast.NewColumn("nickname", "TEXT"),
			want:   "ALTER TABLE users ALTER COLUMN nickname DROP NOT NULL;",
			absent: "SET NOT NULL",
		},
		{
			name:   "nullable unique non-key column drops not null",
			column: ast.NewColumn("nickname", "TEXT").SetUnique(),
			want:   "ALTER TABLE users ALTER COLUMN nickname DROP NOT NULL;",
			absent: "SET NOT NULL",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			alter := &ast.AlterTableNode{
				Name:       "users",
				Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{Column: test.column}},
			}
			out := legacyPostgresSQL(renderPG(t, alter))
			c.Assert(out, qt.Contains, test.want)
			c.Assert(out, qt.Not(qt.Contains), test.absent)
		})
	}
}
