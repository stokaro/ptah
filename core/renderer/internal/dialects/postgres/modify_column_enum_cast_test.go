package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
)

// TestPostgres_ModifyColumn_EnumTargetGetsAUsingCast pins that changing an
// existing column into an enum carries the explicit cast PostgreSQL requires.
//
// PostgreSQL has no assignment cast from varchar to an enum, so a bare
// `ALTER COLUMN ... TYPE <enum>` aborts the migration:
//
//	ERROR: column "s" cannot be cast automatically to type status_kind
//	(SQLSTATE 42804)
//
// measured live on PostgreSQL 17.10 with `ptah schema apply --auto-approve`,
// TRUE_EXIT=2 and the column left as `character varying`.
//
// The clause used to be attached only when the type name began with "enum_", so
// two tables with identical enum values behaved differently on the basis of the
// type's spelling alone: `enum_status` applied and `status_kind` aborted. The
// two rows below are that discriminating pair, with the name as the only
// difference (stokaro/ptah#931 item 1).
func TestPostgres_ModifyColumn_EnumTargetGetsAUsingCast(t *testing.T) {
	tests := []struct {
		name   string
		column *ast.ColumnNode
		want   string
	}{
		{
			name:   "enum whose name has no prefix",
			column: enumColumn("s", "status_kind"),
			want:   `ALTER TABLE "a" ALTER COLUMN "s" TYPE status_kind USING "s"::status_kind;`,
		},
		{
			name:   "enum whose name has the historical prefix",
			column: enumColumn("s", "enum_status"),
			want:   `ALTER TABLE "a" ALTER COLUMN "s" TYPE enum_status USING "s"::enum_status;`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t2 *testing.T) {
			c := qt.New(t2)
			alter := &ast.AlterTableNode{
				Name:       "a",
				Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{Column: test.column}},
			}

			c.Assert(renderPG(t, alter), qt.Contains, test.want)
		})
	}
}

// TestPostgres_ModifyColumn_NonEnumTargetGetsNoUsingCast is the inverse control.
// Without it, "always emit USING" would satisfy the test above while attaching a
// pointless self-cast to every ordinary type change.
func TestPostgres_ModifyColumn_NonEnumTargetGetsNoUsingCast(t *testing.T) {
	c := qt.New(t)

	alter := &ast.AlterTableNode{
		Name: "a",
		Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
			Column: ast.NewColumn("s", "TEXT"),
		}},
	}

	out := renderPG(t, alter)

	c.Assert(out, qt.Contains, `ALTER TABLE "a" ALTER COLUMN "s" TYPE TEXT;`)
	c.Assert(out, qt.Not(qt.Contains), "USING")
}

func enumColumn(name, enumType string) *ast.ColumnNode {
	column := ast.NewColumn(name, enumType)
	column.EnumType = true
	return column
}
