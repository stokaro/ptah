package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
)

func alterTable(operations ...ast.AlterOperation) ast.Node {
	return &ast.AlterTableNode{Name: "users", Operations: operations}
}

// Each ALTER COLUMN action renders as the one clause it names, and DROP COLUMN
// keeps its IF EXISTS.
func TestRenderAlterColumnOperation_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		operation ast.AlterOperation
		wantSQL   string
	}{
		{
			// The operation carries no column type, so a literal is written
			// the way an untyped default is: quoted, which PostgreSQL casts.
			name:      "SET DEFAULT a literal",
			operation: &ast.AlterColumnOperation{ColumnName: "n", Action: ast.AlterColumnSetDefault, Default: &ast.DefaultValue{Value: "5", ValueSet: true}},
			wantSQL:   `ALTER TABLE "users" ALTER COLUMN "n" SET DEFAULT '5';`,
		},
		{
			name:      "SET DEFAULT an expression",
			operation: &ast.AlterColumnOperation{ColumnName: "at", Action: ast.AlterColumnSetDefault, Default: &ast.DefaultValue{Expression: "now()"}},
			wantSQL:   `ALTER TABLE "users" ALTER COLUMN "at" SET DEFAULT now();`,
		},
		{
			name:      "DROP DEFAULT",
			operation: &ast.AlterColumnOperation{ColumnName: "n", Action: ast.AlterColumnDropDefault},
			wantSQL:   `ALTER TABLE "users" ALTER COLUMN "n" DROP DEFAULT;`,
		},
		{
			name:      "SET NOT NULL",
			operation: &ast.AlterColumnOperation{ColumnName: "n", Action: ast.AlterColumnSetNotNull},
			wantSQL:   `ALTER TABLE "users" ALTER COLUMN "n" SET NOT NULL;`,
		},
		{
			name:      "DROP NOT NULL",
			operation: &ast.AlterColumnOperation{ColumnName: "n", Action: ast.AlterColumnDropNotNull},
			wantSQL:   `ALTER TABLE "users" ALTER COLUMN "n" DROP NOT NULL;`,
		},
		{
			name:      "TYPE with USING",
			operation: &ast.AlterColumnOperation{ColumnName: "n", Action: ast.AlterColumnSetType, Type: "bigint", Using: "n::bigint"},
			wantSQL:   `ALTER TABLE "users" ALTER COLUMN "n" TYPE bigint USING n::bigint;`,
		},
		{
			name:      "DROP COLUMN IF EXISTS",
			operation: &ast.DropColumnOperation{ColumnName: "n", IfExists: true},
			wantSQL:   `ALTER TABLE "users" DROP COLUMN IF EXISTS "n";`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL("postgres", alterTable(test.operation))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.wantSQL)
		})
	}
}

// An action with nothing to set is refused rather than rendered as something
// else.
func TestRenderAlterColumnOperation_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		operation *ast.AlterColumnOperation
		wantErr   string
	}{
		{name: "SET DEFAULT without a default", operation: &ast.AlterColumnOperation{ColumnName: "n", Action: ast.AlterColumnSetDefault}, wantErr: `.*ALTER COLUMN n: SET DEFAULT carries no default`},
		{name: "TYPE without a type", operation: &ast.AlterColumnOperation{ColumnName: "n", Action: ast.AlterColumnSetType}, wantErr: `.*ALTER COLUMN n: SET DATA TYPE carries no type`},
		{name: "an unknown action", operation: &ast.AlterColumnOperation{ColumnName: "n", Action: "SET STORAGE"}, wantErr: `.*ALTER COLUMN n: unknown action "SET STORAGE"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL("postgres", alterTable(test.operation))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// A dialect whose renderer does not write DROP COLUMN IF EXISTS refuses the
// operation rather than drop the guard, and so does one that does not render
// the ALTER COLUMN operation.
func TestRenderColumnGuards_UnsupportedDialect(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb", "sqlite", "sqlserver", "clickhouse", "oracle", "spanner"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(dialect, alterTable(&ast.DropColumnOperation{ColumnName: "n", IfExists: true}))

			c.Assert(err, qt.ErrorMatches, `.*does not render DROP COLUMN IF EXISTS`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
