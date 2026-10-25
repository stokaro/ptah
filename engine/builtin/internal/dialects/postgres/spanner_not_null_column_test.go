package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin/internal/dialects/postgres"
)

// spannerNotNullComment is the comment a SET NOT NULL with nothing to fill
// the column's NULL rows carries.
const spannerNotNullComment = "-- SPANNER: SET NOT NULL fails if any row of \"t\" holds NULL in \"c\"; " +
	"the column declares no default to fill it with.\n"

// TestRenderer_AddNotNullColumnOnSpanner pins how a column added to an
// existing table reaches Spanner. The emulator behind PGAdapter v0.56.1
// refuses a NOT NULL column without a default in ADD COLUMN, on an empty
// table too, and accepts it added nullable and made NOT NULL next. A default
// fills the rows that exist, so a column that declares one is added as
// declared.
func TestRenderer_AddNotNullColumnOnSpanner(t *testing.T) {
	tests := []struct {
		name   string
		column *ast.ColumnNode
		want   string
	}{
		{
			name:   "NOT NULL, no default",
			column: &ast.ColumnNode{Name: "c", Type: "TEXT"},
			want: "ALTER TABLE \"t\" ADD COLUMN \"c\" TEXT;\n" + spannerNotNullComment +
				"ALTER TABLE \"t\" ALTER COLUMN \"c\" SET NOT NULL;\n",
		},
		{
			name:   "NOT NULL with a CHECK, which Spanner also takes as a statement of its own",
			column: &ast.ColumnNode{Name: "c", Type: "TEXT", Check: "c <> ''"},
			want: "ALTER TABLE \"t\" ADD COLUMN \"c\" TEXT;\n" +
				"ALTER TABLE \"t\" ADD CONSTRAINT \"t_c_check\" CHECK (c <> '');\n" + spannerNotNullComment +
				"ALTER TABLE \"t\" ALTER COLUMN \"c\" SET NOT NULL;\n",
		},
		{
			name:   "NOT NULL with a literal default",
			column: ast.NewColumn("c", "TEXT").SetNotNull().SetDefault("x"),
			want:   "ALTER TABLE \"t\" ADD COLUMN \"c\" TEXT NOT NULL DEFAULT 'x';\n",
		},
		{
			name:   "NOT NULL with a default expression",
			column: ast.NewColumn("c", "TIMESTAMPTZ").SetNotNull().SetDefaultExpression("CURRENT_TIMESTAMP"),
			want:   "ALTER TABLE \"t\" ADD COLUMN \"c\" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP;\n",
		},
		{
			name:   "nullable",
			column: &ast.ColumnNode{Name: "c", Type: "TEXT", Nullable: true},
			want:   "ALTER TABLE \"t\" ADD COLUMN \"c\" TEXT;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			r := postgres.NewWithCapabilities(capability.SpannerPostgres(), platform.Spanner)

			got, err := r.Render(&ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{
				&ast.AddColumnOperation{Column: test.column},
			}})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want+"\n")
		})
	}
}

// TestRenderer_AddNotNullColumnOnPostgres is the control: PostgreSQL adds a
// NOT NULL column without a default in one statement, which succeeds on an
// empty table, so the column stays as declared.
func TestRenderer_AddNotNullColumnOnPostgres(t *testing.T) {
	c := qt.New(t)
	r := postgres.NewWithCapabilities(capability.Postgres17(), platform.Postgres)

	got, err := r.Render(&ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{
		&ast.AddColumnOperation{Column: &ast.ColumnNode{Name: "c", Type: "TEXT"}},
	}})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "ALTER TABLE \"t\" ADD COLUMN \"c\" TEXT NOT NULL;\n\n")
}
