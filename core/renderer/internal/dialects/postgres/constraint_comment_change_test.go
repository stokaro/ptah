package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer/internal/dialects/postgres"
)

// alterStatements is the header the renderer writes above an ALTER TABLE's
// statements.
const alterStatements = "-- ALTER statements: --\n"

// setConstraintComment is an ALTER on app.orders that sets the comment of its
// constraint orders_total_positive to comment.
func setConstraintComment(comment string) *ast.AlterTableNode {
	return &ast.AlterTableNode{
		Name: "app.orders",
		Operations: []ast.AlterOperation{&ast.SetConstraintCommentOperation{
			Constraint: "orders_total_positive", Comment: comment,
		}},
	}
}

// commentedConstraintAddition is an ALTER on app.orders that adds a CHECK
// carrying a comment.
func commentedConstraintAddition() *ast.AlterTableNode {
	return &ast.AlterTableNode{
		Name: "app.orders",
		Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{
			Type: ast.CheckConstraint, Name: "orders_total_positive", Expression: "total > 0", Comment: "note",
		}}},
	}
}

// A changed constraint comment is one COMMENT ON CONSTRAINT on the table the
// ALTER names, and a removed one sets it to NULL (stokaro/ptah#3678). A target
// that does not store a constraint's comment gets the named skip rather than a
// statement it refuses: the Spanner interface answers `Unknown statement`.
func TestPostgreSQLRenderer_ConstraintComment_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "a comment changed",
			caps: capability.Postgres18(),
			node: setConstraintComment("it's positive"),
			want: alterStatements + "COMMENT ON CONSTRAINT \"orders_total_positive\" ON \"app\".\"orders\" IS 'it''s positive';\n\n",
		},
		{
			name: "a comment removed",
			caps: capability.Postgres18(),
			node: setConstraintComment(""),
			want: alterStatements + "COMMENT ON CONSTRAINT \"orders_total_positive\" ON \"app\".\"orders\" IS NULL;\n\n",
		},
		{
			name: "a comment changed on CockroachDB 25.4",
			caps: capability.CockroachDB25(),
			node: setConstraintComment("note"),
			want: alterStatements + "COMMENT ON CONSTRAINT \"orders_total_positive\" ON \"app\".\"orders\" IS 'note';\n\n",
		},
		{
			name: "a comment changed on the Spanner interface",
			caps: capability.SpannerPostgres(),
			node: setConstraintComment("note"),
			want: alterStatements + "-- POSTGRES: constraint comment orders_total_positive on app.orders is not supported by this target; skipped.\n\n",
		},
		{
			name: "a commented constraint added on the Spanner interface",
			caps: capability.SpannerPostgres(),
			node: commentedConstraintAddition(),
			want: alterStatements + "ALTER TABLE \"app\".\"orders\" ADD CONSTRAINT \"orders_total_positive\" CHECK (total > 0);\n" +
				"-- POSTGRES: constraint comment orders_total_positive on app.orders is not supported by this target; skipped.\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := postgres.NewWithCapabilities(test.caps, platform.Postgres).Render(test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}
