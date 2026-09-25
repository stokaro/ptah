package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/postgres"
)

// An object comment is written where the target stores the kind's comment and
// named as skipped where it does not, each kind decided by its own key: the
// engines take different subsets of these statements (stokaro/ptah#3627).
func TestPostgreSQLRenderer_ObjectComment_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node *ast.ObjectCommentNode
		want string
	}{
		{
			name: "a view",
			caps: capability.Postgres18(),
			node: ast.NewObjectComment(ast.CommentedView, "app.v", "it's here"),
			want: "COMMENT ON VIEW \"app\".\"v\" IS 'it''s here';\n",
		},
		{
			name: "a comment removed",
			caps: capability.Postgres18(),
			node: ast.NewObjectComment(ast.CommentedDomain, "app.d", ""),
			want: "COMMENT ON DOMAIN \"app\".\"d\" IS NULL;\n",
		},
		{
			name: "an extension, whose name is never split on a dot",
			caps: capability.Postgres18(),
			node: ast.NewObjectComment(ast.CommentedExtension, "odd.name", "x"),
			want: "COMMENT ON EXTENSION \"odd.name\" IS 'x';\n",
		},
		{
			name: "a target without the key",
			caps: capability.CockroachDB25(),
			node: ast.NewObjectComment(ast.CommentedSequence, "app.s", "x"),
			want: "-- POSTGRES: sequence comment app.s is not supported by this target; skipped.\n",
		},
		{
			name: "a target with one key and not another",
			caps: capability.CockroachDB263(),
			node: ast.NewObjectComment(ast.CommentedSequence, "app.s", "x"),
			want: "COMMENT ON SEQUENCE \"app\".\"s\" IS 'x';\n",
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

// A kind no key covers is refused rather than written with a keyword the
// renderer never checked.
func TestPostgreSQLRenderer_ObjectComment_FailurePath(t *testing.T) {
	c := qt.New(t)

	sql, err := postgres.New().Render(ast.NewObjectComment("MATERIALIZED VIEW", "app.m", "x"))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*COMMENT ON "MATERIALIZED VIEW" names no object kind this renderer comments`)
	c.Assert(sql, qt.Equals, "")
}

// A create node's comment is the object's own and follows the CREATE, except
// on an enum, which the model keeps no comment for and whose node comment
// stays a note in the script.
func TestPostgreSQLRenderer_CreatedObjectComment(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "a view",
			caps: capability.Postgres18(),
			node: ast.NewCreateView("v").SetBody("SELECT 1").SetComment("note"),
			want: "CREATE VIEW \"v\" AS\nSELECT 1\n;\nCOMMENT ON VIEW \"v\" IS 'note';\n",
		},
		{
			name: "a sequence",
			caps: capability.Postgres18(),
			node: ast.NewCreateSequence("s").SetComment("note"),
			want: "CREATE SEQUENCE \"s\";\nCOMMENT ON SEQUENCE \"s\" IS 'note';\n",
		},
		{
			// The start-counter grammar returns early, and the comment has to
			// be written on that path too.
			name: "a sequence on a start-counter-only target",
			caps: capability.Postgres18().With(capability.SequenceStartCounterOnly, true),
			node: ast.NewCreateSequence("s").SetComment("note"),
			want: "CREATE SEQUENCE \"s\";\nCOMMENT ON SEQUENCE \"s\" IS 'note';\n",
		},
		{
			name: "a domain",
			caps: capability.Postgres18(),
			node: ast.NewCreateType("d", &ast.DomainTypeDef{BaseType: "integer", Nullable: true}).SetComment("note"),
			want: "CREATE DOMAIN \"d\" AS integer;\nCOMMENT ON DOMAIN \"d\" IS 'note';\n",
		},
		{
			name: "a composite type",
			caps: capability.Postgres18(),
			node: ast.NewCreateType("c", &ast.CompositeTypeDef{Fields: []*ast.CompositeField{{Name: "n", Type: "integer"}}}).
				SetComment("note"),
			want: "CREATE TYPE \"c\" AS (\"n\" integer);\nCOMMENT ON TYPE \"c\" IS 'note';\n",
		},
		{
			name: "a range type",
			caps: capability.Postgres18(),
			node: ast.NewCreateType("r", &ast.RangeTypeDef{Subtype: "integer"}).SetComment("note"),
			want: "CREATE TYPE \"r\" AS RANGE (SUBTYPE = integer);\nCOMMENT ON TYPE \"r\" IS 'note';\n",
		},
		{
			name: "an enum",
			caps: capability.Postgres18(),
			node: ast.NewCreateType("e", &ast.EnumTypeDef{Values: []string{"a"}}).SetComment("note"),
			want: "-- note\nCREATE TYPE \"e\" AS ENUM ('a');\n",
		},
		{
			name: "a target that does not store the kind's comment",
			caps: capability.CockroachDB263(),
			node: ast.NewCreateType("c", &ast.CompositeTypeDef{Fields: []*ast.CompositeField{{Name: "n", Type: "integer"}}}).
				SetComment("note"),
			want: "CREATE TYPE \"c\" AS (\"n\" integer);\n-- POSTGRES: type comment c is not supported by this target; skipped.\n",
		},
		{
			name: "no comment",
			caps: capability.Postgres18(),
			node: ast.NewCreateView("v").SetBody("SELECT 1"),
			want: "CREATE VIEW \"v\" AS\nSELECT 1\n;\n",
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
