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
		{
			name: "a function, named by its arguments",
			caps: capability.Postgres18(),
			node: routineComment(ast.CommentedFunction, "app.f", new("a integer, b text")),
			want: "COMMENT ON FUNCTION \"app\".\"f\"(a integer, b text) IS 'x';\n",
		},
		{
			name: "a procedure that takes no arguments",
			caps: capability.Postgres18(),
			node: routineComment(ast.CommentedProcedure, "app.p", new("")),
			want: "COMMENT ON PROCEDURE \"app\".\"p\"() IS 'x';\n",
		},
		{
			// A server resolves the bare name when it has one overload.
			name: "a routine whose arguments nobody recorded",
			caps: capability.Postgres18(),
			node: routineComment(ast.CommentedFunction, "app.f", nil),
			want: "COMMENT ON FUNCTION \"app\".\"f\" IS 'x';\n",
		},
		{
			name: "a materialized view",
			caps: capability.Postgres18(),
			node: ast.NewObjectComment(ast.CommentedMaterializedView, "app.m", "x"),
			want: "COMMENT ON MATERIALIZED VIEW \"app\".\"m\" IS 'x';\n",
		},
		{
			name: "a trigger, on its table",
			caps: capability.Postgres18(),
			node: ast.NewObjectComment(ast.CommentedTrigger, "tg", "x").SetTable("app.t"),
			want: "COMMENT ON TRIGGER \"tg\" ON \"app\".\"t\" IS 'x';\n",
		},
		{
			name: "a policy's comment removed",
			caps: capability.Postgres18(),
			node: ast.NewObjectComment(ast.CommentedPolicy, "pol", "").SetTable("app.t"),
			want: "COMMENT ON POLICY \"pol\" ON \"app\".\"t\" IS NULL;\n",
		},
		{
			name: "a function on CockroachDB 26.3, which stores it",
			caps: capability.CockroachDB263(),
			node: routineComment(ast.CommentedFunction, "app.f", new("a integer")),
			want: "COMMENT ON FUNCTION \"app\".\"f\"(a integer) IS 'x';\n",
		},
		{
			name: "a trigger on CockroachDB 26.3, which refuses the statement",
			caps: capability.CockroachDB263(),
			node: ast.NewObjectComment(ast.CommentedTrigger, "tg", "x").SetTable("app.t"),
			want: "-- POSTGRES: trigger comment tg on app.t is not supported by this target; skipped.\n",
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

// routineComment is a comment of x on the routine name, addressed by
// arguments.
func routineComment(object ast.CommentedObject, name string, arguments *string) *ast.ObjectCommentNode {
	node := ast.NewObjectComment(object, name, "x")
	node.Arguments = arguments
	return node
}

// A trigger or a policy is named within its table, and COMMENT ON without one
// would address an object that cannot exist.
func TestPostgreSQLRenderer_ScopedObjectComment_FailurePath(t *testing.T) {
	for _, object := range []ast.CommentedObject{ast.CommentedTrigger, ast.CommentedPolicy} {
		t.Run(string(object), func(t *testing.T) {
			c := qt.New(t)

			sql, err := postgres.New().Render(ast.NewObjectComment(object, "x", "note"))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*COMMENT ON `+string(object)+` x names no table.*`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// A kind no key covers is refused rather than written with a keyword the
// renderer never checked.
func TestPostgreSQLRenderer_ObjectComment_FailurePath(t *testing.T) {
	c := qt.New(t)

	sql, err := postgres.New().Render(ast.NewObjectComment("RULE", "app.m", "x"))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*COMMENT ON "RULE" names no object kind this renderer comments`)
	c.Assert(sql, qt.Equals, "")
}

// A create node's comment is the object's own and follows the CREATE.
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
			want: "CREATE TYPE \"e\" AS ENUM ('a');\nCOMMENT ON TYPE \"e\" IS 'note';\n",
		},
		{
			name: "an enum node",
			caps: capability.Postgres18(),
			node: commentedEnum("app.e", "note"),
			want: "CREATE TYPE \"app\".\"e\" AS ENUM ('a');\nCOMMENT ON TYPE \"app\".\"e\" IS 'note';\n",
		},
		{
			// COMMENT ON refuses a default, so the statement names the
			// overload the CREATE wrote by its identity alone.
			name: "a function with a parameter default",
			caps: capability.Postgres18(),
			node: ast.NewCreateFunction("app.f").SetParameters("a integer, b text DEFAULT 'x'").SetReturns("integer").
				SetLanguage("sql").SetBody("SELECT 1").SetComment("note"),
			want: "CREATE OR REPLACE FUNCTION \"app\".\"f\"(a integer, b text DEFAULT 'x') RETURNS integer AS $$\nSELECT 1\n$$\n" +
				"LANGUAGE sql;\nCOMMENT ON FUNCTION \"app\".\"f\"(a integer, b text) IS 'note';\n",
		},
		{
			name: "a procedure",
			caps: capability.Postgres18(),
			node: ast.NewCreateFunction("app.p").SetKind("PROCEDURE").SetParameters("INOUT a integer").
				SetLanguage("sql").SetBody("SELECT 1").SetComment("note"),
			want: "CREATE OR REPLACE PROCEDURE \"app\".\"p\"(INOUT a integer) AS $$\nSELECT 1\n$$\n" +
				"LANGUAGE sql;\nCOMMENT ON PROCEDURE \"app\".\"p\"(INOUT a integer) IS 'note';\n",
		},
		{
			name: "a materialized view",
			caps: capability.Postgres18(),
			node: ast.NewCreateMaterializedView("app.m").SetBody("SELECT 1").SetComment("note"),
			want: "CREATE MATERIALIZED VIEW \"app\".\"m\" AS\nSELECT 1\n;\nCOMMENT ON MATERIALIZED VIEW \"app\".\"m\" IS 'note';\n",
		},
		{
			name: "a trigger",
			caps: capability.Postgres18(),
			node: ast.NewCreateTrigger("tg", "app.t").SetTiming("BEFORE").SetEvent("INSERT").SetForEach("ROW").
				SetFunctionName("app.touch").SetExternalFunction().SetComment("note"),
			want: "CREATE TRIGGER \"tg\" BEFORE INSERT ON \"app\".\"t\" FOR EACH ROW EXECUTE FUNCTION \"app\".\"touch\"();\n" +
				"COMMENT ON TRIGGER \"tg\" ON \"app\".\"t\" IS 'note';\n",
		},
		{
			name: "a policy",
			caps: capability.Postgres18(),
			node: ast.NewCreatePolicy("pol", "app.t").SetUsingExpression("true").SetComment("note"),
			want: "CREATE POLICY \"pol\" ON \"app\".\"t\"\n    USING (true)\n;\nCOMMENT ON POLICY \"pol\" ON \"app\".\"t\" IS 'note';\n",
		},
		{
			name: "a policy on CockroachDB 26.3, which refuses the statement",
			caps: capability.CockroachDB263(),
			node: ast.NewCreatePolicy("pol", "app.t").SetUsingExpression("true").SetComment("note"),
			want: "CREATE POLICY \"pol\" ON \"app\".\"t\"\n    USING (true)\n;\n" +
				"-- POSTGRES: policy comment pol on app.t is not supported by this target; skipped.\n",
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

// commentedEnum is an enum node with one value and comment.
func commentedEnum(name, comment string) *ast.EnumNode {
	node := ast.NewEnum(name, "a")
	node.Comment = comment
	return node
}
