package typerender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/feature/pgtype/typerender"
	"ptah.run/internal/modelast"
)

func render(target string, payload ast.ExtensionPayload) ([]string, error) {
	return must.Must(typerender.Registry()).Render(renderer.ExtensionContext{Target: target}, ast.StatementExtension, payload)
}

// commonLines renders node with the PostgreSQL-family renderer and returns its
// lines, without blank ones.
func commonLines(c *qt.C, target string, node ast.Node) []string {
	c.Helper()
	sql, err := builtin.RenderSQL(target, node)
	c.Assert(err, qt.IsNil)
	var lines []string
	for line := range strings.SplitSeq(sql, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			lines = append(lines, trimmed)
		}
	}
	return lines
}

func on(kind schemaext.Kind, schema, name string) pgtype.Target {
	return pgtype.Target{Kind: kind, Schema: schema, Name: name}
}

// alterType builds the common ALTER TYPE node of name with operations.
func alterType(name string, operations ...ast.TypeOperation) *ast.AlterTypeNode {
	node := ast.NewAlterType(name)
	for _, operation := range operations {
		node.AddOperation(operation)
	}
	return node
}

// TestRender_MatchesTheCommonRenderer pins that each payload renders the
// statement the PostgreSQL-family renderer writes for the common node that
// expresses the same change, byte for byte: the owner takes over these
// statements without changing one.
func TestRender_MatchesTheCommonRenderer(t *testing.T) {
	tests := []struct {
		name    string
		payload ast.ExtensionPayload
		common  ast.Node
	}{
		{name: "a bare domain",
			payload: &pgtype.CreateDomain{Target: on(pgtype.DomainKind, "", "email"), BaseType: "TEXT"},
			common:  modelast.FromDomain(schemamodel.Domain{Name: "email", BaseType: "TEXT"})},
		{name: "a domain with every clause",
			payload: &pgtype.CreateDomain{Target: on(pgtype.DomainKind, "app", "qty"), BaseType: "integer", NotNull: true, Default: "7",
				Check: "VALUE > 0"},
			common: modelast.FromDomain(schemamodel.Domain{Name: "qty", Schema: "app", BaseType: "integer", NotNull: true, Default: "7",
				Check: "VALUE > 0"})},
		{name: "a quoted literal default",
			payload: &pgtype.CreateDomain{Target: on(pgtype.DomainKind, "", "label"), BaseType: "text", Default: "it's"},
			common:  modelast.FromDomain(schemamodel.Domain{Name: "label", BaseType: "text", Default: "it's"})},
		{name: "a boolean default",
			payload: &pgtype.CreateDomain{Target: on(pgtype.DomainKind, "", "flag"), BaseType: "boolean", Default: "TRUE"},
			common:  modelast.FromDomain(schemamodel.Domain{Name: "flag", BaseType: "boolean", Default: "TRUE"})},
		{name: "a default expression over a literal",
			payload: &pgtype.CreateDomain{Target: on(pgtype.DomainKind, "", "stamp"), BaseType: "timestamptz", Default: "x",
				DefaultExpr: "now()"},
			common: modelast.FromDomain(schemamodel.Domain{Name: "stamp", BaseType: "timestamptz", Default: "x", DefaultExpr: "now()"})},
		{name: "a dotted name",
			payload: &pgtype.CreateDomain{Target: on(pgtype.DomainKind, "app", "a.b"), BaseType: "text"},
			common:  modelast.FromDomain(schemamodel.Domain{Name: "a.b", Schema: "app", BaseType: "text"})},
		{name: "a composite type",
			payload: &pgtype.CreateComposite{Target: on(pgtype.CompositeKind, "geo", "addr"),
				Attributes: []pgtype.Attribute{{Name: "City", Type: "text"}, {Name: "amount", Type: "numeric(10,2)"}}},
			common: modelast.FromCompositeType(schemamodel.CompositeType{Name: "addr", Schema: "geo",
				Fields: []schemamodel.CompositeField{{Name: "City", Type: "text"}, {Name: "amount", Type: "numeric(10,2)"}}})},
		{name: "a removed domain",
			payload: &pgtype.Drop{Target: on(pgtype.DomainKind, "app", "qty"), Cascade: true},
			common:  ast.NewDropType("app.qty").SetDomain().SetIfExists().SetCascade()},
		{name: "a rebuilt composite type",
			payload: &pgtype.Drop{Target: on(pgtype.CompositeKind, "", "addr")},
			common:  ast.NewDropType("addr").SetIfExists()},
		{name: "a domain altered in place",
			payload: &pgtype.AlterDomain{Target: on(pgtype.DomainKind, "app", "qty"), DropChecks: []string{"qty_check", "Other"},
				AddCheck: "VALUE > 1", SetDefault: "'8'", NotNull: new(true)},
			common: alterType("app.qty", ast.NewDropDomainConstraintOperation("qty_check"), ast.NewDropDomainConstraintOperation("Other"),
				ast.NewAddDomainConstraintOperation("VALUE > 1"), ast.NewSetDomainDefaultOperation("'8'"), ast.NewDomainNotNullOperation(true))},
		{name: "a domain losing NOT NULL",
			payload: &pgtype.AlterDomain{Target: on(pgtype.DomainKind, "", "qty"), NotNull: new(false)},
			common:  alterType("qty", ast.NewDomainNotNullOperation(false))},
		{name: "a composite type altered in place",
			payload: &pgtype.AlterComposite{Target: on(pgtype.CompositeKind, "", "addr"), Drop: []string{"zip"},
				Add: []pgtype.Attribute{{Name: "Zip", Type: "varchar(10)"}}},
			common: alterType("addr", ast.NewDropCompositeAttributeOperation("zip"), ast.NewAddCompositeAttributeOperation("Zip", "varchar(10)"))},
		{name: "a domain's comment",
			payload: &pgtype.Comment{Target: on(pgtype.DomainKind, "app", "qty"), Comment: "it's a quantity"},
			common:  ast.NewObjectComment(ast.CommentedDomain, "app.qty", "it's a quantity")},
		{name: "a type's removed comment",
			payload: &pgtype.Comment{Target: on(pgtype.RangeKind, "", "fr")},
			common:  ast.NewObjectComment(ast.CommentedType, "fr", "")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := render("postgres", test.payload)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, commonLines(c, "postgres", test.common))
		})
	}
}

// TestRender_FailurePath pins the two refusals: a payload outside the
// PostgreSQL family, and a payload its own validation refuses.
func TestRender_FailurePath(t *testing.T) {
	t.Run("outside the PostgreSQL family", func(t *testing.T) {
		c := qt.New(t)

		statements, err := render("mysql", &pgtype.Drop{Target: on(pgtype.DomainKind, "", "qty")})

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(statements, qt.IsNil)
	})

	t.Run("an alteration that changes nothing", func(t *testing.T) {
		c := qt.New(t)

		statements, err := render("postgres", &pgtype.AlterDomain{Target: on(pgtype.DomainKind, "", "qty")})

		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(err, qt.ErrorMatches, `.*a domain alteration changes something`)
		c.Assert(statements, qt.IsNil)
	})
}

func TestRenderRangeOptions(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload *pgtype.CreateRange
		want    []string
	}{
		{name: "subtype", payload: &pgtype.CreateRange{Target: on(pgtype.RangeKind, "", "fr"), Subtype: "float8"}, want: []string{`CREATE TYPE "fr" AS RANGE (SUBTYPE = float8);`}},
		{name: "all options", payload: &pgtype.CreateRange{Target: on(pgtype.RangeKind, "app", "tr"), Subtype: "text", SubtypeOpClass: "text_ops", Collation: `"C"`, Canonical: "app.tr_canon", SubtypeDiff: "app.tr_diff"}, want: []string{`CREATE TYPE "app"."tr" AS RANGE (SUBTYPE = text, SUBTYPE_OPCLASS = text_ops, COLLATION = "C", CANONICAL = app.tr_canon, SUBTYPE_DIFF = app.tr_diff);`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := render("postgres", test.payload)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
