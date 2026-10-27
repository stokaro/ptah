package typerender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
	"ptah.run/feature/pgtype/typerender"
)

func render(target string, payload ast.ExtensionPayload) ([]string, error) {
	return must.Must(typerender.Registry()).Render(renderer.ExtensionContext{Target: target}, ast.StatementExtension, payload)
}

func on(kind schemaext.Kind, schema, name string) pgtype.Target {
	return pgtype.Target{Kind: kind, Schema: schema, Name: name}
}

// TestRender_HappyPath pins the statements each payload renders, byte for
// byte, on PostgreSQL.
func TestRender_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		payload ast.ExtensionPayload
		want    []string
	}{
		{name: "a bare domain",
			payload: &pgtype.CreateDomain{Target: on(pgtype.DomainKind, "", "email"), BaseType: "TEXT"},
			want:    []string{"CREATE DOMAIN \"email\" AS TEXT;"}},
		{name: "a domain with every clause",
			payload: &pgtype.CreateDomain{Target: on(pgtype.DomainKind, "app", "qty"), BaseType: "integer", NotNull: true, Default: "7",
				Check: "VALUE > 0"},
			want: []string{"CREATE DOMAIN \"app\".\"qty\" AS integer NOT NULL DEFAULT 7 CHECK (VALUE > 0);"}},
		{name: "a quoted literal default",
			payload: &pgtype.CreateDomain{Target: on(pgtype.DomainKind, "", "label"), BaseType: "text", Default: "it's"},
			want:    []string{"CREATE DOMAIN \"label\" AS text DEFAULT 'it''s';"}},
		{name: "a boolean default",
			payload: &pgtype.CreateDomain{Target: on(pgtype.DomainKind, "", "flag"), BaseType: "boolean", Default: "TRUE"},
			want:    []string{"CREATE DOMAIN \"flag\" AS boolean DEFAULT true;"}},
		{name: "a default expression over a literal",
			payload: &pgtype.CreateDomain{Target: on(pgtype.DomainKind, "", "stamp"), BaseType: "timestamptz", Default: "x",
				DefaultExpr: "now()"},
			want: []string{"CREATE DOMAIN \"stamp\" AS timestamptz DEFAULT now();"}},
		{name: "a dotted name",
			payload: &pgtype.CreateDomain{Target: on(pgtype.DomainKind, "app", "a.b"), BaseType: "text"},
			want:    []string{"CREATE DOMAIN \"app\".\"a.b\" AS text;"}},
		{name: "a composite type",
			payload: &pgtype.CreateComposite{Target: on(pgtype.CompositeKind, "geo", "addr"),
				Attributes: []pgtype.Attribute{{Name: "City", Type: "text"}, {Name: "amount", Type: "numeric(10,2)"}}},
			want: []string{"CREATE TYPE \"geo\".\"addr\" AS (\"City\" text, \"amount\" numeric(10,2));"}},
		{name: "a removed domain",
			payload: &pgtype.Drop{Target: on(pgtype.DomainKind, "app", "qty"), IfExists: true, Cascade: true},
			want:    []string{"DROP DOMAIN IF EXISTS \"app\".\"qty\" CASCADE;"}},
		{name: "a rebuilt composite type",
			payload: &pgtype.Drop{Target: on(pgtype.CompositeKind, "", "addr"), IfExists: true},
			want:    []string{"DROP TYPE IF EXISTS \"addr\";"}},
		{name: "an exact removed domain",
			payload: &pgtype.Drop{Target: on(pgtype.DomainKind, "app", "qty")},
			want:    []string{"DROP DOMAIN \"app\".\"qty\";"}},
		{name: "a domain altered in place",
			payload: &pgtype.AlterDomain{Target: on(pgtype.DomainKind, "app", "qty"), DropChecks: []string{"qty_check", "Other"},
				AddCheck: "VALUE > 1", SetDefault: "'8'", NotNull: new(true)},
			want: []string{"ALTER DOMAIN \"app\".\"qty\" DROP CONSTRAINT \"qty_check\";", "ALTER DOMAIN \"app\".\"qty\" DROP CONSTRAINT \"Other\";", "ALTER DOMAIN \"app\".\"qty\" ADD CHECK (VALUE > 1);", "ALTER DOMAIN \"app\".\"qty\" SET DEFAULT '8';", "ALTER DOMAIN \"app\".\"qty\" SET NOT NULL;"}},
		{name: "a domain losing NOT NULL",
			payload: &pgtype.AlterDomain{Target: on(pgtype.DomainKind, "", "qty"), NotNull: new(false)},
			want:    []string{"ALTER DOMAIN \"qty\" DROP NOT NULL;"}},
		{name: "a composite type altered in place",
			payload: &pgtype.AlterComposite{Target: on(pgtype.CompositeKind, "", "addr"), Drop: []string{"zip"},
				Add: []pgtype.Attribute{{Name: "Zip", Type: "varchar(10)"}}},
			want: []string{"ALTER TYPE \"addr\" DROP ATTRIBUTE \"zip\";", "ALTER TYPE \"addr\" ADD ATTRIBUTE \"Zip\" varchar(10);"}},
		{name: "a domain's comment",
			payload: &pgtype.Comment{Target: on(pgtype.DomainKind, "app", "qty"), Comment: "it's a quantity"},
			want:    []string{"COMMENT ON DOMAIN \"app\".\"qty\" IS 'it''s a quantity';"}},
		{name: "a type's removed comment",
			payload: &pgtype.Comment{Target: on(pgtype.RangeKind, "", "fr")},
			want:    []string{"COMMENT ON TYPE \"fr\" IS NULL;"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := render("postgres", test.payload)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
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
