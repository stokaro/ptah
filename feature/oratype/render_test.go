package oratype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/feature/oratype"
)

// render writes one operation with the handlers on Oracle under caps.
func render(c *qt.C, caps capability.Capabilities, payload ast.ExtensionPayload) ([]string, error) {
	c.Helper()
	registry := must.Must(renderer.NewExtensions(oratype.Handlers()...))
	return registry.Render(renderer.ExtensionContext{Target: platform.Oracle, Capabilities: caps}, ast.StatementExtension, payload)
}

// domain and objectType name a type that names no schema.
func domain(name string) oratype.Target { return oratype.Target{Kind: oratype.DomainKind, Name: name} }

func objectType(name string) oratype.Target {
	return oratype.Target{Kind: oratype.ObjectTypeKind, Name: name}
}

// TestHandlers_RenderEachStatement pins each statement's spelling: the clause
// order, the default as an expression or a literal, the comment line before a
// creation, the guard the target's existence guards decide, and each name
// part bare where Oracle takes it bare and quoted otherwise.
func TestHandlers_RenderEachStatement(t *testing.T) {
	guarded, bare := capability.Oracle23(), capability.Oracle23().With(capability.ObjectExistenceGuards, false)
	tests := []struct {
		name    string
		caps    capability.Capabilities
		payload ast.ExtensionPayload
		want    []string
	}{
		{name: "a domain with every clause", caps: guarded, payload: &oratype.CreateDomain{Target: domain("email_d"), BaseType: "VARCHAR2(255)",
			NotNull: true, Default: "it's", Check: "VALUE <> 'x'", Comment: "mail"},
			want: []string{"-- mail", "CREATE DOMAIN IF NOT EXISTS email_d AS VARCHAR2(255) NOT NULL DEFAULT 'it''s' CHECK (VALUE <> 'x');"}},
		{name: "a default expression beside a literal", caps: guarded, payload: &oratype.CreateDomain{Target: domain("d"), BaseType: "DATE",
			Default: "ignored", DefaultExpr: "SYSDATE"},
			want: []string{"CREATE DOMAIN IF NOT EXISTS d AS DATE DEFAULT SYSDATE;"}},
		{name: "a literal that is already quoted", caps: guarded, payload: &oratype.CreateDomain{Target: domain("d"), BaseType: "CHAR(1)", Default: "'y'"},
			want: []string{"CREATE DOMAIN IF NOT EXISTS d AS CHAR(1) DEFAULT 'y';"}},
		{name: "a literal number", caps: guarded, payload: &oratype.CreateDomain{Target: domain("d"), BaseType: "NUMBER", Default: "7"},
			want: []string{"CREATE DOMAIN IF NOT EXISTS d AS NUMBER DEFAULT '7';"}},
		{name: "a target without existence guards", caps: bare, payload: &oratype.CreateDomain{Target: domain("d"), BaseType: "NUMBER"},
			want: []string{"CREATE DOMAIN d AS NUMBER;"}},
		{name: "a schema and a name that need quotes", caps: guarded,
			payload: &oratype.CreateDomain{Target: oratype.Target{Kind: oratype.DomainKind, Schema: "app", Name: "my domain"}, BaseType: "NUMBER"},
			want:    []string{`CREATE DOMAIN IF NOT EXISTS app."my domain" AS NUMBER;`}},
		{name: "an object type", caps: guarded, payload: &oratype.ReplaceObjectType{Target: objectType("addr_t"),
			Attributes: []oratype.Attribute{{Name: "street", Type: " VARCHAR2(80) "}, {Name: "level", Type: "NUMBER(10,2)"}}, Comment: "address"},
			want: []string{"-- address", `CREATE OR REPLACE TYPE addr_t AS OBJECT (street VARCHAR2(80), "level" NUMBER(10,2));`}},
		{name: "a domain drop", caps: guarded, payload: &oratype.Drop{Target: domain("d")}, want: []string{"DROP DOMAIN d;"}},
		{name: "an object type drop", caps: guarded, payload: &oratype.Drop{Target: objectType("t")}, want: []string{"DROP TYPE t;"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := render(c, test.caps, test.payload)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// TestHandlers_FailurePath pins what the handlers refuse: another target, an
// invalid payload, and a statement the target's capabilities do not hold,
// which is refused rather than skipped, since a skipped domain leaves its
// columns naming a type the server has no definition of.
func TestHandlers_FailurePath(t *testing.T) {
	registry := must.Must(renderer.NewExtensions(oratype.Handlers()...))
	tests := []struct {
		name    string
		target  string
		caps    capability.Capabilities
		payload ast.ExtensionPayload
		wantIs  error
		want    string
	}{
		{name: "another target", target: platform.Postgres, caps: capability.Postgres18(),
			payload: &oratype.CreateDomain{Target: domain("d"), BaseType: "NUMBER"}, wantIs: ptaherr.ErrUnsupportedFeature,
			want: `.*target "postgres" does not support extension "ptah.run/oratype/create-domain".*`},
		{name: "a domain without a base type", target: platform.Oracle, caps: capability.Oracle23(),
			payload: &oratype.CreateDomain{Target: domain("d")}, wantIs: ptaherr.ErrInvalidSchemaDiff, want: `.*a domain creation names a base type.*`},
		{name: "an object type without attributes", target: platform.Oracle, caps: capability.Oracle23(),
			payload: &oratype.ReplaceObjectType{Target: objectType("t")}, wantIs: ptaherr.ErrInvalidSchemaDiff,
			want: `.*an object type requires at least one attribute.*`},
		{name: "a domain on Oracle 21", target: platform.Oracle, caps: capability.Oracle21(),
			payload: &oratype.CreateDomain{Target: domain("d"), BaseType: "NUMBER"}, wantIs: ptaherr.ErrUnsupportedFeature,
			want: `unsupported feature: oracle: CREATE DOMAIN d: user types are not rendered for Oracle`},
		{name: "a domain drop on Oracle 21", target: platform.Oracle, caps: capability.Oracle21(),
			payload: &oratype.Drop{Target: domain("d")}, wantIs: ptaherr.ErrUnsupportedFeature,
			want: `unsupported feature: oracle: DROP DOMAIN d: user types are not rendered for Oracle`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := registry.Render(renderer.ExtensionContext{Target: test.target, Capabilities: test.caps},
				ast.StatementExtension, test.payload)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(statements, qt.IsNil)
		})
	}
}
