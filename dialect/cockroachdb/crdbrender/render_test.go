package crdbrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/cockroachdb/crdbast"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbrender"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

func added() *crdbast.AlterRowTTL {
	return &crdbast.AlterRowTTL{Change: crdbdiff.RowTTL{After: &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "expires_at"}}}}
}

// TestRegistry_QuotesTheTableAsThePostgreSQLFamilyDoes pins the table spelling
// the owner writes: every part quoted, a quoted part kept whole, and an
// embedded quote doubled.
func TestRegistry_QuotesTheTableAsThePostgreSQLFamilyDoes(t *testing.T) {
	tests := []struct {
		table string
		want  string
	}{
		{table: "sessions", want: `ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at');`},
		{table: "audit.sessions", want: `ALTER TABLE "audit"."sessions" SET (ttl_expiration_expression = 'expires_at');`},
		{table: `"a.b"."c"`, want: `ALTER TABLE "a.b"."c" SET (ttl_expiration_expression = 'expires_at');`},
		{table: `odd"name`, want: `ALTER TABLE "odd""name" SET (ttl_expiration_expression = 'expires_at');`},
		{table: "Größe", want: `ALTER TABLE "Größe" SET (ttl_expiration_expression = 'expires_at');`},
	}
	for _, test := range tests {
		t.Run(test.table, func(t *testing.T) {
			c := qt.New(t)
			registry := must.Must(crdbrender.Registry())
			statements, err := registry.Render(renderer.ExtensionContext{
				Target: "cockroachdb", Capabilities: capability.CockroachDB26(), Parent: &ast.AlterTableNode{Name: test.table},
			}, ast.AlterExtension, added())
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{test.want})
		})
	}
}

func TestRegistry_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		ctx     renderer.ExtensionContext
		role    ast.ExtensionRole
		wantErr error
	}{
		{name: "another target", ctx: renderer.ExtensionContext{Target: "postgres", Capabilities: capability.Postgres17(), Parent: &ast.AlterTableNode{Name: "t"}},
			role: ast.AlterExtension, wantErr: ptaherr.ErrUnsupportedDialect},
		{name: "a capability set without the key", ctx: renderer.ExtensionContext{Target: "cockroachdb", Capabilities: capability.CockroachDB26().With(capability.RowLevelTTL, false),
			Parent: &ast.AlterTableNode{Name: "t"}}, role: ast.AlterExtension, wantErr: ptaherr.ErrUnsupportedFeature},
		{name: "no parent", ctx: renderer.ExtensionContext{Target: "cockroachdb", Capabilities: capability.CockroachDB26()},
			role: ast.AlterExtension, wantErr: ptaherr.ErrInvalidSchemaDiff},
		{name: "a standalone statement", ctx: renderer.ExtensionContext{Target: "cockroachdb", Capabilities: capability.CockroachDB26()},
			role: ast.StatementExtension, wantErr: ptaherr.ErrUnsupportedFeature},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := must.Must(crdbrender.Registry()).Render(test.ctx, test.role, added())
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestCreateTableClause_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		facets schemaext.Facets
		want   string
	}{
		{name: "a table without a policy", want: ""},
		{
			name:   "a policy",
			facets: must.Must(schemaext.NewFacets(&crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpireAfter: "3 days", Pause: true}})),
			want:   " WITH (ttl_expire_after = '3 days', ttl_pause = true)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			clause, err := crdbrender.CreateTableClause("cockroachdb", capability.CockroachDB26(), "t", test.facets)
			c.Assert(err, qt.IsNil)
			c.Assert(clause, qt.Equals, test.want)
		})
	}
}

func TestCreateTableClause_FailurePath(t *testing.T) {
	policy := must.Must(schemaext.NewFacets(&crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpireAfter: "3 days"}}))
	tests := []struct {
		name    string
		target  string
		caps    capability.Capabilities
		facets  schemaext.Facets
		wantErr error
	}{
		{name: "another owner's facet", target: "cockroachdb", caps: capability.CockroachDB26(),
			facets: must.Must(schemaext.NewFacets(&chschema.DesiredTable{})), wantErr: ptaherr.ErrUnsupportedFeature},
		{name: "an observation where a declaration belongs", target: "cockroachdb", caps: capability.CockroachDB26(),
			facets: must.Must(schemaext.NewFacets(&crdbschema.ObservedRowTTL{Policy: crdbschema.Policy{ExpireAfter: "3 days"}})), wantErr: schemaext.ErrInvalidValue},
		{name: "an invalid declaration", target: "cockroachdb", caps: capability.CockroachDB26(),
			facets: must.Must(schemaext.NewFacets(&crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{JobCron: "@daily"}})), wantErr: schemaext.ErrInvalidValue},
		{name: "a capability set without the key", target: "cockroachdb", caps: capability.CockroachDB26().With(capability.RowLevelTTL, false),
			facets: policy, wantErr: ptaherr.ErrUnsupportedFeature},
		{name: "another target", target: "postgres", caps: capability.Postgres17(), facets: policy, wantErr: ptaherr.ErrUnsupportedDialect},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			clause, err := crdbrender.CreateTableClause(test.target, test.caps, "t", test.facets)
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(clause, qt.Equals, "")
		})
	}
}
