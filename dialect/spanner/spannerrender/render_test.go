package spannerrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/spanner/spannerast"
	"ptah.run/dialect/spanner/spannerdiff"
	"ptah.run/dialect/spanner/spannerrender"
	"ptah.run/dialect/spanner/spannerschema"
)

func policy(column, interval string) spannerschema.Policy {
	return spannerschema.Policy{Column: column, Interval: interval}
}

// TestRegistry_PicksTheVerbByTheTransition pins the statement each transition
// lowers to. Measured against the Cloud Spanner emulator behind PGAdapter,
// ADD and ALTER are each refused in the other's position, and DROP names no
// column. The table is spelled as the PostgreSQL-family renderer spells it.
func TestRegistry_PicksTheVerbByTheTransition(t *testing.T) {
	stored, declared := policy("created_at", "4 WEEKS 2 DAYS"), policy("created_at", "60 days")
	tests := []struct {
		name   string
		table  string
		change spannerdiff.RowDeletion
		want   string
	}{
		{name: "an addition", table: "sessions", change: spannerdiff.RowDeletion{After: &spannerschema.DesiredRowDeletion{Policy: declared}},
			want: `ALTER TABLE "sessions" ADD TTL INTERVAL '60 days' ON "created_at";`},
		{name: "a change", table: "sessions", change: spannerdiff.RowDeletion{Before: &spannerschema.ObservedRowDeletion{Policy: stored}, After: &spannerschema.DesiredRowDeletion{Policy: declared}},
			want: `ALTER TABLE "sessions" ALTER TTL INTERVAL '60 days' ON "created_at";`},
		{name: "a removal", table: "sessions", change: spannerdiff.RowDeletion{Before: &spannerschema.ObservedRowDeletion{Policy: stored}},
			want: `ALTER TABLE "sessions" DROP TTL;`},
		{name: "a quoted name", table: `odd"name`, change: spannerdiff.RowDeletion{After: &spannerschema.DesiredRowDeletion{Policy: policy(`a"b`, "1 days")}},
			want: `ALTER TABLE "odd""name" ADD TTL INTERVAL '1 days' ON "a""b";`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := must.Must(spannerrender.Registry()).Render(renderer.ExtensionContext{
				Target: "spanner", Capabilities: capability.SpannerPostgres(), Parent: &ast.AlterTableNode{Name: test.table},
			}, ast.AlterExtension, &spannerast.AlterRowDeletion{Change: test.change})
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{test.want})
		})
	}
}

func TestRegistry_FailurePath(t *testing.T) {
	added := &spannerast.AlterRowDeletion{Change: spannerdiff.RowDeletion{After: &spannerschema.DesiredRowDeletion{Policy: policy("ts", "1 days")}}}
	tests := []struct {
		name    string
		ctx     renderer.ExtensionContext
		role    ast.ExtensionRole
		wantErr error
	}{
		{name: "another target", ctx: renderer.ExtensionContext{Target: "postgres", Capabilities: capability.Postgres17(), Parent: &ast.AlterTableNode{Name: "t"}},
			role: ast.AlterExtension, wantErr: ptaherr.ErrUnsupportedDialect},
		{name: "a capability set without the key", ctx: renderer.ExtensionContext{Target: "spanner", Capabilities: capability.SpannerPostgres().With(capability.RowDeletionPolicy, false),
			Parent: &ast.AlterTableNode{Name: "t"}}, role: ast.AlterExtension, wantErr: ptaherr.ErrUnsupportedFeature},
		{name: "no parent", ctx: renderer.ExtensionContext{Target: "spanner", Capabilities: capability.SpannerPostgres()},
			role: ast.AlterExtension, wantErr: ptaherr.ErrInvalidSchemaDiff},
		{name: "a standalone statement", ctx: renderer.ExtensionContext{Target: "spanner", Capabilities: capability.SpannerPostgres()},
			role: ast.StatementExtension, wantErr: ptaherr.ErrUnsupportedFeature},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := must.Must(spannerrender.Registry()).Render(test.ctx, test.role, added)
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
			name:   "a policy, in the author's spelling",
			facets: must.Must(schemaext.NewFacets(&spannerschema.DesiredRowDeletion{Policy: policy("created_at", "30 days")})),
			want:   ` TTL INTERVAL '30 days' ON "created_at"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			clause, err := spannerrender.CreateTableClause("spanner", capability.SpannerPostgres(), "t", test.facets)
			c.Assert(err, qt.IsNil)
			c.Assert(clause, qt.Equals, test.want)
		})
	}
}

func TestCreateTableClause_FailurePath(t *testing.T) {
	declared := must.Must(schemaext.NewFacets(&spannerschema.DesiredRowDeletion{Policy: policy("created_at", "30 days")}))
	tests := []struct {
		name    string
		target  string
		caps    capability.Capabilities
		facets  schemaext.Facets
		wantErr error
	}{
		{name: "another owner's facet", target: "spanner", caps: capability.SpannerPostgres(),
			facets: must.Must(schemaext.NewFacets(&crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpireAfter: "3 days"}})), wantErr: ptaherr.ErrUnsupportedFeature},
		{name: "an observation where a declaration belongs", target: "spanner", caps: capability.SpannerPostgres(),
			facets: must.Must(schemaext.NewFacets(&spannerschema.ObservedRowDeletion{Policy: policy("created_at", "30 days")})), wantErr: schemaext.ErrInvalidValue},
		{name: "an interval the server refuses", target: "spanner", caps: capability.SpannerPostgres(),
			facets: must.Must(schemaext.NewFacets(&spannerschema.DesiredRowDeletion{Policy: policy("created_at", "36 hours")})), wantErr: schemaext.ErrInvalidValue},
		{name: "a capability set without the key", target: "spanner", caps: capability.SpannerPostgres().With(capability.RowDeletionPolicy, false),
			facets: declared, wantErr: ptaherr.ErrUnsupportedFeature},
		{name: "another target", target: "postgres", caps: capability.Postgres17(), facets: declared, wantErr: ptaherr.ErrUnsupportedDialect},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			clause, err := spannerrender.CreateTableClause(test.target, test.caps, "t", test.facets)
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(clause, qt.Equals, "")
		})
	}
}
