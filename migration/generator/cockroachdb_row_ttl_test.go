package generator_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// rowTTLSource is a Go annotation source for one table, with the given
// platform.cockroachdb properties on its directive.
func rowTTLSource(c *qt.C, properties string) *schemamodel.Database {
	c.Helper()
	database := must.Must(goschema.ParseSource("sessions.go", `package entities

//ptah:schema:table name="sessions"`+properties+`
type Session struct {
	//ptah:schema:field name="id" type="INT8" primary="true"
	ID int64
	//ptah:schema:field name="expires_at" type="TIMESTAMPTZ"
	ExpiresAt string
}
`))
	return &database
}

// liveRowTTLTable is the table that source creates, read with complete
// row-level TTL knowledge and carrying the given policy, if any.
func liveRowTTLTable(policy *crdbschema.Policy) *catalog.Database {
	table := catalog.Table{Name: "sessions", Type: "BASE TABLE", Columns: []catalog.Column{
		{Name: "id", DataType: "bigint", UDTName: "int8", IsNullable: "NO", IsPrimaryKey: true},
		{Name: "expires_at", DataType: "timestamp with time zone", UDTName: "timestamptz", IsNullable: "YES"},
	}}
	if policy != nil {
		table.Facets = must.Must(must.Must(schemaext.NewFacets(&crdbschema.ObservedRowTTL{Policy: *policy})).
			WithTargetScope(crdbschema.RowTTLKind, "cockroachdb"))
	}
	subject := objectidentity.NewBuilder(identifier.ForDialect("cockroachdb")).TableParts("", "sessions")
	return &catalog.Database{Tables: []catalog.Table{table}, FeatureCoverage: must.Must(crdbschema.RowTTLCoverage(schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables were read"},
		[]schemaext.SubjectCoverage{{Kind: crdbschema.RowTTLKind, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}))}
}

func renderedStatements(c *qt.C, nodes []ast.Node) []string {
	c.Helper()
	sql := must.Must(builtin.RenderSQLWithCapabilities("cockroachdb", capability.CockroachDB26(), nodes...))
	var statements []string
	for line := range strings.SplitSeq(sql, "\n") {
		if strings.HasPrefix(line, "ALTER TABLE") || strings.HasPrefix(line, "CREATE TABLE") || strings.HasPrefix(line, "DROP TABLE") || strings.HasPrefix(line, "-- Row-level TTL") {
			statements = append(statements, line)
		}
	}
	return statements
}

// TestCockroachDBRowTTLPlansBothDirections drives a Go annotation declaration
// through comparison and the bidirectional plan, and renders both directions.
// The reverse restores the prior policy and says what it cannot restore.
func TestCockroachDBRowTTLPlansBothDirections(t *testing.T) {
	tests := []struct {
		name            string
		properties      string
		current         *crdbschema.Policy
		wantForward     []string
		wantReverse     []string
		wantLimitations int
	}{
		{
			name: "adding a policy", properties: ` platform.cockroachdb.ttl_expiration_expression="expires_at"`,
			wantForward:     []string{"-- Row-level TTL on table: sessions", `ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at');`},
			wantReverse:     []string{"-- Row-level TTL on table: sessions", `ALTER TABLE "sessions" RESET (ttl);`},
			wantLimitations: 1,
		},
		{
			name: "removing a policy", current: &crdbschema.Policy{ExpirationExpression: "expires_at", JobCron: "@daily"},
			wantForward: []string{"-- Row-level TTL on table: sessions", `ALTER TABLE "sessions" RESET (ttl);`},
			wantReverse: []string{"-- Row-level TTL on table: sessions", `ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@daily');`},
		},
		{
			name: "changing a policy", properties: ` platform.cockroachdb.ttl_expire_after="3 days"`,
			current: &crdbschema.Policy{ExpirationExpression: "expires_at", SelectBatchSize: new(int64(500))},
			wantForward: []string{
				"-- Row-level TTL on table: sessions",
				`ALTER TABLE "sessions" SET (ttl_expire_after = '3 days');`,
				`ALTER TABLE "sessions" RESET (ttl_expiration_expression, ttl_select_batch_size);`,
			},
			wantReverse: []string{
				"-- Row-level TTL on table: sessions",
				`ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at', ttl_select_batch_size = 500);`,
				`ALTER TABLE "sessions" RESET (ttl_expire_after);`,
			},
			wantLimitations: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			source := rowTTLSource(c, test.properties)
			current := liveRowTTLTable(test.current)
			diff, err := schemadiff.CompareWithDialect(t.Context(), source, current, "cockroachdb", runtime)
			c.Assert(err, qt.IsNil)
			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: source, CurrentSchema: current, Dialect: "cockroachdb", Capabilities: capability.CockroachDB26(),
			})
			c.Assert(err, qt.IsNil)
			c.Assert(renderedStatements(c, plan.Forward.Nodes), qt.DeepEquals, test.wantForward)
			c.Assert(renderedStatements(c, plan.Reverse.Nodes), qt.DeepEquals, test.wantReverse)
			c.Assert(plan.Reverse.Recovery, qt.HasLen, 1)
			c.Assert(plan.Reverse.Recovery[0].Limitations, qt.HasLen, test.wantLimitations)
		})
	}
}

// TestCockroachDBRowTTLTableCreationCarriesThePolicy pins that a created table
// carries its policy in the CREATE TABLE and that the reverse drops the table
// with it, rather than planning a separate change for either.
func TestCockroachDBRowTTLTableCreationCarriesThePolicy(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	source := rowTTLSource(c, ` platform.cockroachdb.ttl_expire_after="3 days"`)
	current := &catalog.Database{}

	diff, err := schemadiff.CompareWithDialect(t.Context(), source, current, "cockroachdb", runtime)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: source, CurrentSchema: current, Dialect: "cockroachdb", Capabilities: capability.CockroachDB26(),
	})
	c.Assert(err, qt.IsNil)

	forward := renderedStatements(c, plan.Forward.Nodes)
	c.Assert(forward, qt.HasLen, 1)
	c.Assert(forward[0], qt.Contains, `CREATE TABLE "sessions"`)
	c.Assert(must.Must(builtin.RenderSQLWithCapabilities("cockroachdb", capability.CockroachDB26(), plan.Forward.Nodes...)),
		qt.Contains, ") WITH (ttl_expire_after = '3 days');")
	c.Assert(renderedStatements(c, plan.Reverse.Nodes), qt.DeepEquals, []string{`DROP TABLE IF EXISTS "sessions" CASCADE;`})
}

// TestCockroachDBRowTTLPlansBothDirectionsBesideAnRLSToggle pins a row-level
// TTL change on a table whose row-level security changes in the same plan.
// The generator cannot project the reverse capture of such a table; the TTL
// reverse does not read it, so both directions are planned rather than the
// whole plan being refused.
func TestCockroachDBRowTTLPlansBothDirectionsBesideAnRLSToggle(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	source := must.Must(goschema.ParseSource("sessions.go", `package entities

//ptah:schema:rls:enable table="sessions"
//ptah:schema:table name="sessions" platform.cockroachdb.ttl_expire_after="3 days"
type Session struct {
	//ptah:schema:field name="id" type="INT8" primary="true"
	ID int64
	//ptah:schema:field name="expires_at" type="TIMESTAMPTZ"
	ExpiresAt string
}
`))
	current := liveRowTTLTable(nil)
	// The read reported the table's row-level security, with both switches off.
	current.FeatureCoverage = must.Must(current.FeatureCoverage.Combine(must.Must(pgpolicy.CompleteCoverage(schemaext.Observed))))

	diff, err := schemadiff.CompareWithDialect(t.Context(), &source, current, "cockroachdb", runtime)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: &source, CurrentSchema: current, Dialect: "cockroachdb", Capabilities: capability.CockroachDB26(),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(renderedStatements(c, plan.Forward.Nodes), qt.DeepEquals, []string{
		"-- Row-level TTL on table: sessions",
		`ALTER TABLE "sessions" SET (ttl_expire_after = '3 days');`,
		`ALTER TABLE "sessions" ENABLE ROW LEVEL SECURITY;`,
	})
	c.Assert(renderedStatements(c, plan.Reverse.Nodes), qt.DeepEquals, []string{
		"-- Row-level TTL on table: sessions",
		`ALTER TABLE "sessions" RESET (ttl);`,
		`ALTER TABLE "sessions" DISABLE ROW LEVEL SECURITY;`,
	})
}
