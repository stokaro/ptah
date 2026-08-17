package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/postgres"
)

func int64p(value int64) *int64 { return &value }
func boolp(value bool) *bool    { return &value }

// ttlTable is the smallest table a TTL can hang off: one key column and the
// timestamp an expiry expression refers to.
func ttlTable(spec *ast.RowTTLSpec) *ast.CreateTableNode {
	return &ast.CreateTableNode{
		Name: "sessions",
		Columns: []*ast.ColumnNode{
			{Name: "id", Type: "BIGINT", Primary: true},
			{Name: "expires_at", Type: "TIMESTAMPTZ", Nullable: true},
		},
		RowTTL: spec,
	}
}

// TestRender_RowTTLClause pins the SQL a declared policy becomes.
//
// The statements below were run against live CockroachDB v25.4.14 and v26.2.5
// and accepted verbatim, and pg_class.reloptions then reported exactly the
// parameters each one carried. That is what makes this an assertion about the
// server rather than about a string.
func TestRender_RowTTLClause(t *testing.T) {
	tests := []struct {
		name string
		spec *ast.RowTTLSpec
		want string
	}{
		{
			name: "a table with no TTL carries no WITH clause",
			spec: nil,
			want: `);`,
		},
		{
			name: "the enabler alone, which is the issue's reproducer",
			spec: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			want: `) WITH (ttl_expiration_expression = 'expires_at');`,
		},
		{
			// An expression containing a quote reaches the server as SQL's
			// doubled-quote form. Getting this wrong is a syntax error at
			// apply time, on the most common non-trivial expression there is.
			name: "an expression carrying a quote",
			spec: &ast.RowTTLSpec{ExpirationExpression: "expires_at + INTERVAL '1 day'"},
			want: `) WITH (ttl_expiration_expression = 'expires_at + INTERVAL ''1 day''');`,
		},
		{
			name: "every managed parameter, in the order the plan fixes",
			spec: &ast.RowTTLSpec{
				ExpirationExpression:         "expires_at",
				JobCron:                      "@daily",
				SelectBatchSize:              int64p(500),
				DeleteBatchSize:              int64p(100),
				SelectRateLimit:              int64p(200),
				DeleteRateLimit:              int64p(300),
				Pause:                        boolp(true),
				LabelMetrics:                 boolp(true),
				DisableChangefeedReplication: boolp(true),
			},
			want: `) WITH (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@daily', ` +
				`ttl_select_batch_size = 500, ttl_delete_batch_size = 100, ttl_select_rate_limit = 200, ` +
				`ttl_delete_rate_limit = 300, ttl_pause = true, ttl_label_metrics = true, ` +
				`ttl_disable_changefeed_replication = true);`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.NewWithCapabilities(capability.CockroachDB26(), platform.CockroachDB)
			sql, err := renderer.Render(ttlTable(test.spec))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestRender_RowTTLIsRefusedWithoutTheCapability is the gate, and a refusal
// rather than a dropped clause is the whole point.
//
// Row-level TTL deletes rows. A renderer that quietly omitted the clause would
// emit a CREATE TABLE the server accepts and leave a table whose declared
// retention policy simply does not exist -- the operator would find out by
// noticing rows that should have expired. YugabyteDB makes that concrete: it
// answers `WARNING: storage parameter ttl_expiration_expression is unsupported,
// ignoring` before refusing, so an engine that ignores the parameter is not
// hypothetical (stokaro/ptah#1027).
func TestRender_RowTTLIsRefusedWithoutTheCapability(t *testing.T) {
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{platform.Postgres, capability.Postgres17()},
		{platform.YugabyteDB, capability.YugabyteDB25()},
		{platform.Spanner, capability.SpannerPostgres()},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.NewWithCapabilities(test.caps, test.dialect)
			sql, err := renderer.Render(ttlTable(&ast.RowTTLSpec{ExpirationExpression: "expires_at"}))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err.Error(), qt.Contains, "declares row-level TTL")
			c.Assert(sql, qt.Not(qt.Contains), "ttl_expiration_expression = ")
		})
	}
}

// TestRender_TableWithoutTTLIsUnchangedOnEveryTarget is the non-interference
// control. Adding this capability must not change one byte of what a schema
// with no TTL renders, on the dialect this renderer has always served.
func TestRender_TableWithoutTTLIsUnchangedOnEveryTarget(t *testing.T) {
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{platform.Postgres, capability.Postgres17()},
		{platform.CockroachDB, capability.CockroachDB26()},
		{platform.YugabyteDB, capability.YugabyteDB25()},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.NewWithCapabilities(test.caps, test.dialect)
			sql, err := renderer.Render(ttlTable(nil))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Not(qt.Contains), "WITH (")
			c.Assert(sql, qt.Not(qt.Contains), "ttl")
		})
	}
}

// TestRender_RowTTLAlterOperations pins the two statements a transition uses.
//
// Both were measured on v25.4.14 and v26.2.5: SET adds a policy and changes
// one, replacing only the parameters it names; RESET removes named parameters,
// and `RESET (ttl)` removes the whole configuration and succeeds even against a
// table that never had one.
func TestRender_RowTTLAlterOperations(t *testing.T) {
	tests := []struct {
		name      string
		operation ast.AlterOperation
		want      string
	}{
		{
			name:      "setting the enabler",
			operation: &ast.SetRowTTLOperation{Options: []string{"ttl_expiration_expression = 'expires_at'"}},
			want:      `ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at');`,
		},
		{
			name: "setting several parameters at once",
			operation: &ast.SetRowTTLOperation{Options: []string{
				"ttl_expiration_expression = 'expires_at'", "ttl_job_cron = '@hourly'",
			}},
			want: `ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@hourly');`,
		},
		{
			name:      "removing the whole policy",
			operation: &ast.ResetRowTTLOperation{Parameters: []string{"ttl"}},
			want:      `ALTER TABLE "sessions" RESET (ttl);`,
		},
		{
			// RESET takes several names at once, which is why a plan needs one
			// statement rather than one per dropped parameter.
			name:      "removing several parameters at once",
			operation: &ast.ResetRowTTLOperation{Parameters: []string{"ttl_job_cron", "ttl_select_batch_size"}},
			want:      `ALTER TABLE "sessions" RESET (ttl_job_cron, ttl_select_batch_size);`,
		},
		{
			// An empty operation emits no statement. The header the ALTER
			// renderer always writes is what remains, which is what "nothing
			// was emitted" looks like from outside.
			name:      "an empty SET renders no statement",
			operation: &ast.SetRowTTLOperation{},
			want:      "-- ALTER statements: --",
		},
		{
			name:      "an empty RESET renders no statement",
			operation: &ast.ResetRowTTLOperation{},
			want:      "-- ALTER statements: --",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.NewWithCapabilities(capability.CockroachDB26(), platform.CockroachDB)
			sql, err := renderer.Render(&ast.AlterTableNode{
				Name:       "sessions",
				Operations: []ast.AlterOperation{test.operation},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(strings.TrimSpace(sql), qt.Contains, test.want)
		})
	}
}

// TestRender_RowTTLAlterIsRefusedWithoutTheCapability closes the same gate on
// the ALTER path. A plan reaching a target that cannot run these statements
// must fail with the explanation, not with the server's parse error.
func TestRender_RowTTLAlterIsRefusedWithoutTheCapability(t *testing.T) {
	tests := []struct {
		name      string
		operation ast.AlterOperation
	}{
		{"set", &ast.SetRowTTLOperation{Options: []string{"ttl_expiration_expression = 'expires_at'"}}},
		{"reset", &ast.ResetRowTTLOperation{Parameters: []string{"ttl"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.NewWithCapabilities(capability.Postgres17(), platform.Postgres)
			sql, err := renderer.Render(&ast.AlterTableNode{
				Name:       "sessions",
				Operations: []ast.AlterOperation{test.operation},
			})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Not(qt.Contains), "ttl")
		})
	}
}
