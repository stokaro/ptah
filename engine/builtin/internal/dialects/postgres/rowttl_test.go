package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbast"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/engine/builtin/internal/dialects/postgres"
)

// ttlTable is the smallest table a TTL can hang off: one key column and the
// timestamp an expiry expression refers to. A nil policy declares none.
func ttlTable(policy *crdbschema.Policy) *ast.CreateTableNode {
	node := &ast.CreateTableNode{
		Name: "sessions",
		Columns: []*ast.ColumnNode{
			{Name: "id", Type: "BIGINT", Primary: true},
			{Name: "expires_at", Type: "TIMESTAMPTZ", Nullable: true},
		},
	}
	if policy != nil {
		node.Facets = must.Must(schemaext.NewFacets(&crdbschema.DesiredRowTTL{Policy: *policy}))
	}
	return node
}

// TestRender_RowTTLClause pins the SQL a declared policy becomes.
//
// The statements below were run against live CockroachDB v25.4.14 and v26.2.5
// and accepted verbatim, and pg_class.reloptions then reported exactly the
// parameters each one carried. That is what makes this an assertion about the
// server rather than about a string.
func TestRender_RowTTLClause(t *testing.T) {
	tests := []struct {
		name   string
		policy *crdbschema.Policy
		want   string
	}{
		{
			name:   "a table with no TTL carries no WITH clause",
			policy: nil,
			want:   `);`,
		},
		{
			name:   "the enabler alone, which is the issue's reproducer",
			policy: &crdbschema.Policy{ExpirationExpression: "expires_at"},
			want:   `) WITH (ttl_expiration_expression = 'expires_at');`,
		},
		{
			// An expression containing a quote reaches the server as SQL's
			// doubled-quote form. Getting this wrong is a syntax error at
			// apply time, on the most common non-trivial expression there is.
			name:   "an expression carrying a quote",
			policy: &crdbschema.Policy{ExpirationExpression: "expires_at + INTERVAL '1 day'"},
			want:   `) WITH (ttl_expiration_expression = 'expires_at + INTERVAL ''1 day''');`,
		},
		{
			// The interval enabler is sent verbatim: the server normalizes it,
			// and the comparison reads both sides as intervals rather than
			// Ptah predicting the stored spelling (stokaro/ptah#1605).
			name:   "the interval enabler",
			policy: &crdbschema.Policy{ExpireAfter: "72 hours"},
			want:   `) WITH (ttl_expire_after = '72 hours');`,
		},
		{
			name: "every managed parameter, in the order the plan fixes",
			policy: &crdbschema.Policy{
				ExpirationExpression:         "expires_at",
				JobCron:                      "@daily",
				SelectBatchSize:              new(int64(500)),
				DeleteBatchSize:              new(int64(100)),
				SelectRateLimit:              new(int64(200)),
				DeleteRateLimit:              new(int64(300)),
				Pause:                        true,
				LabelMetrics:                 true,
				DisableChangefeedReplication: true,
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
			sql, err := renderer.Render(ttlTable(test.policy))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestRender_RowTTLIsRefusedOnEveryOtherTarget is the gate, and a refusal
// rather than a dropped clause is the whole point.
//
// Row-level TTL deletes rows. A renderer that quietly omitted the clause would
// emit a CREATE TABLE the server accepts and leave a table whose declared
// retention policy simply does not exist -- the operator would find out by
// noticing rows that should have expired. YugabyteDB makes that concrete: it
// answers `WARNING: storage parameter ttl_expiration_expression is unsupported,
// ignoring` before refusing, so an engine that ignores the parameter is not
// hypothetical (stokaro/ptah#1027).
func TestRender_RowTTLIsRefusedOnEveryOtherTarget(t *testing.T) {
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
			sql, err := renderer.Render(ttlTable(&crdbschema.Policy{ExpirationExpression: "expires_at"}))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err.Error(), qt.Contains, string(crdbschema.RowTTLKind))
			c.Assert(sql, qt.Not(qt.Contains), "ttl_expiration_expression = ")
		})
	}
}

// TestRender_RowTTLIsRefusedWithoutTheCapability closes the gate on a
// CockroachDB target whose capability set lacks the key, which a pinned or
// hand-built set can.
func TestRender_RowTTLIsRefusedWithoutTheCapability(t *testing.T) {
	c := qt.New(t)

	caps := capability.CockroachDB26().With(capability.RowLevelTTL, false)
	renderer := postgres.NewWithCapabilities(caps, platform.CockroachDB)
	sql, err := renderer.Render(ttlTable(&crdbschema.Policy{ExpirationExpression: "expires_at"}))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err.Error(), qt.Contains, "declares row-level TTL")
	c.Assert(sql, qt.Not(qt.Contains), "ttl_expiration_expression = ")
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

func alterRowTTL(before, after *crdbschema.Policy) ast.AlterOperation {
	change := crdbdiff.RowTTL{}
	if before != nil {
		change.Before = &crdbschema.ObservedRowTTL{Policy: *before}
	}
	if after != nil {
		change.After = &crdbschema.DesiredRowTTL{Policy: *after}
	}
	return &ast.ExtensionAlterOperation{Payload: &crdbast.AlterRowTTL{Change: change}}
}

// TestRender_RowTTLAlterOperations pins the statements a transition lowers to.
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
			name:      "adding a policy",
			operation: alterRowTTL(nil, &crdbschema.Policy{ExpirationExpression: "expires_at"}),
			want:      `ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at');`,
		},
		{
			name: "changing a value names every parameter the policy keeps",
			operation: alterRowTTL(&crdbschema.Policy{ExpirationExpression: "expires_at", JobCron: "@daily"},
				&crdbschema.Policy{ExpirationExpression: "expires_at", JobCron: "@hourly"}),
			want: `ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@hourly');`,
		},
		{
			name:      "removing the whole policy",
			operation: alterRowTTL(&crdbschema.Policy{ExpirationExpression: "expires_at"}, nil),
			want:      `ALTER TABLE "sessions" RESET (ttl);`,
		},
		{
			// RESET takes several names at once, which is why a plan needs one
			// statement rather than one per dropped parameter, and it comes
			// before the SET so the text is a function of the two states.
			name: "a dropped parameter is reset before the rest is set",
			operation: alterRowTTL(
				&crdbschema.Policy{ExpirationExpression: "expires_at", JobCron: "@daily", SelectBatchSize: new(int64(500))},
				&crdbschema.Policy{ExpirationExpression: "expires_at + INTERVAL '1 hour'"}),
			want: "ALTER TABLE \"sessions\" RESET (ttl_job_cron, ttl_select_batch_size);\n" +
				`ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at + INTERVAL ''1 hour''');`,
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

// TestRender_RowTTLAlterFailurePath closes the gate on the ALTER path. A plan
// reaching a target that cannot run these statements must fail with the
// explanation rather than with the server's parse error, and a change whose
// operands are the same policy must not render statements that change nothing.
func TestRender_RowTTLAlterFailurePath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		caps      capability.Capabilities
		operation ast.AlterOperation
		wantErr   error
	}{
		{
			name: "PostgreSQL has no owner for the operation", dialect: platform.Postgres, caps: capability.Postgres17(),
			operation: alterRowTTL(nil, &crdbschema.Policy{ExpirationExpression: "expires_at"}), wantErr: ptaherr.ErrUnsupportedFeature,
		},
		{
			name: "a CockroachDB set without the capability", dialect: platform.CockroachDB,
			caps:      capability.CockroachDB26().With(capability.RowLevelTTL, false),
			operation: alterRowTTL(&crdbschema.Policy{ExpirationExpression: "expires_at"}, nil), wantErr: ptaherr.ErrUnsupportedFeature,
		},
		{
			name: "two spellings of one interval are not a change", dialect: platform.CockroachDB, caps: capability.CockroachDB26(),
			operation: alterRowTTL(&crdbschema.Policy{ExpireAfter: "72:00:00"}, &crdbschema.Policy{ExpireAfter: "72 hours"}),
			wantErr:   schemaext.ErrInvalidValue,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.NewWithCapabilities(test.caps, test.dialect)
			sql, err := renderer.Render(&ast.AlterTableNode{
				Name:       "sessions",
				Operations: []ast.AlterOperation{test.operation},
			})

			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(sql, qt.Not(qt.Contains), "ttl")
		})
	}
}

// TestRender_RowTTLStandaloneOperationNeedsItsTable answers a standalone
// owned operation the way it is answered inside a parent: validated, and then
// reported as needing the ALTER TABLE that names its table.
func TestRender_RowTTLStandaloneOperationNeedsItsTable(t *testing.T) {
	c := qt.New(t)

	renderer := postgres.NewWithCapabilities(capability.CockroachDB26(), platform.CockroachDB)
	sql, err := renderer.Render(alterRowTTL(nil, &crdbschema.Policy{ExpirationExpression: "expires_at"}))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `(?s).*requires an ALTER TABLE parent.*`)
	c.Assert(sql, qt.Equals, "")
}
