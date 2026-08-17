package crdbttl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/crdbttl"
)

// TestOptions_RendersTheMeasuredSpelling pins the text that goes into a
// statement, because the statement is what the server stores and what a later
// read is compared against.
//
// The ORDER is asserted, not just the contents. A plan is fingerprinted by the
// migration layer and re-approved by a person, so a statement whose parameter
// order varied between runs over the same two states would churn both.
func TestOptions_RendersTheMeasuredSpelling(t *testing.T) {
	tests := []struct {
		name string
		spec *ast.RowTTLSpec
		want []crdbttl.Option
	}{
		{
			name: "a nil spec renders nothing",
			spec: nil,
			want: nil,
		},
		{
			name: "an empty spec renders nothing",
			spec: &ast.RowTTLSpec{},
			want: nil,
		},
		{
			name: "the enabler alone",
			spec: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			want: []crdbttl.Option{{Name: "ttl_expiration_expression", Value: "'expires_at'"}},
		},
		{
			// An expression may contain a quote of its own, and the engine's
			// own documentation uses one. Doubling is how a value carrying a
			// quote reaches the server intact.
			name: "an expression carrying a quote is doubled",
			spec: &ast.RowTTLSpec{ExpirationExpression: "expires_at + INTERVAL '1 day'"},
			want: []crdbttl.Option{
				{Name: "ttl_expiration_expression", Value: `'expires_at + INTERVAL ''1 day'''`},
			},
		},
		{
			name: "the enabler comes first, then the cron, then ints, then bools",
			spec: &ast.RowTTLSpec{
				DisableChangefeedReplication: new(true),
				DeleteRateLimit:              new(int64(300)),
				JobCron:                      "@daily",
				SelectBatchSize:              new(int64(500)),
				ExpirationExpression:         "expires_at",
				LabelMetrics:                 new(true),
				DeleteBatchSize:              new(int64(100)),
				Pause:                        new(true),
				SelectRateLimit:              new(int64(200)),
			},
			want: []crdbttl.Option{
				{Name: "ttl_expiration_expression", Value: "'expires_at'"},
				{Name: "ttl_job_cron", Value: "'@daily'"},
				{Name: "ttl_select_batch_size", Value: "500"},
				{Name: "ttl_delete_batch_size", Value: "100"},
				{Name: "ttl_select_rate_limit", Value: "200"},
				{Name: "ttl_delete_rate_limit", Value: "300"},
				{Name: "ttl_pause", Value: "true"},
				{Name: "ttl_label_metrics", Value: "true"},
				{Name: "ttl_disable_changefeed_replication", Value: "true"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(crdbttl.Options(test.spec), qt.DeepEquals, test.want)
		})
	}
}

// TestEqual_IsExact pins that the comparison does no normalizing of its own.
//
// Every modeled parameter was measured to read back from the catalog exactly as
// written, so two values that differ are two policies that differ. A comparison
// that treated them as equal would report convergence while a table's
// data-lifecycle policy differed, which is the failure stokaro/ptah#1027 names.
func TestEqual_IsExact(t *testing.T) {
	tests := []struct {
		name string
		a    *ast.RowTTLSpec
		b    *ast.RowTTLSpec
		want bool
	}{
		{
			name: "nil equals nil",
			want: true,
		},
		{
			name: "nil equals an empty spec, because both mean no policy",
			a:    nil,
			b:    &ast.RowTTLSpec{},
			want: true,
		},
		{
			name: "a policy does not equal no policy",
			a:    &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			b:    nil,
			want: false,
		},
		{
			name: "identical policies",
			a:    &ast.RowTTLSpec{ExpirationExpression: "expires_at", JobCron: "@daily"},
			b:    &ast.RowTTLSpec{ExpirationExpression: "expires_at", JobCron: "@daily"},
			want: true,
		},
		{
			// Whitespace is not formatting here: the catalog stores it, so two
			// spellings that differ by a space are two stored values.
			name: "an expression differing only in whitespace",
			a:    &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			b:    &ast.RowTTLSpec{ExpirationExpression: " expires_at "},
			want: false,
		},
		{
			name: "an expression differing only in case",
			a:    &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			b:    &ast.RowTTLSpec{ExpirationExpression: "EXPIRES_AT"},
			want: false,
		},
		{
			name: "a knob set on one side only",
			a:    &ast.RowTTLSpec{ExpirationExpression: "expires_at", SelectBatchSize: new(int64(500))},
			b:    &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			want: false,
		},
		{
			name: "the same knob with different values",
			a:    &ast.RowTTLSpec{ExpirationExpression: "expires_at", SelectBatchSize: new(int64(500))},
			b:    &ast.RowTTLSpec{ExpirationExpression: "expires_at", SelectBatchSize: new(int64(501))},
			want: false,
		},
		{
			name: "a boolean set on one side only",
			a:    &ast.RowTTLSpec{ExpirationExpression: "expires_at", Pause: new(true)},
			b:    &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(crdbttl.Equal(test.a, test.b), qt.Equals, test.want)
			c.Assert(crdbttl.Equal(test.b, test.a), qt.Equals, test.want,
				qt.Commentf("equality has to be symmetric"))
		})
	}
}

// TestDroppedParameters_NamesWhatSetWouldLeaveBehind is the rule the whole
// change transition rests on.
//
// Measured on v26.2.5, `SET` replaces only the parameters it names: a table
// carrying ttl_job_cron and ttl_select_batch_size, given
// `SET (ttl_job_cron = '@hourly')`, keeps its batch size. So a declaration that
// stops naming a parameter leaves it in place forever unless the plan resets it.
func TestDroppedParameters_NamesWhatSetWouldLeaveBehind(t *testing.T) {
	tests := []struct {
		name    string
		desired *ast.RowTTLSpec
		current *ast.RowTTLSpec
		want    []string
	}{
		{
			name:    "nothing live, nothing dropped",
			desired: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			current: nil,
			want:    nil,
		},
		{
			name:    "an unchanged policy drops nothing",
			desired: &ast.RowTTLSpec{ExpirationExpression: "expires_at", JobCron: "@daily"},
			current: &ast.RowTTLSpec{ExpirationExpression: "expires_at", JobCron: "@daily"},
			want:    nil,
		},
		{
			name:    "a knob the declaration stopped naming",
			desired: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			current: &ast.RowTTLSpec{ExpirationExpression: "expires_at", SelectBatchSize: new(int64(500))},
			want:    []string{"ttl_select_batch_size"},
		},
		{
			// Several at once, in the order ManagedParameters lists them, so
			// the RESET statement's text is a function of the two states.
			name:    "several dropped knobs come back in a fixed order",
			desired: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			current: &ast.RowTTLSpec{
				ExpirationExpression: "expires_at",
				Pause:                new(true),
				JobCron:              "@daily",
				DeleteBatchSize:      new(int64(100)),
			},
			want: []string{"ttl_job_cron", "ttl_delete_batch_size", "ttl_pause"},
		},
		{
			// A changed value is NOT a dropped parameter: SET replaces it.
			name:    "a changed value is not a drop",
			desired: &ast.RowTTLSpec{ExpirationExpression: "expires_at", JobCron: "@hourly"},
			current: &ast.RowTTLSpec{ExpirationExpression: "expires_at", JobCron: "@daily"},
			want:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(crdbttl.DroppedParameters(test.desired, test.current), qt.DeepEquals, test.want)
		})
	}
}

func TestValidateDeclared_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		tables []crdbttl.TableTTL
	}{
		{
			name:   "no tables at all",
			tables: nil,
		},
		{
			name:   "a table declaring no TTL",
			tables: []crdbttl.TableTTL{{Name: "sessions"}},
		},
		{
			name: "the enabler alone",
			tables: []crdbttl.TableTTL{{
				Name: "sessions", RowTTL: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			}},
		},
		{
			name: "every knob alongside the enabler",
			tables: []crdbttl.TableTTL{{Name: "sessions", RowTTL: &ast.RowTTLSpec{
				ExpirationExpression: "expires_at",
				JobCron:              "@daily",
				SelectBatchSize:      new(int64(1)),
				DeleteBatchSize:      new(int64(1)),
				SelectRateLimit:      new(int64(1)),
				DeleteRateLimit:      new(int64(1)),
				Pause:                new(true),
			}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := crdbttl.ValidateDeclared(
				platform.CockroachDB, capability.CockroachDB26(), crdbttl.DeclaredIn(test.tables))
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestValidateDeclared_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		tables  []crdbttl.TableTTL
		wantErr string
	}{
		{
			// Every other ttl_ parameter is refused by the server when no
			// expiry is configured, so Ptah refuses it before the statement.
			name: "a knob with no enabler",
			tables: []crdbttl.TableTTL{{
				Name: "sessions", RowTTL: &ast.RowTTLSpec{JobCron: "@daily"},
			}},
			wantErr: `(?s).*declares row-level TTL settings but no ttl_expiration_expression.*`,
		},
		{
			// Zero is the silent shape: accepted, then stored nowhere.
			name: "a zero-valued knob",
			tables: []crdbttl.TableTTL{{Name: "sessions", RowTTL: &ast.RowTTLSpec{
				ExpirationExpression: "expires_at", SelectBatchSize: new(int64(0)),
			}}},
			wantErr: `(?s).*declares ttl_select_batch_size = 0: CockroachDB refuses a negative value.*stores nothing at all for zero.*`,
		},
		{
			name: "a negative knob",
			tables: []crdbttl.TableTTL{{Name: "sessions", RowTTL: &ast.RowTTLSpec{
				ExpirationExpression: "expires_at", DeleteRateLimit: new(int64(-1)),
			}}},
			wantErr: `(?s).*declares ttl_delete_rate_limit = -1.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := crdbttl.ValidateDeclared(
				platform.CockroachDB, capability.CockroachDB26(), crdbttl.DeclaredIn(test.tables))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestValidateDeclared_RefusesEveryDialectWithoutTheCapability is the dialect
// gate, and it is the assertion that matters most here.
//
// PostgreSQL refuses the parameter itself, but YugabyteDB answers `WARNING:
// storage parameter ttl_expiration_expression is unsupported, ignoring` before
// its own error — an engine that IGNORES a storage parameter would accept a
// row-expiry policy and never apply it. That is why the gate is Ptah's and not
// the server's.
func TestValidateDeclared_RefusesEveryDialectWithoutTheCapability(t *testing.T) {
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{platform.Postgres, capability.Postgres17()},
		{platform.YugabyteDB, capability.YugabyteDB25()},
		{platform.Spanner, capability.SpannerPostgres()},
		{platform.MySQL, capability.MySQL84()},
		{platform.MariaDB, capability.MariaDB1011()},
		{platform.SQLite, capability.SQLite3()},
		{platform.SQLServer, capability.SQLServer2022()},
		{platform.ClickHouse, capability.ClickHouse24()},
	}

	declared := []crdbttl.TableTTL{{
		Name: "sessions", RowTTL: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
	}}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			err := crdbttl.ValidateDeclared(test.dialect, test.caps, crdbttl.DeclaredIn(declared))

			c.Assert(err, qt.ErrorMatches, `(?s).*declares row-level TTL, which .* does not support.*`)
		})
	}
}

// TestValidateDeclared_AcceptsTheSameDeclarationOnCockroachDB is the other half
// of the control above. Without it, a ValidateDeclared that refused every
// dialect would satisfy that test completely.
func TestValidateDeclared_AcceptsTheSameDeclarationOnCockroachDB(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
	}{
		{"CockroachDB23", capability.CockroachDB23()},
		{"CockroachDB25", capability.CockroachDB25()},
		{"CockroachDB26", capability.CockroachDB26()},
	}

	declared := []crdbttl.TableTTL{{
		Name: "sessions", RowTTL: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(
				crdbttl.ValidateDeclared(platform.CockroachDB, test.caps, crdbttl.DeclaredIn(declared)),
				qt.IsNil)
		})
	}
}

// TestValidateDeclared_LeavesADialectAloneWhenNothingDeclaresATTL keeps the
// gate from firing on schemas that have nothing to do with it. Every dialect
// must accept a schema with no TTL, or adding this capability would refuse
// schemas that worked before.
func TestValidateDeclared_LeavesADialectAloneWhenNothingDeclaresATTL(t *testing.T) {
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{platform.Postgres, capability.Postgres17()},
		{platform.MySQL, capability.MySQL84()},
		{platform.SQLite, capability.SQLite3()},
		{platform.ClickHouse, capability.ClickHouse24()},
	}

	declared := []crdbttl.TableTTL{{Name: "sessions"}, {Name: "events", RowTTL: &ast.RowTTLSpec{}}}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(
				crdbttl.ValidateDeclared(test.dialect, test.caps, crdbttl.DeclaredIn(declared)),
				qt.IsNil)
		})
	}
}

// TestValidateDeclared_IsDeterministic guards the diagnostic against the order
// tables arrive in. A schema with two offending tables must name the same one
// on every run.
func TestValidateDeclared_IsDeterministic(t *testing.T) {
	c := qt.New(t)

	forward := []crdbttl.TableTTL{
		{Name: "zeta", RowTTL: &ast.RowTTLSpec{JobCron: "@daily"}},
		{Name: "alpha", RowTTL: &ast.RowTTLSpec{JobCron: "@hourly"}},
	}
	reversed := []crdbttl.TableTTL{forward[1], forward[0]}

	first := crdbttl.ValidateDeclared(platform.CockroachDB, capability.CockroachDB26(), crdbttl.DeclaredIn(forward))
	second := crdbttl.ValidateDeclared(platform.CockroachDB, capability.CockroachDB26(), crdbttl.DeclaredIn(reversed))

	c.Assert(first, qt.IsNotNil)
	c.Assert(second, qt.IsNotNil)
	c.Assert(second.Error(), qt.Equals, first.Error())
}
