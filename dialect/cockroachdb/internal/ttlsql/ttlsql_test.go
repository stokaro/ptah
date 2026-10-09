package ttlsql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/cockroachdb/internal/ttlsql"
)

// TestOptions_RendersTheMeasuredSpelling pins the text that goes into a
// statement, because the statement is what the server stores and what a later
// read is compared against. The ORDER is asserted too: a plan is fingerprinted
// and re-approved by a person.
func TestOptions_RendersTheMeasuredSpelling(t *testing.T) {
	tests := []struct {
		name   string
		policy crdbschema.Policy
		want   []string
	}{
		{name: "an empty policy renders nothing", want: nil},
		{name: "the enabler alone", policy: crdbschema.Policy{ExpirationExpression: "expires_at"}, want: []string{"ttl_expiration_expression = 'expires_at'"}},
		{
			// An expression may contain a quote of its own, and the engine's
			// own documentation uses one. Doubling is how it reaches the server.
			name:   "an expression carrying a quote is doubled",
			policy: crdbschema.Policy{ExpirationExpression: "expires_at + INTERVAL '1 day'"},
			want:   []string{"ttl_expiration_expression = 'expires_at + INTERVAL ''1 day'''"},
		},
		{name: "the interval enabler renders verbatim", policy: crdbschema.Policy{ExpireAfter: "72 hours"}, want: []string{"ttl_expire_after = '72 hours'"}},
		{
			name: "the enabler comes first, then the cron, then counts, then flags",
			policy: crdbschema.Policy{
				DisableChangefeedReplication: true, DeleteRateLimit: new(int64(300)), JobCron: "@daily",
				SelectBatchSize: new(int64(500)), ExpirationExpression: "expires_at", LabelMetrics: true,
				DeleteBatchSize: new(int64(100)), Pause: true, SelectRateLimit: new(int64(200)),
			},
			want: []string{
				"ttl_expiration_expression = 'expires_at'", "ttl_job_cron = '@daily'", "ttl_select_batch_size = 500",
				"ttl_delete_batch_size = 100", "ttl_select_rate_limit = 200", "ttl_delete_rate_limit = 300",
				"ttl_pause = true", "ttl_label_metrics = true", "ttl_disable_changefeed_replication = true",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ttlsql.Options(test.policy), qt.DeepEquals, test.want)
		})
	}
}

// TestEquivalent_ReadsOnlyTheRewrittenValues pins that the comparison is exact
// on every parameter the catalog keeps verbatim and reads only the two the
// server rewrites through the value they denote. Treating two stored values as
// equal would report convergence while a table's data-lifecycle policy
// differed, which is the failure stokaro/ptah#1027 names.
func TestEquivalent_ReadsOnlyTheRewrittenValues(t *testing.T) {
	tests := []struct {
		name string
		a, b crdbschema.Policy
		want bool
	}{
		{name: "two empty policies", want: true},
		{name: "a policy and none", a: crdbschema.Policy{ExpirationExpression: "expires_at"}, want: false},
		{name: "identical policies", a: crdbschema.Policy{ExpirationExpression: "expires_at", JobCron: "@daily"}, b: crdbschema.Policy{ExpirationExpression: "expires_at", JobCron: "@daily"}, want: true},
		{
			// Whitespace is not formatting here: the catalog stores it.
			name: "an expression differing only in whitespace",
			a:    crdbschema.Policy{ExpirationExpression: "expires_at"}, b: crdbschema.Policy{ExpirationExpression: " expires_at "}, want: false,
		},
		{name: "an expression differing only in case", a: crdbschema.Policy{ExpirationExpression: "expires_at"}, b: crdbschema.Policy{ExpirationExpression: "EXPIRES_AT"}, want: false},
		{
			// Measured, `72 hours` reads back as `72:00:00`.
			name: "two spellings of the same interval",
			a:    crdbschema.Policy{ExpireAfter: "72 hours"}, b: crdbschema.Policy{ExpireAfter: "72:00:00"}, want: true,
		},
		{name: "an interval the server folds into another unit", a: crdbschema.Policy{ExpireAfter: "1 week"}, b: crdbschema.Policy{ExpireAfter: "7 days"}, want: true},
		{
			// A month is not thirty days, and the server keeps them apart.
			name: "two intervals that only look alike",
			a:    crdbschema.Policy{ExpireAfter: "1 mon"}, b: crdbschema.Policy{ExpireAfter: "30 days"}, want: false,
		},
		{
			// Measured, `600s` reads back as `10m0s` (stokaro/ptah#1721).
			name: "two spellings of the same poll interval",
			a:    crdbschema.Policy{ExpireAfter: "1 day", RowStatsPollInterval: "600s"}, b: crdbschema.Policy{ExpireAfter: "1 day", RowStatsPollInterval: "10m0s"}, want: true,
		},
		{name: "two different poll intervals", a: crdbschema.Policy{RowStatsPollInterval: "600s"}, b: crdbschema.Policy{RowStatsPollInterval: "11m"}, want: false},
		{name: "a poll interval on one side", a: crdbschema.Policy{RowStatsPollInterval: "600s"}, want: false},
		{name: "an interval on one side", a: crdbschema.Policy{ExpireAfter: "1 day"}, want: false},
		{name: "a count on one side", a: crdbschema.Policy{ExpirationExpression: "e", SelectBatchSize: new(int64(500))}, b: crdbschema.Policy{ExpirationExpression: "e"}, want: false},
		{name: "the same count with different values", a: crdbschema.Policy{SelectBatchSize: new(int64(500))}, b: crdbschema.Policy{SelectBatchSize: new(int64(501))}, want: false},
		{name: "a flag on one side", a: crdbschema.Policy{ExpirationExpression: "e", Pause: true}, b: crdbschema.Policy{ExpirationExpression: "e"}, want: false},
		{name: "an unreadable spelling equal to itself", a: crdbschema.Policy{ExpireAfter: "3 fortnights"}, b: crdbschema.Policy{ExpireAfter: "3 fortnights"}, want: true},
		{name: "an unreadable spelling and another", a: crdbschema.Policy{ExpireAfter: "3 fortnights"}, b: crdbschema.Policy{ExpireAfter: "6 weeks"}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ttlsql.Equivalent(test.a, test.b), qt.Equals, test.want)
			c.Assert(ttlsql.Equivalent(test.b, test.a), qt.Equals, test.want, qt.Commentf("equivalence has to be symmetric"))
		})
	}
}

// TestDropped_NamesWhatSetWouldLeaveBehind is the rule the change transition
// rests on. Measured on v26.2.5, `SET` replaces only the parameters it names: a
// table carrying ttl_job_cron and ttl_select_batch_size, given `SET
// (ttl_job_cron = '@hourly')`, keeps its batch size.
func TestDropped_NamesWhatSetWouldLeaveBehind(t *testing.T) {
	tests := []struct {
		name             string
		desired, current crdbschema.Policy
		want             []string
	}{
		{name: "nothing live, nothing dropped", desired: crdbschema.Policy{ExpirationExpression: "e"}, want: nil},
		{name: "an unchanged policy drops nothing", desired: crdbschema.Policy{ExpirationExpression: "e", JobCron: "@daily"}, current: crdbschema.Policy{ExpirationExpression: "e", JobCron: "@daily"}, want: nil},
		{name: "a count the declaration stopped naming", desired: crdbschema.Policy{ExpirationExpression: "e"}, current: crdbschema.Policy{ExpirationExpression: "e", SelectBatchSize: new(int64(500))}, want: []string{"ttl_select_batch_size"}},
		{
			// Several at once, in statement order, so the RESET text is a
			// function of the two states.
			name: "several dropped parameters come back in a fixed order", desired: crdbschema.Policy{ExpirationExpression: "e"},
			current: crdbschema.Policy{ExpirationExpression: "e", Pause: true, JobCron: "@daily", DeleteBatchSize: new(int64(100))},
			want:    []string{"ttl_job_cron", "ttl_delete_batch_size", "ttl_pause"},
		},
		{name: "a changed value is not a drop", desired: crdbschema.Policy{ExpirationExpression: "e", JobCron: "@hourly"}, current: crdbschema.Policy{ExpirationExpression: "e", JobCron: "@daily"}, want: nil},
		{name: "a dropped enabler", desired: crdbschema.Policy{ExpireAfter: "1 day"}, current: crdbschema.Policy{ExpirationExpression: "e", ExpireAfter: "1 day"}, want: []string{"ttl_expiration_expression"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ttlsql.Dropped(test.desired, test.current), qt.DeepEquals, test.want)
		})
	}
}

func change(before, after *crdbschema.Policy) *crdbdiff.RowTTL {
	result := &crdbdiff.RowTTL{}
	if before != nil {
		result.Before = &crdbschema.ObservedRowTTL{Policy: *before}
	}
	if after != nil {
		result.After = &crdbschema.DesiredRowTTL{Policy: *after}
	}
	return result
}

// TestStatements_LowersEachTransition pins the three transitions, each a
// different statement. Every one was applied to live CockroachDB v25.4.14 and
// v26.2.5 and converged.
func TestStatements_LowersEachTransition(t *testing.T) {
	tests := []struct {
		name   string
		change *crdbdiff.RowTTL
		want   []string
	}{
		{name: "adding a policy", change: change(nil, &crdbschema.Policy{ExpirationExpression: "expires_at"}), want: []string{`ALTER TABLE "t" SET (ttl_expiration_expression = 'expires_at');`}},
		{
			// `RESET (ttl)` removes the whole configuration in one statement,
			// and succeeds on a table that never had one.
			name: "removing the policy", change: change(&crdbschema.Policy{ExpirationExpression: "expires_at", JobCron: "@daily"}, nil),
			want: []string{`ALTER TABLE "t" RESET (ttl);`},
		},
		{
			name:   "changing the policy resets what it stops naming first",
			change: change(&crdbschema.Policy{ExpirationExpression: "e", JobCron: "@daily", DeleteBatchSize: new(int64(1))}, &crdbschema.Policy{ExpireAfter: "1 day"}),
			want: []string{
				`ALTER TABLE "t" RESET (ttl_expiration_expression, ttl_job_cron, ttl_delete_batch_size);`,
				`ALTER TABLE "t" SET (ttl_expire_after = '1 day');`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ttlsql.ValidateChange(test.change), qt.IsNil)
			c.Assert(ttlsql.Statements(`"t"`, test.change), qt.DeepEquals, test.want)
		})
	}
}

// TestValidateChange_FailurePath refuses a change with no operand, with an
// invalid operand, or whose operands are the same policy on the server.
func TestValidateChange_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		change  *crdbdiff.RowTTL
		wantErr string
	}{
		{name: "no change at all", change: nil, wantErr: `(?s).*requires an operand.*`},
		{name: "two absent sides", change: change(nil, nil), wantErr: `(?s).*requires an operand.*`},
		{name: "two spellings of one policy", change: change(&crdbschema.Policy{ExpireAfter: "72:00:00"}, &crdbschema.Policy{ExpireAfter: "72 hours"}), wantErr: `(?s).*operands contain no change.*`},
		{name: "an invalid declaration", change: change(nil, &crdbschema.Policy{JobCron: "@daily"}), wantErr: `(?s).*name neither.*`},
		{name: "an invalid observation", change: change(&crdbschema.Policy{JobCron: "@daily"}, nil), wantErr: `(?s).*observed row-level TTL names neither.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := ttlsql.ValidateChange(test.change)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
