package crdbttl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/internal/crdbttl"
)

// TestFromReloptions_ReadsWhatTheCatalogHolds pins the decoder against the
// exact array elements CockroachDB reports.
//
// Every input below is a transcription of what `SELECT unnest(reloptions)`
// returned on a live server, not a shape invented here. That matters most for
// the escape-string rows: an expression containing a quote comes back as
// `e'... \' ...'` with BACKSLASH escaping rather than doubled quotes, and a
// decoder that knew only the doubled form would silently corrupt the most
// common non-trivial expression there is.
func TestFromReloptions_ReadsWhatTheCatalogHolds(t *testing.T) {
	tests := []struct {
		name    string
		options []string
		want    *ast.RowTTLSpec
	}{
		{
			name:    "a table with no storage parameters at all",
			options: nil,
			want:    nil,
		},
		{
			// v25.4.14 stores nothing beside the TTL; v26.2.5 adds
			// schema_locked to every table. Neither is a TTL parameter and
			// both must be ignored rather than refused.
			name:    "only parameters this package does not model",
			options: []string{"schema_locked=true", "fillfactor=70"},
			want:    nil,
		},
		{
			name:    "the shape v26.2.5 reports for the issue's reproducer",
			options: []string{"ttl='on'", "ttl_expiration_expression='expires_at'", "schema_locked=true"},
			want:    &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
		},
		{
			name:    "the same table on v25.4.14, which adds no schema_locked",
			options: []string{"ttl='on'", "ttl_expiration_expression='expires_at'"},
			want:    &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
		},
		{
			// Measured: this is the element for the declaration
			// `expires_at + INTERVAL '1 day'`.
			name: "an expression carrying a quote, in the escape-string form",
			options: []string{
				"ttl='on'",
				`ttl_expiration_expression=e'expires_at + INTERVAL \'1 day\''`,
				"schema_locked=true",
			},
			want: &ast.RowTTLSpec{ExpirationExpression: "expires_at + INTERVAL '1 day'"},
		},
		{
			name: "every managed parameter",
			options: []string{
				"ttl='on'",
				"ttl_expiration_expression='expires_at'",
				"ttl_job_cron='@daily'",
				"ttl_select_batch_size=500",
				"ttl_delete_batch_size=100",
				"ttl_select_rate_limit=200",
				"ttl_delete_rate_limit=300",
				"ttl_pause=true",
				"ttl_label_metrics=true",
				"ttl_disable_changefeed_replication=true",
				"schema_locked=true",
			},
			want: &ast.RowTTLSpec{
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
		},
		{
			// The parameters Ptah refuses to DECLARE are still ignored rather
			// than refused when READ: a table someone configured outside Ptah
			// is state to describe, not a declaration to reject.
			name: "a table configured with a parameter Ptah does not model",
			options: []string{
				"ttl='on'",
				"ttl_expire_after='3 days':::INTERVAL",
				"ttl_row_stats_poll_interval='10m0s'",
			},
			want: nil,
		},
		{
			// Half-modeled is the dangerous middle: the enabler is one Ptah
			// does not model, so the knob beside it must not be read as a
			// policy Ptah manages.
			name: "an unmodeled enabler beside a modeled knob",
			options: []string{
				"ttl='on'",
				"ttl_expire_after='3 days':::INTERVAL",
				"ttl_job_cron='@daily'",
			},
			want: &ast.RowTTLSpec{JobCron: "@daily"},
		},
		{
			name:    "a malformed element with no equals sign is skipped",
			options: []string{"ttl='on'", "garbage", "ttl_expiration_expression='expires_at'"},
			want:    &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(crdbttl.FromReloptions(test.options), qt.DeepEquals, test.want)
		})
	}
}

// TestFromReloptions_RoundTripsWhatOptionsRendered is the assertion the whole
// convergence guarantee rests on: what Ptah writes into a statement has to be
// what it reads back out of the catalog, or the plan never empties.
//
// It is written as a round trip through BOTH functions rather than as two
// independent expectations, because a defect here is by definition a
// disagreement between them, and each side looks right on its own.
func TestFromReloptions_RoundTripsWhatOptionsRendered(t *testing.T) {
	tests := []struct {
		name string
		spec *ast.RowTTLSpec
	}{
		{
			name: "the enabler alone",
			spec: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
		},
		{
			name: "an expression carrying a quote",
			spec: &ast.RowTTLSpec{ExpirationExpression: "expires_at + INTERVAL '1 day'"},
		},
		{
			name: "an expression carrying whitespace and a cast",
			spec: &ast.RowTTLSpec{ExpirationExpression: "  (expires_at)::TIMESTAMPTZ  "},
		},
		{
			name: "every managed parameter",
			spec: &ast.RowTTLSpec{
				ExpirationExpression:         "expires_at",
				JobCron:                      "0 3 * * *",
				SelectBatchSize:              int64p(500),
				DeleteBatchSize:              int64p(100),
				SelectRateLimit:              int64p(200),
				DeleteRateLimit:              int64p(300),
				Pause:                        boolp(true),
				LabelMetrics:                 boolp(true),
				DisableChangefeedReplication: boolp(true),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			options := crdbttl.Options(test.spec)
			elements := make([]string, 0, len(options)+1)
			elements = append(elements, "ttl='on'")
			for _, option := range options {
				elements = append(elements, catalogElement(option))
			}

			c.Assert(crdbttl.FromReloptions(elements), qt.DeepEquals, test.spec)
			c.Assert(crdbttl.Equal(crdbttl.FromReloptions(elements), test.spec), qt.IsTrue)
		})
	}
}

// catalogElement is what the catalog holds after a statement carrying one
// rendered option runs, and it exists because Options and FromReloptions are
// NOT inverses of each other -- the server sits between them and rewrites the
// quoting.
//
// Measured on v26.2.5: a declared expression containing a quote reaches the
// server as SQL's doubled-quote form and is stored as an escape-string literal
// with backslashes, while a value carrying no quote of its own is stored
// exactly as the statement spelled it. The first test in this file transcribes
// both catalog spellings directly.
//
// Modeling that transformation here rather than feeding Options straight into
// FromReloptions is the difference between testing the round trip that exists
// and one that does not: the doubled form never reaches the catalog, and the
// escape-string form never reaches a statement.
func catalogElement(option crdbttl.Option) string {
	if !strings.Contains(option.Value, "''") {
		return option.Name + "=" + option.Value
	}
	// Exactly one delimiter comes off each end. strings.Trim would eat the
	// doubled quote that is part of the value when the expression ENDS with one.
	unquoted := strings.ReplaceAll(option.Value[1:len(option.Value)-1], "''", "'")
	escaped := strings.ReplaceAll(unquoted, "'", "\\'")
	return option.Name + "=e'" + escaped + "'"
}
