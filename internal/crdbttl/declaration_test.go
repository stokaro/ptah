package crdbttl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/internal/crdbttl"
)

func TestFromAttributes_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		attributes map[string]string
		want       *ast.RowTTLSpec
	}{
		{
			name:       "no ttl attributes at all",
			attributes: map[string]string{"name": "sessions", "comment": "x"},
			want:       nil,
		},
		{
			name:       "the enabler alone, which is the issue's reproducer",
			attributes: map[string]string{"ttl_expiration_expression": "expires_at"},
			want:       &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
		},
		{
			// The other enabler, added by stokaro/ptah#1605. The text is kept
			// verbatim; only the COMPARISON reads it as an interval.
			name:       "the interval enabler",
			attributes: map[string]string{"ttl_expire_after": "3 days"},
			want:       &ast.RowTTLSpec{ExpireAfter: "3 days"},
		},
		{
			name:       "an interval spelling the server will normalize",
			attributes: map[string]string{"ttl_expire_after": "72 hours"},
			want:       &ast.RowTTLSpec{ExpireAfter: "72 hours"},
		},
		{
			name: "both enablers, which the server accepts together",
			attributes: map[string]string{
				"ttl_expiration_expression": "expires_at",
				"ttl_expire_after":          "3 days",
			},
			want: &ast.RowTTLSpec{ExpirationExpression: "expires_at", ExpireAfter: "3 days"},
		},
		{
			// The expression is arbitrary SQL and is kept verbatim, because the
			// catalog keeps it verbatim: measured, whitespace, case, casts and
			// parentheses all survive a round trip unchanged.
			name:       "an expression carrying a quote, a cast and spacing",
			attributes: map[string]string{"ttl_expiration_expression": "  (expires_at)::TIMESTAMPTZ + INTERVAL '1 day'  "},
			want:       &ast.RowTTLSpec{ExpirationExpression: "  (expires_at)::TIMESTAMPTZ + INTERVAL '1 day'  "},
		},
		{
			name: "every managed parameter",
			attributes: map[string]string{
				"ttl_expiration_expression":          "expires_at",
				"ttl_job_cron":                       "@daily",
				"ttl_select_batch_size":              "500",
				"ttl_delete_batch_size":              "100",
				"ttl_select_rate_limit":              "200",
				"ttl_delete_rate_limit":              "300",
				"ttl_pause":                          "true",
				"ttl_label_metrics":                  "true",
				"ttl_disable_changefeed_replication": "true",
			},
			want: &ast.RowTTLSpec{
				ExpirationExpression:         "expires_at",
				JobCron:                      "@daily",
				SelectBatchSize:              new(int64(500)),
				DeleteBatchSize:              new(int64(100)),
				SelectRateLimit:              new(int64(200)),
				DeleteRateLimit:              new(int64(300)),
				Pause:                        new(true),
				LabelMetrics:                 new(true),
				DisableChangefeedReplication: new(true),
			},
		},
		{
			// A false boolean is the engine's default AND is stored nowhere, so
			// it normalizes to not-declared. Keeping it would make every
			// declaration differ from every read of the table it describes.
			name: "false booleans normalize away",
			attributes: map[string]string{
				"ttl_expiration_expression":          "expires_at",
				"ttl_pause":                          "false",
				"ttl_label_metrics":                  "false",
				"ttl_disable_changefeed_replication": "false",
			},
			want: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
		},
		{
			name:       "attribute names are matched case-insensitively",
			attributes: map[string]string{"TTL_Expiration_Expression": "expires_at"},
			want:       &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			spec, err := crdbttl.FromAttributes("sessions", test.attributes)

			c.Assert(err, qt.IsNil)
			c.Assert(spec, qt.DeepEquals, test.want)
		})
	}
}

func TestFromAttributes_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		attributes map[string]string
		wantErr    string
	}{
		{
			// The value is refused, not the parameter: an interval Ptah cannot
			// read would be sent, normalized by the server into a spelling
			// nothing here predicts, and re-issued on every run.
			name:       "an interval this surface cannot read",
			attributes: map[string]string{"ttl_expire_after": "3 fortnights"},
			wantErr:    `(?s).*unknown unit "fortnights".*`,
		},
		{
			// Measured: the server truncates to whole seconds and then stores
			// no parameter at all when that leaves zero, so a table declared
			// with this would report the setting missing on every inspection.
			name:       "a poll interval the server would truncate to nothing",
			attributes: map[string]string{"ttl_expire_after": "1 hour", "ttl_row_stats_poll_interval": "500ms"},
			wantErr:    `(?s).*declares ttl_row_stats_poll_interval:.*below one second.*`,
		},
		{
			name:       "a negative poll interval, which the server refuses",
			attributes: map[string]string{"ttl_expire_after": "1 hour", "ttl_row_stats_poll_interval": "-5s"},
			wantErr:    `(?s).*declares ttl_row_stats_poll_interval:.*is negative.*`,
		},
		{
			// Past this the server wraps and answers that the interval must be
			// at least 0, naming the sign of a number nobody wrote.
			name:       "a poll interval past the largest duration the server holds",
			attributes: map[string]string{"ttl_expire_after": "1 hour", "ttl_row_stats_poll_interval": "2562048h"},
			wantErr:    `(?s).*declares ttl_row_stats_poll_interval:.*is longer than.*`,
		},
		{
			name:       "the derived marker, which the server refuses on its own",
			attributes: map[string]string{"ttl": "on"},
			wantErr:    `(?s).*declares ttl: it is derived from the other parameters.*`,
		},
		{
			// A typo lists the surface rather than failing generically, which
			// is the difference between "you misspelled something" and "here is
			// what you may write".
			name:       "a misspelled parameter",
			attributes: map[string]string{"ttl_expiration_expresion": "expires_at"},
			wantErr:    `(?s).*unknown row-level TTL attribute "ttl_expiration_expresion": Ptah manages ttl_expiration_expression, ttl_expire_after, .*`,
		},
		{
			name:       "an integer knob that is not an integer",
			attributes: map[string]string{"ttl_expiration_expression": "expires_at", "ttl_select_batch_size": "many"},
			wantErr:    `(?s).*declares ttl_select_batch_size = "many", which is not an integer.*`,
		},
		{
			name:       "a boolean knob that is not a boolean",
			attributes: map[string]string{"ttl_expiration_expression": "expires_at", "ttl_pause": "maybe"},
			wantErr:    `(?s).*declares ttl_pause = "maybe", which is not true or false.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := crdbttl.FromAttributes("sessions", test.attributes)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestFromAttributes_IsDeterministic guards the diagnostic against map
// iteration order. Attributes arrive in a map, and a declaration with two
// problems must report the same one on every run or no test can pin it and no
// reader can trust it.
func TestFromAttributes_IsDeterministic(t *testing.T) {
	c := qt.New(t)

	attributes := map[string]string{
		"ttl_expire_after":            "3 fortnights",
		"ttl_row_stats_poll_interval": "10m",
		"ttl_select_batch_size":       "nope",
		"ttl_pause":                   "maybe",
	}

	_, first := crdbttl.FromAttributes("sessions", attributes)
	c.Assert(first, qt.IsNotNil)

	for range 20 {
		_, again := crdbttl.FromAttributes("sessions", attributes)
		c.Assert(again, qt.IsNotNil)
		c.Assert(again.Error(), qt.Equals, first.Error())
	}
}
