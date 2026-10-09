package crdbschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

func TestDecodeDeclared_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		parameters map[string]string
		want       crdbschema.Policy
	}{
		{
			name:       "the enabler alone, which is the reproducer of stokaro/ptah#1027",
			parameters: map[string]string{"ttl_expiration_expression": "expires_at"},
			want:       crdbschema.Policy{ExpirationExpression: "expires_at"},
		},
		{
			// The text is kept verbatim; only the comparison reads it as an
			// interval (stokaro/ptah#1605).
			name:       "an interval spelling the server will respell",
			parameters: map[string]string{"ttl_expire_after": "72 hours"},
			want:       crdbschema.Policy{ExpireAfter: "72 hours"},
		},
		{
			// The expression is arbitrary SQL and is kept verbatim, because the
			// catalog keeps it verbatim: measured, whitespace, case, casts and
			// parentheses all survive a round trip unchanged.
			name:       "an expression carrying a quote, a cast and spacing",
			parameters: map[string]string{"ttl_expiration_expression": "  (expires_at)::TIMESTAMPTZ + INTERVAL '1 day'  "},
			want:       crdbschema.Policy{ExpirationExpression: "  (expires_at)::TIMESTAMPTZ + INTERVAL '1 day'  "},
		},
		{
			name: "every managed parameter",
			parameters: map[string]string{
				"ttl_expiration_expression": "expires_at", "ttl_expire_after": "3 days", "ttl_row_stats_poll_interval": "10m",
				"ttl_job_cron": "@daily", "ttl_select_batch_size": "500", "ttl_delete_batch_size": " 100 ",
				"ttl_select_rate_limit": "200", "ttl_delete_rate_limit": "300", "ttl_pause": "true",
				"ttl_label_metrics": "TRUE", "ttl_disable_changefeed_replication": "1",
			},
			want: crdbschema.Policy{
				ExpirationExpression: "expires_at", ExpireAfter: "3 days", RowStatsPollInterval: "10m", JobCron: "@daily",
				SelectBatchSize: new(int64(500)), DeleteBatchSize: new(int64(100)), SelectRateLimit: new(int64(200)),
				DeleteRateLimit: new(int64(300)), Pause: true, LabelMetrics: true, DisableChangefeedReplication: true,
			},
		},
		{
			// A false flag is the engine's default and is stored nowhere, so it
			// declares the default. Keeping it would make every declaration
			// differ from every read of the table it describes.
			name: "false flags declare the default",
			parameters: map[string]string{
				"ttl_expiration_expression": "expires_at", "ttl_pause": "false",
				"ttl_label_metrics": "false", "ttl_disable_changefeed_replication": "0",
			},
			want: crdbschema.Policy{ExpirationExpression: "expires_at"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared, err := crdbschema.DecodeDeclared(test.parameters)
			c.Assert(err, qt.IsNil)
			c.Assert(declared.Policy, qt.DeepEquals, test.want)
		})
	}
}

func TestDecodeDeclared_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		parameters map[string]string
		wantErr    string
	}{
		{
			name:       "the derived marker, which the server refuses on its own",
			parameters: map[string]string{"ttl": "on"},
			wantErr:    `(?s).*ttl is derived from the other parameters.*`,
		},
		{
			// A typo lists the surface rather than failing generically.
			name:       "a misspelled parameter",
			parameters: map[string]string{"ttl_expiration_expresion": "expires_at"},
			wantErr:    `(?s).*unknown row-level TTL parameter "ttl_expiration_expresion": Ptah manages ttl_expiration_expression, ttl_expire_after, .*`,
		},
		{
			name:       "a count that is not an integer",
			parameters: map[string]string{"ttl_expiration_expression": "expires_at", "ttl_select_batch_size": "many"},
			wantErr:    `(?s).*ttl_select_batch_size = "many", which is not an integer.*`,
		},
		{
			name:       "a flag that is not a boolean",
			parameters: map[string]string{"ttl_expiration_expression": "expires_at", "ttl_pause": "maybe"},
			wantErr:    `(?s).*ttl_pause = "maybe", which is not true or false.*`,
		},
		{
			name:       "an interval this owner cannot read",
			parameters: map[string]string{"ttl_expire_after": "3 fortnights"},
			wantErr:    `(?s).*unknown unit "fortnights".*`,
		},
		{
			name:       "a knob without an enabler",
			parameters: map[string]string{"ttl_job_cron": "@daily"},
			wantErr:    `(?s).*name neither ttl_expiration_expression nor ttl_expire_after.*`,
		},
		{
			name:       "no parameters at all",
			parameters: make(map[string]string),
			wantErr:    `(?s).*name neither ttl_expiration_expression nor ttl_expire_after.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared, err := crdbschema.DecodeDeclared(test.parameters)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(declared, qt.IsNil)
		})
	}
}

// TestDecodeDeclared_IsDeterministic guards the diagnostic against map
// iteration order. A declaration with several problems must report the same
// one on every run, or no test can pin it and no reader can trust it.
func TestDecodeDeclared_IsDeterministic(t *testing.T) {
	c := qt.New(t)

	parameters := map[string]string{
		"ttl_expire_after": "3 fortnights", "ttl_row_stats_poll_interval": "10m",
		"ttl_select_batch_size": "nope", "ttl_pause": "maybe",
	}
	_, first := crdbschema.DecodeDeclared(parameters)
	c.Assert(first, qt.IsNotNil)
	for range 20 {
		_, again := crdbschema.DecodeDeclared(parameters)
		c.Assert(again.Error(), qt.Equals, first.Error())
	}
}

// TestDecodeStored_ReadsWhatTheCatalogKeeps pins the lenient read: unknown
// names are ignored, an unreadable count is left unset rather than recorded as
// a zero nobody stored, and a policy with no managed parameter is none.
func TestDecodeStored_ReadsWhatTheCatalogKeeps(t *testing.T) {
	tests := []struct {
		name       string
		parameters map[string]string
		want       *crdbschema.ObservedRowTTL
	}{
		{name: "nothing stored", parameters: nil, want: nil},
		{name: "only parameters the owner does not model", parameters: map[string]string{"schema_locked": "true", "ttl": "on"}, want: nil},
		{
			name:       "the server's spellings, kept verbatim",
			parameters: map[string]string{"ttl": "on", "ttl_expire_after": "72:00:00", "ttl_row_stats_poll_interval": "10m0s", "schema_locked": "true"},
			want:       &crdbschema.ObservedRowTTL{Policy: crdbschema.Policy{ExpireAfter: "72:00:00", RowStatsPollInterval: "10m0s"}},
		},
		{
			name:       "an unreadable count and a false flag",
			parameters: map[string]string{"ttl_expiration_expression": "expires_at", "ttl_select_batch_size": "x", "ttl_pause": "false"},
			want:       &crdbschema.ObservedRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "expires_at"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(crdbschema.DecodeStored(test.parameters), qt.DeepEquals, test.want)
		})
	}
}
