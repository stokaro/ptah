package ydbcoordination_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/ydb/ydbcoordination"
)

func TestParseDeclaration_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   ydbcoordination.Spec
	}{
		{name: "no setting", values: map[string]string{"name": "locks"}},
		{
			name: "every setting",
			values: map[string]string{
				"self_check_period": "PT2S", "session_grace_period": "PT30S",
				"read_consistency_mode": "strict", "attach_consistency_mode": "relaxed",
				"rate_limiter_counters_mode": "detailed",
			},
			want: ydbcoordination.Spec{
				SelfCheckPeriodMillis: 2000, SessionGracePeriodMillis: 30000,
				ReadConsistencyMode: "strict", AttachConsistencyMode: "relaxed", RateLimiterCountersMode: "detailed",
			},
		},
		{
			name:   "a fraction of a second",
			values: map[string]string{"self_check_period": "PT0.75S"},
			want:   ydbcoordination.Spec{SelfCheckPeriodMillis: 750},
		},
		{
			name:   "a mode in capitals",
			values: map[string]string{"read_consistency_mode": " STRICT "},
			want:   ydbcoordination.Spec{ReadConsistencyMode: "strict"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbcoordination.ParseDeclaration(test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestParseDeclaration_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		values  map[string]string
		wantErr string
	}{
		{name: "not a duration", values: map[string]string{"self_check_period": "1s"},
			wantErr: `self_check_period "1s": a period is an ISO 8601 duration such as PT1S or PT0.5S`},
		{name: "a negative duration", values: map[string]string{"session_grace_period": "-PT5S"},
			wantErr: `session_grace_period "-PT5S": a period is longer than zero`},
		{name: "a zero duration", values: map[string]string{"self_check_period": "PT0S"},
			wantErr: `self_check_period "PT0S": a period is longer than zero`},
		{name: "a fraction of a millisecond", values: map[string]string{"self_check_period": "PT0.5005S"},
			wantErr: `self_check_period "PT0.5005S": the node keeps a period in whole milliseconds`},
		{name: "a period too long for the node", values: map[string]string{"session_grace_period": "P50D"},
			wantErr: `session_grace_period "P50D": the node keeps a period of at most 4294967295 milliseconds`},
		{name: "an empty mode", values: map[string]string{"attach_consistency_mode": " "},
			wantErr: `attach_consistency_mode is empty: leave the setting out to keep the node's default`},
		{name: "an unknown mode", values: map[string]string{"rate_limiter_counters_mode": "per-resource"},
			wantErr: `rate_limiter_counters_mode "per-resource": the mode is "aggregated" or "detailed"`},
		{name: "a combination the node would not run", values: map[string]string{"self_check_period": "PT10S"},
			wantErr: `session_grace_period PT10S: .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbcoordination.ParseDeclaration(test.values)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, ydbcoordination.Spec{})
		})
	}
}

func TestPeriodText_IsWhatParsePeriodReads(t *testing.T) {
	tests := []struct {
		millis uint32
		want   string
	}{
		{millis: 500, want: "PT0.5S"},
		{millis: 1000, want: "PT1S"},
		{millis: 1001, want: "PT1.001S"},
		{millis: 90000, want: "PT1M30S"},
		{millis: 4294967295, want: "P49DT17H2M47.295S"},
	}
	for _, test := range tests {
		t.Run(test.want, func(t *testing.T) {
			c := qt.New(t)
			text := ydbcoordination.PeriodText(test.millis)
			c.Assert(text, qt.Equals, test.want)
			read, err := ydbcoordination.ParsePeriod(text)
			c.Assert(err, qt.IsNil)
			c.Assert(read, qt.Equals, test.millis)
		})
	}
}

func TestAttributes_WritesTheSettingsTheSpecSets(t *testing.T) {
	c := qt.New(t)
	got := ydbcoordination.Attributes(ydbcoordination.Spec{
		SelfCheckPeriodMillis: 1500, ReadConsistencyMode: "strict", RateLimiterCountersMode: "detailed",
	})
	c.Assert(got, qt.DeepEquals, [][2]string{
		{"self_check_period", "PT1.5S"},
		{"read_consistency_mode", "strict"},
		{"rate_limiter_counters_mode", "detailed"},
	})
}
