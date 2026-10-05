package ydbcoordination_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbcoordination"
)

func TestEffective_FillsWhatTheSpecLeavesUnset(t *testing.T) {
	tests := []struct {
		name string
		spec ast.CoordinationNodeSpec
		want ast.CoordinationNodeSpec
	}{
		{
			name: "nothing declared runs with the tablet's defaults",
			want: ast.CoordinationNodeSpec{
				SelfCheckPeriodMillis: 1000, SessionGracePeriodMillis: 10000,
				ReadConsistencyMode: "relaxed", AttachConsistencyMode: "strict", RateLimiterCountersMode: "aggregated",
			},
		},
		{
			name: "a declared setting is kept",
			spec: ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 2500, ReadConsistencyMode: "strict"},
			want: ast.CoordinationNodeSpec{
				SelfCheckPeriodMillis: 2500, SessionGracePeriodMillis: 10000,
				ReadConsistencyMode: "strict", AttachConsistencyMode: "strict", RateLimiterCountersMode: "aggregated",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbcoordination.Effective(test.spec), qt.Equals, test.want)
		})
	}
}

func TestChanges_NamesWhatTheNodeRunsWithDifferently(t *testing.T) {
	tests := []struct {
		name             string
		desired, current ast.CoordinationNodeSpec
		want             ast.CoordinationNodeSpec
	}{
		{name: "both unset"},
		{
			name:    "a setting at its default against one left unset",
			desired: ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 1000, AttachConsistencyMode: "strict"},
		},
		{
			name:    "one left unset against a setting at its default",
			current: ast.CoordinationNodeSpec{SessionGracePeriodMillis: 10000, RateLimiterCountersMode: "aggregated"},
		},
		{
			name:    "one changed setting",
			desired: ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 3000, ReadConsistencyMode: "strict"},
			current: ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 2500, ReadConsistencyMode: "strict"},
			want:    ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 3000},
		},
		{
			name:    "a setting the declaration leaves out goes back to its default",
			current: ast.CoordinationNodeSpec{SessionGracePeriodMillis: 20000, RateLimiterCountersMode: "detailed"},
			want:    ast.CoordinationNodeSpec{SessionGracePeriodMillis: 10000, RateLimiterCountersMode: "aggregated"},
		},
		{
			name: "every setting",
			desired: ast.CoordinationNodeSpec{
				SelfCheckPeriodMillis: 2000, SessionGracePeriodMillis: 15000,
				ReadConsistencyMode: "strict", AttachConsistencyMode: "relaxed", RateLimiterCountersMode: "detailed",
			},
			want: ast.CoordinationNodeSpec{
				SelfCheckPeriodMillis: 2000, SessionGracePeriodMillis: 15000,
				ReadConsistencyMode: "strict", AttachConsistencyMode: "relaxed", RateLimiterCountersMode: "detailed",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbcoordination.Changes(test.desired, test.current), qt.Equals, test.want)
		})
	}
}

func TestMerge_ReplacesTheSettingsAChangeNames(t *testing.T) {
	c := qt.New(t)
	current := ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 2500, SessionGracePeriodMillis: 15000,
		ReadConsistencyMode: "relaxed"}
	changes := ast.CoordinationNodeSpec{SessionGracePeriodMillis: 20000, AttachConsistencyMode: "relaxed",
		RateLimiterCountersMode: "detailed"}

	got := ydbcoordination.Merge(current, changes)

	c.Assert(got, qt.Equals, ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 2500, SessionGracePeriodMillis: 20000,
		ReadConsistencyMode: "relaxed", AttachConsistencyMode: "relaxed", RateLimiterCountersMode: "detailed"})
}

func TestValidate_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		spec ast.CoordinationNodeSpec
	}{
		{name: "nothing declared"},
		{name: "the shortest self-check period", spec: ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 500}},
		{name: "the longest self-check period with a grace period above it",
			spec: ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 10000, SessionGracePeriodMillis: 11000}},
		{name: "the shortest grace period after the default self-check",
			spec: ast.CoordinationNodeSpec{SessionGracePeriodMillis: 2000}},
		{name: "the longest grace period", spec: ast.CoordinationNodeSpec{SessionGracePeriodMillis: 30000}},
		{name: "every mode", spec: ast.CoordinationNodeSpec{ReadConsistencyMode: "strict",
			AttachConsistencyMode: "relaxed", RateLimiterCountersMode: "detailed"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbcoordination.Validate(test.spec), qt.IsNil)
		})
	}
}

func TestValidate_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		spec    ast.CoordinationNodeSpec
		wantErr string
	}{
		{
			name:    "a self-check period the node raises",
			spec:    ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 499},
			wantErr: `self_check_period PT0.499S: YDB runs a node's self-check every PT0.5S to PT10S .*`,
		},
		{
			name:    "a self-check period the node lowers",
			spec:    ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 10001, SessionGracePeriodMillis: 30000},
			wantErr: `self_check_period PT10.001S: .*`,
		},
		{
			name:    "a self-check period that leaves the default grace period too short",
			spec:    ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 10000},
			wantErr: `session_grace_period PT10S: YDB runs a node with a grace period from the self-check period plus PT1S \(PT11S here\) to PT30S .*`,
		},
		{
			name:    "a grace period too close to the self-check period",
			spec:    ast.CoordinationNodeSpec{SelfCheckPeriodMillis: 2000, SessionGracePeriodMillis: 2999},
			wantErr: `session_grace_period PT2.999S: .*\(PT3S here\).*`,
		},
		{
			name:    "a grace period the node lowers",
			spec:    ast.CoordinationNodeSpec{SessionGracePeriodMillis: 30001},
			wantErr: `session_grace_period PT30.001S: .*`,
		},
		{
			name:    "an unknown read mode",
			spec:    ast.CoordinationNodeSpec{ReadConsistencyMode: "eventual"},
			wantErr: `read_consistency_mode "eventual": the mode is "strict" or "relaxed"`,
		},
		{
			name:    "an unknown attach mode",
			spec:    ast.CoordinationNodeSpec{AttachConsistencyMode: "STRICT"},
			wantErr: `attach_consistency_mode "STRICT": the mode is "strict" or "relaxed"`,
		},
		{
			name:    "an unknown counters mode",
			spec:    ast.CoordinationNodeSpec{RateLimiterCountersMode: "none"},
			wantErr: `rate_limiter_counters_mode "none": the mode is "aggregated" or "detailed"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbcoordination.Validate(test.spec), qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestRefuseName_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		schema, node string
	}{
		{name: "a node at the root", node: "locks"},
		{name: "a node in a directory", schema: "app", node: "locks"},
		{name: "the lock node's name in a directory", schema: "app", node: "ptah_locks"},
		{name: "a dotted name", node: "app.locks"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbcoordination.RefuseName(test.schema, test.node), qt.IsNil)
		})
	}
}

func TestRefuseName_FailurePath(t *testing.T) {
	tests := []struct {
		name         string
		schema, node string
		wantErr      string
	}{
		{name: "no name", wantErr: `a coordination node needs a name`},
		{name: "Ptah's lock node", node: "ptah_locks",
			wantErr: `coordination node ptah_locks at the database root holds Ptah's own locks, .*`},
		{name: "Ptah's lock node behind a root slash", schema: "/", node: "ptah_locks",
			wantErr: `coordination node ptah_locks at the database root .*`},
		{name: "a name that starts with a dot", node: ".hidden",
			wantErr: `coordination node .hidden has the path segment ".hidden"; .*`},
		{name: "a directory that starts with a dot", schema: ".sys", node: "locks",
			wantErr: `coordination node .sys/locks has the path segment ".sys"; .*`},
		{name: "a parent segment", schema: "app/..", node: "ptah_locks",
			wantErr: `coordination node app/../ptah_locks has the path segment ".."; .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbcoordination.RefuseName(test.schema, test.node), qt.ErrorMatches, test.wantErr)
		})
	}
}
