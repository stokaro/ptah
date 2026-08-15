package atlaslint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlaslint"
)

func TestRuleForNativeCode(t *testing.T) {

	tests := []struct {
		name string
		code string
		want atlaslint.Rule
	}{
		{
			name: "table drop",
			code: "DS101",
			want: atlaslint.Rule{Analyzer: atlaslint.AnalyzerDestructive, Code: "DS102"},
		},
		{
			name: "column drop",
			code: "DS102",
			want: atlaslint.Rule{Analyzer: atlaslint.AnalyzerDestructive, Code: "DS103"},
		},
		{
			name: "data dependent",
			code: "DD101",
			want: atlaslint.Rule{Analyzer: atlaslint.AnalyzerDataDependent, Code: "MF103"},
		},
		{
			name: "overlapping native fallback",
			code: "DS103",
			want: atlaslint.Rule{Analyzer: atlaslint.AnalyzerPtah, Code: "DS103"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := atlaslint.RuleForNativeCode(tt.code)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

// TestNativeSuppressionTargets pins the resolution of one Atlas nolint selector
// into native rule targets.
//
// The two collision rows are the point of the table. Native DS102 and native
// DS103 both print as DS103 on the compatibility surface, and native DD101 and
// native MF103 both print as MF103; a selector naming a printed code has to
// reach every producer of that code, not the first one. Reverting the change
// drops the second target from each of those rows, which is what let
// `-- atlas:nolint DS103` silence a DROP COLUMN while leaving an
// ALTER COLUMN ... TYPE on the very next line reported.
//
// The "DS" and "D" rows pin exactness against the pinned community binary
// (atlas community version v1.3.0): with `-- atlas:nolint DS` or
// `-- atlas:nolint D` above `ALTER TABLE t DROP COLUMN legacy;` it reports
// DS103 and exits 1, so a code selector must never widen into a family. Give
// the code fallback Family:true and both rows silence the whole DS family,
// which is exit 0 where the community binary exits 1.
func TestNativeSuppressionTargets(t *testing.T) {

	tests := []struct {
		name     string
		selector string
		want     []atlaslint.Target
	}{
		{
			name:     "destructive analyzer",
			selector: "destructive",
			want:     []atlaslint.Target{atlaslint.FamilyTarget("DS"), atlaslint.FamilyTarget("CD")},
		},
		{
			name:     "data dependent analyzer",
			selector: "data_depend",
			want:     []atlaslint.Target{atlaslint.FamilyTarget("DD")},
		},
		{
			name:     "concurrent index analyzer",
			selector: "concurrent_index",
			want:     []atlaslint.Target{atlaslint.CodeTarget("PG101"), atlaslint.CodeTarget("PG103")},
		},
		{
			name:     "incompatible analyzer",
			selector: "incompatible",
			want:     []atlaslint.Target{atlaslint.FamilyTarget("BC")},
		},
		{
			name:     "nested transaction analyzer",
			selector: "nestedtx",
			want:     []atlaslint.Target{atlaslint.CodeTarget("TX201")},
		},
		{
			name:     "Atlas table drop",
			selector: "DS102",
			want:     []atlaslint.Target{atlaslint.CodeTarget("DS101")},
		},
		{
			name:     "Atlas column drop reaches both producers",
			selector: "DS103",
			want:     []atlaslint.Target{atlaslint.CodeTarget("DS102"), atlaslint.CodeTarget("DS103")},
		},
		{
			name:     "Atlas data dependent reaches both producers",
			selector: "MF103",
			want:     []atlaslint.Target{atlaslint.CodeTarget("DD101"), atlaslint.CodeTarget("MF103")},
		},
		{
			name:     "code the compatibility surface prints unchanged",
			selector: "PG101",
			want:     []atlaslint.Target{atlaslint.CodeTarget("PG101")},
		},
		{
			name:     "code remapped away is never printed",
			selector: "DS101",
			want:     nil,
		},
		{
			name:     "native code remapped away is never printed",
			selector: "DD101",
			want:     nil,
		},
		{
			name:     "family prefix is not a code",
			selector: "DS",
			want:     []atlaslint.Target{atlaslint.CodeTarget("DS")},
		},
		{
			name:     "single letter is not a code",
			selector: "D",
			want:     []atlaslint.Target{atlaslint.CodeTarget("D")},
		},
		{
			name:     "unknown selector resolves to a target matching nothing",
			selector: "totally_bogus_selector",
			want:     []atlaslint.Target{atlaslint.CodeTarget("TOTALLY_BOGUS_SELECTOR")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := atlaslint.NativeSuppressionTargets(tt.selector)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

// TestTargetMatches pins the difference the Target type exists to carry: a
// family target widens to every code sharing its prefix, an exact target does
// not. Collapse the two and `-- atlas:nolint DS` starts silencing DS101.
func TestTargetMatches(t *testing.T) {

	tests := []struct {
		name   string
		target atlaslint.Target
		code   string
		want   bool
	}{
		{name: "family covers its prefix", target: atlaslint.FamilyTarget("DS"), code: "DS101", want: true},
		{name: "family stops at its prefix", target: atlaslint.FamilyTarget("DS"), code: "DD101", want: false},
		{name: "empty family covers everything", target: atlaslint.FamilyTarget(""), code: "PG101", want: true},
		{name: "exact code matches itself", target: atlaslint.CodeTarget("DS101"), code: "DS101", want: true},
		{name: "exact code does not widen", target: atlaslint.CodeTarget("DS"), code: "DS101", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tt.target.Matches(tt.code), qt.Equals, tt.want)
		})
	}
}

// TestTargetMatchesEveryRule pins which target the bare directive produces:
// only the empty family means "every rule". A code target that happens to be
// empty must not, or a directive whose selector parsed to nothing would mark a
// whole file ignored.
func TestTargetMatchesEveryRule(t *testing.T) {

	tests := []struct {
		name   string
		target atlaslint.Target
		want   bool
	}{
		{name: "empty family", target: atlaslint.FamilyTarget(""), want: true},
		{name: "named family", target: atlaslint.FamilyTarget("DS"), want: false},
		{name: "empty code", target: atlaslint.CodeTarget(""), want: false},
		{name: "named code", target: atlaslint.CodeTarget("DS101"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tt.target.MatchesEveryRule(), qt.Equals, tt.want)
		})
	}
}

// TestAnalyzerSuppressionTargets pins which selectors are analyzer names. The
// ok flag is what lets the native surface resolve an Atlas analyzer name while
// still reading a code selector in its own namespace; drop it and
// `-- atlas:nolint concurrent_index` stops silencing PG101 on
// `ptah migrations lint`.
func TestAnalyzerSuppressionTargets(t *testing.T) {

	tests := []struct {
		name     string
		selector string
		want     []atlaslint.Target
		wantOK   bool
	}{
		{
			name:     "analyzer name",
			selector: "concurrent_index",
			want:     []atlaslint.Target{atlaslint.CodeTarget("PG101"), atlaslint.CodeTarget("PG103")},
			wantOK:   true,
		},
		{
			name:     "analyzer name uppercased by the directive parser",
			selector: "DESTRUCTIVE",
			want:     []atlaslint.Target{atlaslint.FamilyTarget("DS"), atlaslint.FamilyTarget("CD")},
			wantOK:   true,
		},
		{
			name:     "diagnostic code is not an analyzer name",
			selector: "DS103",
			want:     nil,
			wantOK:   false,
		},
		{
			name:     "unknown selector is not an analyzer name",
			selector: "totally_bogus_selector",
			want:     nil,
			wantOK:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := atlaslint.AnalyzerSuppressionTargets(tt.selector)
			c.Assert(ok, qt.Equals, tt.wantOK)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}
