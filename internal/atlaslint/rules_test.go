package atlaslint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlaslint"
)

func TestRuleForNativeCode(t *testing.T) {
	c := qt.New(t)

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
		c.Run(tt.name, func(c *qt.C) {
			got := atlaslint.RuleForNativeCode(tt.code)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

func TestNativeSuppressionTargets(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		selector string
		want     []string
	}{
		{
			name:     "destructive analyzer",
			selector: "destructive",
			want:     []string{"DS", "CD"},
		},
		{
			name:     "data dependent analyzer",
			selector: "data_depend",
			want:     []string{"DD"},
		},
		{
			name:     "concurrent index analyzer",
			selector: "concurrent_index",
			want:     []string{"PG101", "PG103"},
		},
		{
			name:     "incompatible analyzer",
			selector: "incompatible",
			want:     []string{"BC"},
		},
		{
			name:     "nested transaction analyzer",
			selector: "nestedtx",
			want:     []string{"TX201"},
		},
		{
			name:     "Atlas table drop",
			selector: "DS102",
			want:     []string{"DS101"},
		},
		{
			name:     "Atlas column drop",
			selector: "DS103",
			want:     []string{"DS102"},
		},
		{
			name:     "Atlas data dependent",
			selector: "MF103",
			want:     []string{"DD101"},
		},
		{
			name:     "overlapping unknown code",
			selector: "DS101",
			want:     nil,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got := atlaslint.NativeSuppressionTargets(tt.selector)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}
