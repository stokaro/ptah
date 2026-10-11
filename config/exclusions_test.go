package config_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/schemaext"
)

// TestCompareOptions_Exclusions pins the ignore list as the extension
// owner's comparison exclusions: every entry, in order, spelled exactly as
// IsExtensionIgnored compares it.
func TestCompareOptions_Exclusions(t *testing.T) {
	tests := []struct {
		name    string
		options *config.CompareOptions
		want    []schemaext.Exclusion
	}{
		{name: "the default list", options: config.DefaultCompareOptions(),
			want: []schemaext.Exclusion{{Kind: coverage.Extension, Name: "plpgsql"}}},
		{name: "entries kept as written", options: config.WithIgnoredExtensions(" PG_TRGM", "", "adminpack"),
			want: []schemaext.Exclusion{{Kind: coverage.Extension, Name: " PG_TRGM"}, {Kind: coverage.Extension, Name: ""}, {Kind: coverage.Extension, Name: "adminpack"}}},
		{name: "an empty list", options: config.WithIgnoredExtensions(), want: make([]schemaext.Exclusion, 0)},
		{name: "no options", options: nil, want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.options.Exclusions(), qt.DeepEquals, test.want)
		})
	}
}
