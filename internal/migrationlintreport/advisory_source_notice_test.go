package migrationlintreport_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migrationlintreport"
	"ptah.run/internal/vulnerability"
)

// TestWriteAdvisorySourceNotice pins that a run whose extension rules fell
// back from the advisory source writes the scan's line once, and that a run
// that read its source or matched nothing writes nothing.
func TestWriteAdvisorySourceNotice(t *testing.T) {
	tests := []struct {
		name   string
		origin vulnerability.Origin
		want   string
	}{
		{
			name:   "fell back",
			origin: vulnerability.Origin{Kind: "bundled", URL: "https://example.org/postgresql.json", Fallback: "fetch failed: refused"},
			want: "warning: advisory source https://example.org/postgresql.json was not used: fetch failed: refused; " +
				"postgres is matched against the snapshot built into this binary\n",
		},
		{name: "read the source", origin: vulnerability.Origin{Kind: "remote", URL: "https://example.org/postgresql.json"}, want: ""},
		{name: "matched nothing", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var stderr strings.Builder
			c.Assert(migrationlintreport.WriteAdvisorySourceNotice(&stderr, migrationlintreport.Report{AdvisoryOrigin: test.origin}), qt.IsNil)
			c.Assert(stderr.String(), qt.Equals, test.want)
		})
	}
}
