package crdbduration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/crdbduration"
)

// TestCanonical_ReproducesWhatTheServerStored is the table this package exists
// for.
//
// Every row is a (declared, stored) pair measured on live CockroachDB CCL
// v26.2.5: the left value was written into `ttl_row_stats_poll_interval` and
// the right one is what `SHOW CREATE TABLE` reported back. Canonical must
// produce the right one exactly, or a declaration using that spelling can never
// converge and the plan re-issues the change forever.
func TestCanonical_ReproducesWhatTheServerStored(t *testing.T) {
	tests := []struct {
		declared string
		stored   string
	}{
		// The Go duration spelling, which is what the server itself stores.
		{declared: "10m0s", stored: "10m0s"},
		{declared: "2h45m30s", stored: "2h45m30s"},
		{declared: "1s", stored: "1s"},
		{declared: "100000h", stored: "100000h0m0s"},

		// Seconds and minutes are re-expressed, which is the whole reason a
		// text comparison cannot work here.
		{declared: "600s", stored: "10m0s"},
		{declared: "90s", stored: "1m30s"},
		{declared: "10m", stored: "10m0s"},
		{declared: "1h30m", stored: "1h30m0s"},

		// Interval spellings the server accepts for a duration-valued
		// parameter: verbose, clock and ISO-8601.
		{declared: "5 minutes", stored: "5m0s"},
		{declared: "00:10:00", stored: "10m0s"},
		{declared: "PT10M", stored: "10m0s"},
		{declared: "10 m", stored: "10m0s"},
		{declared: "10minutes", stored: "10m0s"},

		// Calendar units, folded on the server's terms. A month is thirty days
		// and a year is 365.25 days, so twelve months is a YEAR rather than
		// twelve month-lengths -- the row that separates this conversion from
		// the plausible one.
		{declared: "1 day", stored: "24h0m0s"},
		{declared: "1 week", stored: "168h0m0s"},
		{declared: "1 month", stored: "720h0m0s"},
		{declared: "11 months", stored: "7920h0m0s"},
		{declared: "12 months", stored: "8766h0m0s"},
		{declared: "1 year", stored: "8766h0m0s"},
		{declared: "2 years", stored: "17532h0m0s"},
		{declared: "23 months", stored: "16686h0m0s"},
		{declared: "25 months", stored: "18252h0m0s"},
		{declared: "1 year 1 month", stored: "9486h0m0s"},
		{declared: "1 mon 2 days", stored: "768h0m0s"},
		{declared: "365 days", stored: "8760h0m0s"},

		// Sub-second precision is truncated toward zero, not rounded. 2500ms
		// is the row that says so: rounding would store 3s.
		{declared: "1999ms", stored: "1s"},
		{declared: "2500ms", stored: "2s"},

		// The largest value the server keeps, which is exactly the largest a
		// Go duration holds.
		{declared: "2562047h", stored: "2562047h0m0s"},
		{declared: "292 years", stored: "2559672h0m0s"},
	}

	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			c := qt.New(t)
			got, err := crdbduration.Canonical(test.declared)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.stored)
		})
	}
}

// TestCanonical_RefusesWhatTheServerWouldNotKeep covers the values that reach
// the server and come back as something other than themselves -- or as nothing.
//
// Each row was measured: the server either drops the parameter entirely or
// answers `ERROR: "ttl_row_stats_poll_interval" must be at least 0`. That
// message is accurate for a negative value and misleading for an overflow, so
// both are refused here, before any SQL, with the reason named.
func TestCanonical_RefusesWhatTheServerWouldNotKeep(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		wantErr  string
	}{
		{
			name:     "below one second, which the server stores nowhere at all",
			declared: "500ms",
			wantErr:  `(?s).*is below one second.*stores no parameter at all.*`,
		},
		{
			name:     "zero, which is the same case spelled plainly",
			declared: "0s",
			wantErr:  `(?s).*is below one second.*`,
		},
		{
			name:     "negative, which the server refuses outright",
			declared: "-5s",
			wantErr:  `(?s).*is negative.*at least 0.*`,
		},
		{
			name:     "one hour past the largest duration the server keeps",
			declared: "2562048h",
			wantErr:  `(?s).*is longer than.*server wraps.*`,
		},
		{
			name:     "a calendar spelling past the same limit",
			declared: "293 years",
			wantErr:  `(?s).*is longer than.*server wraps.*`,
		},
		{
			name:     "empty, which is a missing value rather than a zero one",
			declared: "",
			wantErr:  `(?s).*empty duration.*`,
		},
		{
			name:     "a spelling no accepted syntax reads",
			declared: "every ten minutes",
			wantErr:  `(?s).*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := crdbduration.Canonical(test.declared)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestCanonical_IsIdempotent asserts that feeding the stored form back in
// produces itself.
//
// This is what makes the comparison stable across runs: the catalog hands back
// the canonical spelling, and a plan that normalized it again must not move it.
func TestCanonical_IsIdempotent(t *testing.T) {
	stored := []string{"1s", "10m0s", "1m30s", "2h45m30s", "720h0m0s", "8766h0m0s", "2562047h0m0s"}

	for _, value := range stored {
		t.Run(value, func(t *testing.T) {
			c := qt.New(t)
			got, err := crdbduration.Canonical(value)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, value)
		})
	}
}
