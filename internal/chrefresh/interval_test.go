package chrefresh_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/chrefresh"
)

// TestCanonicalInterval_ReproducesWhatTheServerStored is the table this package
// exists for.
//
// Every row is a (declared, stored) pair measured on live clickhouse-server
// 26.7.3.19: the left value was written after REFRESH EVERY in a CREATE
// MATERIALIZED VIEW, and the right one is what system.tables.create_table_query
// reported back. CanonicalInterval must produce the right one exactly, or a
// declaration using that spelling can never converge and the plan re-issues a
// drop and a create forever -- on an object whose drop takes its rows.
func TestCanonicalInterval_ReproducesWhatTheServerStored(t *testing.T) {
	tests := []struct {
		declared string
		stored   string
	}{
		// Already canonical: the server keeps what it would have written.
		{declared: "1 SECOND", stored: "1 SECOND"},
		{declared: "5 WEEK", stored: "5 WEEK"},
		{declared: "2 WEEK", stored: "2 WEEK"},
		{declared: "4 WEEK", stored: "4 WEEK"},
		{declared: "1 MONTH", stored: "1 MONTH"},
		{declared: "1 YEAR", stored: "1 YEAR"},
		{declared: "2 HOUR 30 MINUTE", stored: "2 HOUR 30 MINUTE"},

		// One unit re-expressed as the next one up.
		{declared: "60 SECOND", stored: "1 MINUTE"},
		{declared: "3600 SECOND", stored: "1 HOUR"},
		{declared: "86400 SECOND", stored: "1 DAY"},
		{declared: "604800 SECOND", stored: "1 WEEK"},
		{declared: "120 MINUTE", stored: "2 HOUR"},
		{declared: "24 HOUR", stored: "1 DAY"},
		{declared: "7 DAY", stored: "1 WEEK"},
		{declared: "14 DAY", stored: "2 WEEK"},

		// Decomposition into several terms, largest first.
		{declared: "90 SECOND", stored: "1 MINUTE 30 SECOND"},
		{declared: "100 SECOND", stored: "1 MINUTE 40 SECOND"},
		{declared: "3661 SECOND", stored: "1 HOUR 1 MINUTE 1 SECOND"},
		{declared: "10 DAY", stored: "1 WEEK 3 DAY"},
		{declared: "30 DAY", stored: "4 WEEK 2 DAY"},

		// A multi-term declaration is summed and then decomposed, so the terms
		// it was written with do not survive as written.
		{declared: "1 MINUTE 90 SECOND", stored: "2 MINUTE 30 SECOND"},
		{declared: "2 HOUR 90 MINUTE", stored: "3 HOUR 30 MINUTE"},
		{declared: "3 DAY 25 HOUR", stored: "4 DAY 1 HOUR"},
		{declared: "1 WEEK 8 DAY", stored: "2 WEEK 1 DAY"},

		// The boundaries on either side of each carry, where an off-by-one
		// decomposition is the defect that would survive every round row above.
		{declared: "59 SECOND", stored: "59 SECOND"},
		{declared: "61 SECOND", stored: "1 MINUTE 1 SECOND"},
		{declared: "119 SECOND", stored: "1 MINUTE 59 SECOND"},
		{declared: "121 SECOND", stored: "2 MINUTE 1 SECOND"},
		{declared: "59 MINUTE", stored: "59 MINUTE"},
		{declared: "61 MINUTE", stored: "1 HOUR 1 MINUTE"},
		{declared: "59 HOUR", stored: "2 DAY 11 HOUR"},
		{declared: "121 HOUR", stored: "5 DAY 1 HOUR"},
		{declared: "59 DAY", stored: "8 WEEK 3 DAY"},
		{declared: "119 DAY", stored: "17 WEEK"},
		{declared: "121 DAY", stored: "17 WEEK 2 DAY"},

		// Weeks are the top of the clock ladder, so a large count stays a large
		// count instead of becoming months.
		{declared: "59 WEEK", stored: "59 WEEK"},
		{declared: "121 WEEK", stored: "121 WEEK"},

		// The calendar ladder, which never meets the clock one.
		{declared: "1 QUARTER", stored: "3 MONTH"},
		{declared: "12 MONTH", stored: "1 YEAR"},
		{declared: "18 MONTH", stored: "1 YEAR 6 MONTH"},
		{declared: "23 MONTH", stored: "1 YEAR 11 MONTH"},
		{declared: "24 MONTH", stored: "2 YEAR"},
		{declared: "25 MONTH", stored: "2 YEAR 1 MONTH"},
		{declared: "1 YEAR 13 MONTH", stored: "2 YEAR 1 MONTH"},
	}

	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			c := qt.New(t)

			got, err := chrefresh.CanonicalInterval(test.declared)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.stored)
		})
	}
}

// TestCanonicalInterval_IsIdempotent is the property that makes the comparison
// work at all: canonicalizing what the server stored has to be a no-op, or the
// read side and the declared side would land in different places and a
// synchronized view would compare as drifted.
func TestCanonicalInterval_IsIdempotent(t *testing.T) {
	stored := []string{
		"1 SECOND", "1 MINUTE", "1 MINUTE 30 SECOND", "1 HOUR",
		"1 HOUR 1 MINUTE 1 SECOND", "1 DAY", "1 WEEK", "1 WEEK 3 DAY",
		"4 WEEK 2 DAY", "5 WEEK", "1 MONTH", "3 MONTH", "1 YEAR", "1 YEAR 6 MONTH",
	}

	for _, value := range stored {
		t.Run(value, func(t *testing.T) {
			c := qt.New(t)

			once, err := chrefresh.CanonicalInterval(value)
			c.Assert(err, qt.IsNil)
			twice, err := chrefresh.CanonicalInterval(once)

			c.Assert(err, qt.IsNil)
			c.Assert(once, qt.Equals, value)
			c.Assert(twice, qt.Equals, value)
		})
	}
}

// TestCanonicalInterval_AcceptsThePluralSpelling covers the one liberty this
// package takes over the measured table: a declaration may read `2 HOURS`,
// because that is how an operator writes it, and the server stores the singular
// either way.
func TestCanonicalInterval_AcceptsThePluralSpelling(t *testing.T) {
	tests := []struct {
		declared string
		stored   string
	}{
		{declared: "2 HOURS", stored: "2 HOUR"},
		{declared: "90 SECONDS", stored: "1 MINUTE 30 SECOND"},
		{declared: "3 MONTHS", stored: "3 MONTH"},
		{declared: "1 hour", stored: "1 HOUR"},
	}

	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			c := qt.New(t)

			got, err := chrefresh.CanonicalInterval(test.declared)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.stored)
		})
	}
}

// TestCanonicalInterval_RefusesWhatTheServerRefuses reproduces the server's own
// refusals, so a declaration is answered before a statement is sent rather than
// after one comes back.
//
// The two measured messages are quoted in the wording, because an operator who
// sees the same sentence from Ptah and from ClickHouse does not have to work
// out whether they are the same problem.
func TestCanonicalInterval_RefusesWhatTheServerRefuses(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     string
	}{
		{
			// Measured: Code: 36 ... Interval shouldn't contain both calendar
			// units and clock units.
			name:     "calendar and clock units mixed",
			declared: "1 MONTH 1 DAY",
			want:     `refresh interval "1 MONTH 1 DAY": interval shouldn't contain both calendar units and clock units`,
		},
		{
			name:     "year and hour mixed",
			declared: "1 YEAR 1 HOUR",
			want:     `refresh interval "1 YEAR 1 HOUR": interval shouldn't contain both calendar units and clock units`,
		},
		{
			// Measured: Code: 36 ... Interval must be positive.
			name:     "zero",
			declared: "0 SECOND",
			want:     `refresh interval "0 SECOND": interval must be positive`,
		},
		{
			name:     "negative",
			declared: "-1 SECOND",
			want:     `refresh interval "-1 SECOND": interval must be positive`,
		},
		{
			name:     "empty",
			declared: "   ",
			want:     `refresh interval is empty`,
		},
		{
			name:     "count without a unit",
			declared: "90",
			want:     `refresh interval "90" is not a sequence of <count> <unit> terms`,
		},
		{
			name:     "unit Ptah does not know",
			declared: "1 FORTNIGHT",
			want:     `refresh interval "1 FORTNIGHT": "FORTNIGHT" is not an interval unit`,
		},
		{
			name:     "count that is not a number",
			declared: "many HOUR",
			want:     `refresh interval "many HOUR": "many" is not a count`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := chrefresh.CanonicalInterval(test.declared)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.Equals, "")
		})
	}
}
