package spannerttl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/spannerttl"
)

// TestParse_ReadsWhatTheCatalogPrints pins the shapes a live server produced.
//
// Every row here is a value read out of
// information_schema.tables.row_deletion_policy_expression on the Cloud Spanner
// emulator behind PGAdapter 0.55.2, or the form a declaration takes on the way
// in (stokaro/ptah#2236).
func TestParse_ReadsWhatTheCatalogPrints(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		column     string
		interval   string
	}{
		{
			// What `TTL INTERVAL '30 days' ON created_at` reads back as. The
			// server rewrote the interval; the column it did not.
			name:       "the rewritten interval a server printed",
			expression: "INTERVAL '4 WEEKS 2 DAYS' ON created_at",
			column:     "created_at",
			interval:   "4 WEEKS 2 DAYS",
		},
		{
			name:       "a zero interval, which the server accepts",
			expression: "INTERVAL '0 DAYS' ON ts",
			column:     "ts",
			interval:   "0 DAYS",
		},
		{
			name:       "the spelling a declaration is written in",
			expression: "INTERVAL '30 days' ON created_at",
			column:     "created_at",
			interval:   "30 days",
		},
		{
			name:       "a quoted column keeps its name, not its quoting",
			expression: `INTERVAL '1 DAYS' ON "Created At"`,
			column:     "Created At",
			interval:   "1 DAYS",
		},
		{
			name:       "keywords are matched whatever case they arrive in",
			expression: "interval '7 days' on ts",
			column:     "ts",
			interval:   "7 days",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, found, err := spannerttl.ParseExpression(test.expression)

			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(parsed, qt.Equals, spannerttl.Expression{Column: test.column, Interval: test.interval})
		})
	}
}

// TestParse_NoPolicyIsNotAnError is the row every table without a policy takes.
func TestParse_NoPolicyIsNotAnError(t *testing.T) {
	tests := []struct {
		name       string
		expression string
	}{
		{name: "the empty string every ordinary table reports", expression: ""},
		{name: "whitespace only", expression: "   "},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, found, err := spannerttl.ParseExpression(test.expression)

			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsFalse)
			c.Assert(parsed, qt.Equals, spannerttl.Expression{})
		})
	}
}

// TestParse_RefusesWhatItCannotRead keeps a half-read policy from becoming a
// table Ptah believes has none.
//
// The direction matters: a policy silently dropped is an unbounded table, found
// on the storage bill. Refusing names the expression instead.
func TestParse_RefusesWhatItCannotRead(t *testing.T) {
	tests := []struct {
		name       string
		expression string
	}{
		{name: "no INTERVAL keyword", expression: "'30 days' ON created_at"},
		{name: "the interval is not quoted", expression: "INTERVAL 30 days ON created_at"},
		{name: "the quote is not closed", expression: "INTERVAL '30 days ON created_at"},
		{name: "no ON clause", expression: "INTERVAL '30 days'"},
		{name: "ON names nothing", expression: "INTERVAL '30 days' ON "},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, found, err := spannerttl.ParseExpression(test.expression)

			c.Assert(err, qt.ErrorMatches, `row deletion policy .* cannot be read: .*`)
			c.Assert(found, qt.IsFalse)
			c.Assert(parsed, qt.Equals, spannerttl.Expression{})
		})
	}
}

// TestEqualIntervals_ComparesTheIntervalAsAValue is the property the whole
// package exists for: the server rewrites the interval, so text comparison
// plans a change forever. The column is the owner's comparison; see
// spannerschema.Equivalent.
func TestEqualIntervals_ComparesTheIntervalAsAValue(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		stored   string
		want     bool
	}{
		{name: "the rewriting a live server did", declared: "30 days", stored: "4 WEEKS 2 DAYS", want: true},
		{name: "a week is seven days, which is the server's own rule", declared: "7 days", stored: "1 WEEKS", want: true},
		{
			// The row that decided the arithmetic. Under PostgreSQL's interval
			// rules months and days do not convert, so a comparison built on
			// them calls these different and plans the same ALTER forever --
			// measured, immediately after applying it successfully.
			name: "a month is thirty days, which PostgreSQL would deny", declared: "60 days", stored: "2 MONTHS", want: true,
		},
		{name: "a day is twenty-four hours", declared: "1 days", stored: "24 HOURS", want: true},
		{name: "the mixed form a year is stored as", declared: "365 days", stored: "12 MONTHS 5 DAYS", want: true},
		{name: "four weeks and a day", declared: "29 days", stored: "4 WEEKS 24 HOURS", want: true},
		{
			// The control for the arithmetic: one day apart must stay a
			// difference, or the reduction has folded everything together.
			name: "one day apart is still a change", declared: "30 days", stored: "1 MONTHS 24 HOURS", want: false,
		},
		{
			// The control for the interval: without it, a comparison that
			// folded every interval to equal would pass every row above.
			name: "a genuinely different interval is a change", declared: "30 days", stored: "60 days", want: false,
		},
		{
			// An interval neither side can read falls back to text, which
			// converges. Reporting a difference here would plan a change on
			// every run and never reach agreement.
			name: "a spelling this package cannot read, identical on both sides", declared: "every other tuesday", stored: "every other tuesday", want: true,
		},
		{name: "two unreadable spellings that differ are still a change", declared: "every other tuesday", stored: "some fridays", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(spannerttl.EqualIntervals(test.declared, test.stored), qt.Equals, test.want)
		})
	}
}

// TestClause_EmitsWhatTheAuthorWrote pins that rendering is verbatim: the
// author's spelling survives, not the server's.
func TestClause_EmitsWhatTheAuthorWrote(t *testing.T) {
	c := qt.New(t)

	rendered := spannerttl.Clause("created_at", "30 days", func(name string) string { return `"` + name + `"` })

	c.Assert(rendered, qt.Equals, `TTL INTERVAL '30 days' ON "created_at"`)
}

// TestValidateInterval_HappyPath accepts every spelling the server stored and
// a declaration may use, a zero interval among them, which the server accepts.
func TestValidateInterval_HappyPath(t *testing.T) {
	for _, interval := range []string{"30 days", "4 WEEKS 2 DAYS", "2 MONTHS", "24 HOURS", "0 DAYS", "12 MONTHS 5 DAYS"} {
		t.Run(interval, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(spannerttl.ValidateInterval(interval), qt.IsNil)
		})
	}
}

// TestValidateInterval_FailurePath refuses what the server refuses and what
// this package cannot read, since an unread spelling compared as text plans a
// change on every run.
func TestValidateInterval_FailurePath(t *testing.T) {
	tests := []struct {
		interval string
		wantErr  string
	}{
		{interval: "1 hour", wantErr: `interval "1 hour" is not a whole number of days, which Spanner refuses`},
		{interval: "36 hours", wantErr: `interval "36 hours" is not a whole number of days, which Spanner refuses`},
		{interval: "-1 days", wantErr: `interval "-1 days" is negative`},
		{interval: "30d", wantErr: `interval "30d" is not a number of months, weeks, days or hours, such as 30 days`},
		{interval: "P30D", wantErr: `interval "P30D" is not a number of months, weeks, days or hours, such as 30 days`},
		{interval: "", wantErr: `interval "" is not a number of months, weeks, days or hours, such as 30 days`},
	}
	for _, test := range tests {
		t.Run(test.interval, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(spannerttl.ValidateInterval(test.interval), qt.ErrorMatches, test.wantErr)
		})
	}
}
