package spannerttl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/spannerttl"
)

// spannerColumnKey is the rule the comparator passes in production: the
// target's own column-name semantics, which for Spanner are exact.
var spannerColumnKey = identifier.ForDialect(platform.Spanner).ColumnIdentityKey

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

			spec, err := spannerttl.Parse(test.expression)

			c.Assert(err, qt.IsNil)
			c.Assert(spec, qt.IsNotNil)
			c.Assert(spec.Column, qt.Equals, test.column)
			c.Assert(spec.Interval, qt.Equals, test.interval)
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

			spec, err := spannerttl.Parse(test.expression)

			c.Assert(err, qt.IsNil)
			c.Assert(spec.IsZero(), qt.IsTrue)
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

			_, err := spannerttl.Parse(test.expression)

			c.Assert(err, qt.ErrorMatches, `row deletion policy .* cannot be read: .*`)
		})
	}
}

// TestEqual_ComparesTheIntervalAsAValue is the property the whole package
// exists for: the server rewrites the interval, so text comparison plans a
// change forever.
func TestEqual_ComparesTheIntervalAsAValue(t *testing.T) {
	tests := []struct {
		name     string
		declared *ast.RowDeletionPolicySpec
		stored   *ast.RowDeletionPolicySpec
		want     bool
	}{
		{
			name:     "the rewriting a live server did",
			declared: &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "30 days"},
			stored:   &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "4 WEEKS 2 DAYS"},
			want:     true,
		},
		{
			name:     "a week is seven days, which is the server's own rule",
			declared: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "7 days"},
			stored:   &ast.RowDeletionPolicySpec{Column: "ts", Interval: "1 WEEKS"},
			want:     true,
		},
		{
			// The row that decided the arithmetic. Under PostgreSQL's interval
			// rules months and days do not convert, so a comparison built on
			// them calls these different and plans the same ALTER forever --
			// measured, immediately after applying it successfully.
			name:     "a month is thirty days, which PostgreSQL would deny",
			declared: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "60 days"},
			stored:   &ast.RowDeletionPolicySpec{Column: "ts", Interval: "2 MONTHS"},
			want:     true,
		},
		{
			name:     "a day is twenty-four hours",
			declared: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "1 days"},
			stored:   &ast.RowDeletionPolicySpec{Column: "ts", Interval: "24 HOURS"},
			want:     true,
		},
		{
			name:     "the mixed form a year is stored as",
			declared: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "365 days"},
			stored:   &ast.RowDeletionPolicySpec{Column: "ts", Interval: "12 MONTHS 5 DAYS"},
			want:     true,
		},
		{
			name:     "four weeks and a day",
			declared: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "29 days"},
			stored:   &ast.RowDeletionPolicySpec{Column: "ts", Interval: "4 WEEKS 24 HOURS"},
			want:     true,
		},
		{
			// The control for the arithmetic: one day apart must stay a
			// difference, or the reduction has folded everything together.
			name:     "one day apart is still a change",
			declared: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "30 days"},
			stored:   &ast.RowDeletionPolicySpec{Column: "ts", Interval: "1 MONTHS 24 HOURS"},
			want:     false,
		},
		{
			// The control for the column: without it, a comparison that only
			// read the interval would call these equal and leave rows expiring
			// off the wrong timestamp.
			name:     "the same interval on a different column is a different policy",
			declared: &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "30 days"},
			stored:   &ast.RowDeletionPolicySpec{Column: "updated_at", Interval: "30 days"},
			want:     false,
		},
		{
			// Case is part of the name on this target: identifier.ForDialect
			// gives Spanner ComparisonExact for columns, so these are two
			// columns and moving the policy between them is a real change.
			// Folding them together would leave the deletion tied to the wrong
			// timestamp with nothing planned.
			name:     "two columns differing only in case are two columns",
			declared: &ast.RowDeletionPolicySpec{Column: "CreatedAt", Interval: "30 days"},
			stored:   &ast.RowDeletionPolicySpec{Column: "createdat", Interval: "30 days"},
			want:     false,
		},
		{
			// And the same spelling is the same column, which is what keeps the
			// row above from being satisfied by a comparison that answers false
			// for everything.
			name:     "the same name is the same column",
			declared: &ast.RowDeletionPolicySpec{Column: "CreatedAt", Interval: "30 days"},
			stored:   &ast.RowDeletionPolicySpec{Column: "CreatedAt", Interval: "4 WEEKS 2 DAYS"},
			want:     true,
		},
		{
			// The control for the interval: without it, a comparison that
			// folded every interval to equal would pass every row above.
			name:     "a genuinely different interval is a change",
			declared: &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "30 days"},
			stored:   &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "60 days"},
			want:     false,
		},
		{
			name:     "no policy on either side",
			declared: nil,
			stored:   nil,
			want:     true,
		},
		{
			name:     "a policy added",
			declared: &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "30 days"},
			stored:   nil,
			want:     false,
		},
		{
			name:     "a policy removed",
			declared: nil,
			stored:   &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "30 days"},
			want:     false,
		},
		{
			// An interval neither side can read falls back to text, which
			// converges. Reporting a difference here would plan a change on
			// every run and never reach agreement.
			name:     "a spelling this package cannot read, identical on both sides",
			declared: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "every other tuesday"},
			stored:   &ast.RowDeletionPolicySpec{Column: "ts", Interval: "every other tuesday"},
			want:     true,
		},
		{
			name:     "two unreadable spellings that differ are still a change",
			declared: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "every other tuesday"},
			stored:   &ast.RowDeletionPolicySpec{Column: "ts", Interval: "some fridays"},
			want:     false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(spannerttl.Equal(test.declared, test.stored, spannerColumnKey), qt.Equals, test.want)
		})
	}
}

// TestRender_EmitsWhatTheAuthorWrote pins that rendering is verbatim.
func TestRender_EmitsWhatTheAuthorWrote(t *testing.T) {
	tests := []struct {
		name string
		spec *ast.RowDeletionPolicySpec
		want string
	}{
		{
			name: "the author's spelling survives, not the server's",
			spec: &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "30 days"},
			want: ` TTL INTERVAL '30 days' ON "created_at"`,
		},
		{
			name: "a table with no policy carries no clause",
			spec: nil,
			want: "",
		},
		{
			name: "a spec missing its column is not a policy",
			spec: &ast.RowDeletionPolicySpec{Interval: "30 days"},
			want: "",
		},
		{
			name: "a spec missing its interval is not a policy",
			spec: &ast.RowDeletionPolicySpec{Column: "created_at"},
			want: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rendered := spannerttl.Render(test.spec, func(name string) string { return `"` + name + `"` })

			c.Assert(rendered, qt.Equals, test.want)
		})
	}
}
