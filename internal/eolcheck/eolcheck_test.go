package eolcheck_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/capabilityprobe"
	"ptah.run/internal/eolcheck"
)

// A date in the past ends support; a date in the future does not; and a
// boolean says the same thing without a date, which is the shape
// endoflife.date uses for a product that publishes no calendar.
func TestCycleCeased(t *testing.T) {
	on := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		cycle eolcheck.Cycle
		want  bool
	}{
		{
			name:  "a date in the past",
			cycle: eolcheck.Cycle{EOL: on.AddDate(0, 0, -1), HasDate: true},
			want:  true,
		},
		{
			name:  "a date in the future",
			cycle: eolcheck.Cycle{EOL: on.AddDate(0, 0, 1), HasDate: true},
			want:  false,
		},
		{
			name:  "today is not past",
			cycle: eolcheck.Cycle{EOL: on, HasDate: true},
			want:  false,
		},
		{
			name:  "ended without a date",
			cycle: eolcheck.Cycle{Ended: true},
			want:  true,
		},
		{
			name:  "supported without a date",
			cycle: eolcheck.Cycle{Ended: false},
			want:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.cycle.Ceased(on), qt.Equals, test.want)
		})
	}
}

// The three spellings of `eol` mean three different things, and decoding them
// into one Go type would make "ended, no date" and "still supported"
// indistinguishable from a missing field.
func TestDecodeCycles_HappyPath(t *testing.T) {
	c := qt.New(t)
	body := `[{"cycle":"18","eol":"2030-11-14"},{"cycle":"13","eol":true},
	          {"cycle":"9.7","eol":false},{"cycle":16.0,"eol":"2033-01-11"}]`

	cycles, err := eolcheck.DecodeCycles(strings.NewReader(body))

	c.Assert(err, qt.IsNil)
	c.Assert(cycles, qt.HasLen, 4)
	c.Assert(cycles[0].HasDate, qt.IsTrue)
	c.Assert(cycles[0].EOL.Format("2006-01-02"), qt.Equals, "2030-11-14")
	c.Assert(cycles[1].HasDate, qt.IsFalse)
	c.Assert(cycles[1].Ended, qt.IsTrue)
	c.Assert(cycles[2].HasDate, qt.IsFalse)
	c.Assert(cycles[2].Ended, qt.IsFalse)
	// A numeric cycle is rendered rather than refused: endoflife.date quotes
	// some and not others, and `16.0` is the SQL Server line this repository
	// declares as "16.0".
	c.Assert(cycles[3].Cycle, qt.Equals, "16")
}

func TestDecodeCycles_FailurePath(t *testing.T) {
	t.Run("an unreadable date", func(t *testing.T) {
		c := qt.New(t)
		_, err := eolcheck.DecodeCycles(strings.NewReader(`[{"cycle":"18","eol":"soon"}]`))
		c.Assert(err, qt.ErrorMatches, `cycle "18" has an unreadable eol "soon".*`)
	})
	t.Run("not an array", func(t *testing.T) {
		c := qt.New(t)
		_, err := eolcheck.DecodeCycles(strings.NewReader(`{"cycle":"18"}`))
		c.Assert(err, qt.IsNotNil)
	})
}

func fixedFetcher(cycles map[string][]eolcheck.Cycle) eolcheck.Fetcher {
	return func(_ context.Context, product string) ([]eolcheck.Cycle, error) {
		got, listed := cycles[product]
		if !listed {
			return nil, errors.New("no fixture for " + product)
		}
		return got, nil
	}
}

func cell(dialect, line string) capabilityprobe.Cell {
	return capabilityprobe.Cell{Dialect: dialect, Line: line}
}

// A line past its date is reported; one still supported is not; and the run
// says how many lines a calendar actually answered, so an empty finding list
// cannot read as success on a run that asked nothing.
func TestCheck_HappyPath(t *testing.T) {
	c := qt.New(t)
	on := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	report, err := eolcheck.Check(context.Background(), []capabilityprobe.Cell{
		cell(platform.Postgres, "13"),
		cell(platform.Postgres, "18"),
	}, on, fixedFetcher(map[string][]eolcheck.Cycle{
		"postgresql": {
			{Cycle: "13", EOL: on.AddDate(-1, 0, 0), HasDate: true},
			{Cycle: "18", EOL: on.AddDate(4, 0, 0), HasDate: true},
		},
	}))

	c.Assert(err, qt.IsNil)
	c.Assert(report.Asked, qt.Equals, 2)
	c.Assert(report.Findings, qt.HasLen, 1)
	c.Assert(report.Findings[0].ID(), qt.Equals, "postgres-13")
	c.Assert(report.Findings[0].Ended(), qt.Equals, on.AddDate(-1, 0, 0).Format("2006-01-02"))
	c.Assert(report.Unanswered, qt.HasLen, 0)
}

// A dialect no calendar carries, and a line the calendar does not list, are
// both reported as unanswered rather than as supported. Reporting them
// supported would be the same invention as reporting them ended.
func TestCheck_UnansweredLines(t *testing.T) {
	c := qt.New(t)
	on := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)

	report, err := eolcheck.Check(context.Background(), []capabilityprobe.Cell{
		cell(platform.YugabyteDB, "2026.1"),
		cell(platform.Postgres, "99"),
	}, on, fixedFetcher(map[string][]eolcheck.Cycle{"postgresql": {{Cycle: "18"}}}))

	c.Assert(err, qt.IsNil)
	c.Assert(report.Findings, qt.HasLen, 0)
	c.Assert(report.Asked, qt.Equals, 0)
	c.Assert(report.Unanswered, qt.HasLen, 2)
	// Ordered by cell id, which is what makes a run's output comparable with
	// the one before it.
	c.Assert(report.Unanswered[0].Reason, qt.Contains, `lists no cycle "99"`)
	c.Assert(report.Unanswered[1].Reason, qt.Contains, "no yugabytedb product")
}

// A calendar that cannot be read fails the run. Treating a network error as
// "nothing is EOL" is the failure shape that reads as success.
func TestCheck_FailurePath(t *testing.T) {
	t.Run("the calendar cannot be read", func(t *testing.T) {
		c := qt.New(t)
		_, err := eolcheck.Check(context.Background(),
			[]capabilityprobe.Cell{cell(platform.Postgres, "13")},
			time.Now(),
			func(context.Context, string) ([]eolcheck.Cycle, error) {
				return nil, errors.New("connection refused")
			})
		c.Assert(err, qt.ErrorMatches, `read the postgresql calendar: connection refused`)
	})
}

// declaredDialects is the distinct normalized dialect of every declared cell,
// in the order the declarations name them.
func declaredDialects() []string {
	seen := make(map[string]bool, len(capabilityprobe.Cells))
	out := make([]string, 0, len(capabilityprobe.Cells))
	for _, declared := range capabilityprobe.Cells {
		dialect := platform.NormalizeDialect(declared.Dialect)
		if seen[dialect] {
			continue
		}
		seen[dialect] = true
		out = append(out, dialect)
	}
	return out
}

// Every dialect the matrix declares is either mapped to a calendar or named as
// unlisted, so a dialect added later shows up here as a gap rather than as a
// line nothing ever asks about.
func TestEveryDeclaredDialectIsClassified(t *testing.T) {
	dialects := declaredDialects()

	// The corpus floor. A declaration that stopped producing dialects would
	// let the loop below pass over an empty list and read as success.
	t.Run("the declarations name dialects to classify", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(len(dialects) >= 5, qt.IsTrue,
			qt.Commentf("only %d dialects are declared", len(dialects)))
	})

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			_, mapped := eolcheck.Product[dialect]
			reason, unlisted := eolcheck.Unlisted[dialect]
			c.Assert(mapped, qt.Not(qt.Equals), unlisted,
				qt.Commentf("dialect %q must be either mapped to an endoflife.date product or "+
					"named in Unlisted with a reason, and never both", dialect))
			// A reason is present exactly when the dialect is unlisted, which
			// says both halves at once: an unlisted dialect carries one, and a
			// mapped dialect is absent from Unlisted rather than present with
			// an empty string.
			c.Assert(reason != "", qt.Equals, unlisted, qt.Commentf(
				"dialect %q is unlisted with no reason, or mapped and named anyway", dialect))
		})
	}
}
