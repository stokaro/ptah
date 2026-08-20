package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/chrefresh"
)

// parseMatViewRefreshSource parses one file declaring a materialized view with
// the given refresh attribute.
func parseMatViewRefreshSource(c *qt.C, attribute string) (*goschema.Database, error) {
	c.Helper()
	dir := c.TB.TempDir()
	source := "package models\n\n" +
		"//ptah:schema:matview name=\"mv\" body=\"SELECT 1\"" + attribute + "\n" +
		"type MV struct{}\n"
	writeGoFile(c, dir, "models.go", source)
	return goschema.ParseDir(dir)
}

// TestParseMatView_CanonicalizesTheDeclaredSchedule is what keeps a declaration
// comparable with the catalog.
//
// ClickHouse stores `EVERY 60 MINUTE` as `EVERY 1 HOUR`. Canonicalizing at the
// parser means every later layer -- renderer, comparator, planner -- sees the
// spelling the server would have stored, so an operator's choice of spelling
// cannot make a synchronized view compare as drifted (stokaro/ptah#1802).
func TestParseMatView_CanonicalizesTheDeclaredSchedule(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     string
	}{
		{name: "already canonical", declared: "every 1 hour", want: "EVERY 1 HOUR"},
		{name: "minutes", declared: "every 60 minute", want: "EVERY 1 HOUR"},
		{name: "seconds", declared: "every 3600 second", want: "EVERY 1 HOUR"},
		{name: "decomposed", declared: "every 90 second", want: "EVERY 1 MINUTE 30 SECOND"},
		{name: "after", declared: "after 30 minute", want: "AFTER 30 MINUTE"},
		{
			name:     "every clause at once",
			declared: "every 1 day offset 120 minute randomize for 1800 second append",
			want:     "EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 30 MINUTE APPEND",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := parseMatViewRefreshSource(c, ` refresh="`+test.declared+`"`)

			c.Assert(err, qt.IsNil)
			c.Assert(database.MaterializedViews, qt.HasLen, 1)
			c.Assert(chrefresh.Clause(database.MaterializedViews[0].Refresh), qt.Equals, test.want)
		})
	}
}

// TestParseMatView_NoRefreshAttributeLeavesNoSchedule is the ordinary view: on
// ClickHouse it is maintained by inserts into its source, and on every other
// dialect a schedule is not a thing at all.
func TestParseMatView_NoRefreshAttributeLeavesNoSchedule(t *testing.T) {
	c := qt.New(t)

	database, err := parseMatViewRefreshSource(c, "")

	c.Assert(err, qt.IsNil)
	c.Assert(database.MaterializedViews, qt.HasLen, 1)
	c.Assert(database.MaterializedViews[0].Refresh, qt.IsNil)
}

// TestParseMatView_RefusesAScheduleTheServerWouldRefuse answers a bad
// declaration where it is written rather than where it is executed.
func TestParseMatView_RefusesAScheduleTheServerWouldRefuse(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     string
	}{
		{
			name:     "not a clause",
			declared: "hourly",
			want:     `.*is not a ClickHouse refresh clause.*`,
		},
		{
			// Measured: the server answers `Interval shouldn't contain both
			// calendar units and clock units`.
			name:     "calendar and clock units mixed",
			declared: "every 1 month 1 day",
			want:     `.*calendar units and clock units.*`,
		},
		{
			name:     "zero interval",
			declared: "every 0 second",
			want:     `.*interval must be positive.*`,
		},
		{
			// Measured: `AFTER ... OFFSET ...` is a syntax error.
			name:     "offset on after",
			declared: "after 1 hour offset 5 minute",
			want:     `.*OFFSET belongs to EVERY.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parseMatViewRefreshSource(c, ` refresh="`+test.declared+`"`)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
		})
	}
}
