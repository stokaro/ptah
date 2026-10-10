package chsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
)

// declaredSchedule is the refresh clause a parsed view carries as the
// ClickHouse owner's setting, or "" when it declares none.
func declaredSchedule(c *qt.C, view schemamodel.MaterializedView) string {
	c.Helper()
	schedule, found, err := schemaext.FacetAs[*chschema.DesiredRefresh](view.Facets, chschema.RefreshKind)
	c.Assert(err, qt.IsNil)
	if !found {
		return ""
	}
	c.Assert(view.Facets.TargetScope(chschema.RefreshKind), qt.DeepEquals, []string{"clickhouse"})
	return schedule.Clause()
}

// parseMatViewRefreshSource parses one file declaring a materialized view with
// the given refresh attribute, with the ClickHouse owner selected.
func parseMatViewRefreshSource(c *qt.C, attribute string) (schemamodel.Database, error) {
	c.Helper()
	owner, err := annotation.NewSet(chsource.Annotations())
	c.Assert(err, qt.IsNil)
	source := "package models\n\n" +
		"//ptah:schema:matview name=\"mv\" body=\"SELECT 1\"" + attribute + "\n" +
		"type MV struct{}\n"
	return goschema.ParseSource(owner, "models.go", source)
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
			c.Assert(declaredSchedule(c, database.MaterializedViews[0]), qt.Equals, test.want)
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
	c.Assert(database.MaterializedViews[0].Facets.IsZero(), qt.IsTrue)
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
			want:     `.*refresh clause starts with .*; expected EVERY or AFTER.*`,
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

// TestParseMatView_WithoutTheOwnerTheScheduleIsAnUnknownAttribute is the
// control on the selection: a parse that does not select the ClickHouse owner
// refuses the attribute by name rather than dropping the schedule.
func TestParseMatView_WithoutTheOwnerTheScheduleIsAnUnknownAttribute(t *testing.T) {
	c := qt.New(t)

	_, err := goschema.ParseSource(annotation.None(), "models.go",
		"package models\n\n//ptah:schema:matview name=\"mv\" body=\"SELECT 1\" refresh=\"every 1 hour\"\ntype MV struct{}\n")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnknownAttribute)
	c.Assert(err, qt.ErrorMatches, `(?s).*refresh.*`)
}
