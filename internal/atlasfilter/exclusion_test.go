package atlasfilter_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/readscope"
)

// unselectedRuntime serves no scope selections: it is what a runtime that does
// not implement selectext.Runtime looks like to NewExclusion.
type unselectedRuntime struct{}

// exclusionDesired declares a table to keep and a table the selectors exclude.
func exclusionDesired() *schemamodel.Database {
	return &schemamodel.Database{Tables: []schemamodel.Table{
		{Name: "widgets", StructName: "Widgets"},
		{Name: "reporting_cache", StructName: "ReportingCache"},
	}}
}

// exclusionObserved reads a table to keep and a table the selectors exclude.
func exclusionObserved() *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{Name: "widgets"}, {Name: "external_audit"}}}
}

func desiredTableNames(db *schemamodel.Database) []string {
	names := make([]string, 0, len(db.Tables))
	for _, table := range db.Tables {
		names = append(names, table.Name)
	}
	return names
}

func observedTableNames(db *catalog.Database) []string {
	names := make([]string, 0, len(db.Tables))
	for _, table := range db.Tables {
		names = append(names, table.Name)
	}
	return names
}

func TestNewExclusion_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		runtime  any
		patterns []string
		excludes bool
	}{
		{name: "no patterns needs no runtime"},
		{name: "blanks and commas need no runtime", patterns: []string{"", " , ", ","}},
		{name: "a selector", runtime: builtintest.Runtime(), patterns: []string{"manual"}, excludes: true},
		{name: "a comma list", runtime: builtintest.Runtime(), patterns: []string{"manual, *[type=view]"}, excludes: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			exclusion, err := atlasfilter.NewExclusion(test.runtime, test.patterns)

			c.Assert(err, qt.IsNil)
			c.Assert(exclusion.Excludes(), qt.Equals, test.excludes)
		})
	}
}

func TestNewExclusion_FailurePath(t *testing.T) {
	t.Run("a runtime that serves no scope selections", func(t *testing.T) {
		c := qt.New(t)

		exclusion, err := atlasfilter.NewExclusion(unselectedRuntime{}, []string{"manual"})

		c.Assert(err, qt.ErrorIs, selectext.ErrUnselected)
		c.Assert(err, qt.ErrorMatches, `no scope selection owners were selected: exclude selectors need a runtime that serves scope selections`)
		c.Assert(exclusion.Excludes(), qt.IsFalse)
	})
	t.Run("a malformed glob", func(t *testing.T) {
		c := qt.New(t)

		exclusion, err := atlasfilter.NewExclusion(builtintest.Runtime(), []string{"us[ers"})

		c.Assert(err, qt.ErrorMatches, `invalid Atlas exclude glob "us\[ers": syntax error in pattern`)
		c.Assert(exclusion.Excludes(), qt.IsFalse)
	})
	t.Run("a pattern no scope can address", func(t *testing.T) {
		c := qt.New(t)

		exclusion, err := atlasfilter.NewExclusion(builtintest.Runtime(), []string{"public.users.name.x"})

		c.Assert(err, qt.ErrorMatches, `too many parts in pattern "public.users.name.x": .*`)
		c.Assert(exclusion.Excludes(), qt.IsFalse)
	})
}

// TestExclusion_FiltersBothSides holds the desired schema and the database
// read to one selector list, so a selector protects an object on either side:
// the declared table is not created and the unmanaged one is not dropped.
func TestExclusion_FiltersBothSides(t *testing.T) {
	c := qt.New(t)
	exclusion := must.Must(atlasfilter.NewExclusion(builtintest.Runtime(), []string{"reporting_cache", "external_audit"}))
	desired := exclusionDesired()
	observed := exclusionObserved()

	filteredDesired, desiredReport, err := exclusion.Desired(desired, "main")
	c.Assert(err, qt.IsNil)
	filteredObserved, observedReport, err := exclusion.Observed(observed, "main")
	c.Assert(err, qt.IsNil)

	c.Assert(desiredTableNames(filteredDesired), qt.DeepEquals, []string{"widgets"})
	c.Assert(observedTableNames(filteredObserved), qt.DeepEquals, []string{"widgets"})
	c.Assert(desiredReport.Unmatched, qt.DeepEquals, []string{"external_audit"})
	c.Assert(observedReport.Unmatched, qt.DeepEquals, []string{"reporting_cache"})
	c.Assert(desiredTableNames(desired), qt.DeepEquals, []string{"widgets", "reporting_cache"})
	c.Assert(observedTableNames(observed), qt.DeepEquals, []string{"widgets", "external_audit"})
}

// TestExclusion_ZeroValueReturnsItsInput is the control for the test above:
// without a selector both states come back as they went in, the same values,
// and no runtime was needed to say so.
func TestExclusion_ZeroValueReturnsItsInput(t *testing.T) {
	c := qt.New(t)
	var exclusion atlasfilter.Exclusion
	desired := exclusionDesired()
	observed := exclusionObserved()

	filteredDesired, desiredReport, desiredErr := exclusion.Desired(desired, "main")
	filteredObserved, observedReport, observedErr := exclusion.Observed(observed, "main")

	c.Assert(desiredErr, qt.IsNil)
	c.Assert(observedErr, qt.IsNil)
	c.Assert(filteredDesired, qt.Equals, desired)
	c.Assert(filteredObserved, qt.Equals, observed)
	c.Assert(desiredReport, qt.DeepEquals, atlasfilter.ExcludeReport{})
	c.Assert(observedReport, qt.DeepEquals, atlasfilter.ExcludeReport{})
}

// TestExclusion_ReadContext marks a read with the kinds a selector leaves out
// whole, which is what lets a MySQL account that cannot see triggers read the
// rest of the database.
func TestExclusion_ReadContext(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		kind     readscope.Kind
		needed   bool
	}{
		{name: "no selector needs every kind", kind: readscope.Triggers, needed: true},
		{name: "every trigger", patterns: []string{"*[type=trigger]"}, kind: readscope.Triggers, needed: false},
		{name: "some triggers", patterns: []string{"audit_*[type=trigger]"}, kind: readscope.Triggers, needed: true},
		{name: "every trigger leaves views needed", patterns: []string{"*[type=trigger]"}, kind: readscope.Views, needed: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			exclusion := must.Must(atlasfilter.NewExclusion(builtintest.Runtime(), test.patterns))

			ctx := exclusion.ReadContext(t.Context())

			c.Assert(readscope.Needs(ctx, test.kind), qt.Equals, test.needed)
		})
	}
}

// TestReportUnmatched warns only about a selector that matched on neither
// side. A selector matching on one side is how an excluded CREATE or DROP
// looks, so it is not reported.
func TestReportUnmatched(t *testing.T) {
	tests := []struct {
		name    string
		reports []atlasfilter.ExcludeReport
		want    string
	}{
		{name: "no report"},
		{
			name: "each selector matched on one side",
			reports: []atlasfilter.ExcludeReport{
				{Unmatched: []string{"external_audit"}},
				{Unmatched: []string{"reporting_cache"}},
			},
		},
		{
			name: "a selector matched on neither side",
			reports: []atlasfilter.ExcludeReport{
				{Unmatched: []string{"nothing_here", "external_audit"}},
				{Unmatched: []string{"nothing_here"}},
			},
			want: "Warning: the --exclude selection matched no objects: \"nothing_here\".\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			atlasfilter.ReportUnmatched(&out, test.reports...)

			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}
