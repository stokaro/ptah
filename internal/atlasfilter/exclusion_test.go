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

	filteredDesired, desiredReport, err := exclusion.Desired(desired, atlasfilter.Naming{DefaultSchema: "main"})
	c.Assert(err, qt.IsNil)
	filteredObserved, observedReport, err := exclusion.Observed(observed, atlasfilter.Naming{DefaultSchema: "main"})
	c.Assert(err, qt.IsNil)

	c.Assert(desiredTableNames(filteredDesired), qt.DeepEquals, []string{"widgets"})
	c.Assert(observedTableNames(filteredObserved), qt.DeepEquals, []string{"widgets"})
	c.Assert(desiredReport.Unmatched, qt.DeepEquals, []string{"external_audit"})
	c.Assert(observedReport.Unmatched, qt.DeepEquals, []string{"reporting_cache"})
	c.Assert(desiredTableNames(desired), qt.DeepEquals, []string{"widgets", "reporting_cache"})
	c.Assert(observedTableNames(observed), qt.DeepEquals, []string{"widgets", "external_audit"})
}

// notesObserved reads a table with two columns in the connection's schema, which
// the read leaves unqualified.
func notesObserved() *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{
		Name:    "notes",
		Columns: []catalog.Column{{Name: "id"}, {Name: "search_vector"}},
	}}}
}

// notesDesired declares the same table and columns as notesObserved.
func notesDesired() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "notes", StructName: "Notes"}},
		Fields: []schemamodel.Field{
			{StructName: "Notes", FieldName: "ID", Name: "id"},
			{StructName: "Notes", FieldName: "SearchVector", Name: "search_vector"},
		},
	}
}

func desiredColumnNames(db *schemamodel.Database) []string {
	var names []string
	for _, field := range db.Fields {
		names = append(names, field.Name)
	}
	return names
}

func observedColumnNames(db *catalog.Database) []string {
	var names []string
	for _, column := range db.Tables[0].Columns {
		names = append(names, column.Name)
	}
	return names
}

// TestExclusion_CountsARealmRelativePattern_HappyPath reads a schema-qualified
// column pattern against the realm, where its three parts name the schema, the
// table and the column, on both sides of a comparison. The bound row is the control: the two-part spelling
// reaches the same column when the connection fills the schema slot.
func TestExclusion_CountsARealmRelativePattern_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		naming  atlasfilter.Naming
	}{
		{name: "realm", pattern: "public.notes.search_vector", naming: atlasfilter.Naming{DefaultSchema: "public", RealmRelative: true}},
		{name: "bound", pattern: "notes.search_vector", naming: atlasfilter.Naming{DefaultSchema: "public"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			exclusion := must.Must(atlasfilter.NewExclusion(builtintest.Runtime(), []string{test.pattern}))

			observed, observedReport, observedErr := exclusion.Observed(notesObserved(), test.naming)
			desired, desiredReport, desiredErr := exclusion.Desired(notesDesired(), test.naming)

			c.Assert(observedErr, qt.IsNil)
			c.Assert(desiredErr, qt.IsNil)
			c.Assert(observedColumnNames(observed), qt.DeepEquals, []string{"id"})
			c.Assert(desiredColumnNames(desired), qt.DeepEquals, []string{"id"})
			c.Assert(observedReport.Unmatched, qt.HasLen, 0)
			c.Assert(desiredReport.Unmatched, qt.HasLen, 0)
		})
	}
}

// TestExclusion_CountsARealmRelativePattern_FailurePath refuses the realm
// spelling where the connection fills the schema slot, on both sides, so a
// comparison cannot filter one side and refuse the other.
func TestExclusion_CountsARealmRelativePattern_FailurePath(t *testing.T) {
	c := qt.New(t)
	exclusion := must.Must(atlasfilter.NewExclusion(builtintest.Runtime(), []string{"public.notes.search_vector"}))
	bound := atlasfilter.Naming{DefaultSchema: "public"}

	observed, _, observedErr := exclusion.Observed(notesObserved(), bound)
	desired, _, desiredErr := exclusion.Desired(exclusionDesired(), bound)

	const refusal = `too many parts in pattern "public.notes.search_vector": this connection is bound to schema "public", ` +
		`so a pattern names object or object.child; write "notes.search_vector"`
	c.Assert(observedErr, qt.ErrorMatches, refusal)
	c.Assert(desiredErr, qt.ErrorMatches, refusal)
	c.Assert(observed, qt.IsNil)
	c.Assert(desired, qt.IsNil)
}

// TestConnectionNaming answers the realm question from the URL, the way
// `schema apply` does: a PostgreSQL URL without search_path describes the
// realm, one with it is bound, and SQLite has one namespace and is bound on
// every URL. The connection's schema owns unqualified objects in every row,
// and the connection's dialect is the target's.
func TestConnectionNaming(t *testing.T) {
	tests := []struct {
		name string
		info catalog.ServerInfo
		want atlasfilter.Naming
	}{
		{
			name: "PostgreSQL without search_path",
			info: catalog.ServerInfo{Dialect: "postgres", URL: "postgres://localhost/app?sslmode=disable", Schema: "public"},
			want: atlasfilter.Naming{DefaultSchema: "public", RealmRelative: true, Dialect: "postgres"},
		},
		{
			name: "PostgreSQL with search_path",
			info: catalog.ServerInfo{Dialect: "postgres", URL: "postgres://localhost/app?search_path=public", Schema: "public"},
			want: atlasfilter.Naming{DefaultSchema: "public", Dialect: "postgres"},
		},
		{
			name: "SQLite",
			info: catalog.ServerInfo{Dialect: "sqlite", URL: "sqlite://file?mode=memory", Schema: "main"},
			want: atlasfilter.Naming{DefaultSchema: "main", Dialect: "sqlite"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlasfilter.ConnectionNaming(test.info), qt.Equals, test.want)
		})
	}
}

// TestExclusion_ZeroValueReturnsItsInput is the control for the test above:
// without a selector both states come back as they went in, the same values,
// and no runtime was needed to say so.
func TestExclusion_ZeroValueReturnsItsInput(t *testing.T) {
	c := qt.New(t)
	var exclusion atlasfilter.Exclusion
	desired := exclusionDesired()
	observed := exclusionObserved()

	filteredDesired, desiredReport, desiredErr := exclusion.Desired(desired, atlasfilter.Naming{DefaultSchema: "main"})
	filteredObserved, observedReport, observedErr := exclusion.Observed(observed, atlasfilter.Naming{DefaultSchema: "main"})

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

			atlasfilter.ReportUnmatched(&out, nil, test.reports...)

			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}
