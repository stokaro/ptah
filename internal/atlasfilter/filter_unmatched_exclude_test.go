package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

// TestExcludeDatabaseReport_NamesSelectorsThatMatchedNothing is the emptiness
// half of stokaro/ptah#933: an exclude selector carries no match requirement,
// so before this report a selector that named nothing subtracted nothing and
// every verb exited 0 with the object still in its output.
//
// Every row asserts the report, not the object list, because the object list
// cannot tell the two apart — that is exactly why the failure was silent.
//
// Red without the report plumbing: `ExcludeDatabaseReport` does not exist and
// the package does not compile. Red with the tracking mutated to mark every
// pattern matched: the first two rows report nothing. Red with it mutated to
// mark none: the last four rows name selectors that did match.
func TestExcludeDatabaseReport_NamesSelectorsThatMatchedNothing(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		want     []string
	}{
		{
			name:     "a selector naming an absent object",
			patterns: []string{"nosuchobject"},
			want:     []string{"nosuchobject"},
		},
		{
			name:     "a qualified selector naming an absent schema",
			patterns: []string{"nosuch.mood"},
			want:     []string{"nosuch.mood"},
		},
		{
			name:     "a selector that matched reports nothing",
			patterns: []string{"public.mood"},
			want:     nil,
		},
		{
			// The reordered child filters: a selector naming an index inside a
			// table another selector already removed still named something.
			// Without the reorder the parent short-circuit hides the match and
			// this row reports "users.users_id_idx".
			name:     "a child selector under an already-excluded table",
			patterns: []string{"users", "users.users_id_idx"},
			want:     nil,
		},
		{
			// Two spellings of one object. Without visiting every pattern the
			// first match returns early and the second is reported empty.
			name:     "two selectors naming the same object",
			patterns: []string{"mood", "public.mood"},
			want:     nil,
		},
		{
			name:     "matched and unmatched selectors together",
			patterns: []string{"public.mood", "nosuchobject"},
			want:     []string{"nosuchobject"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			schema := qualifiedKindsFixture()
			schema.Indexes = []dbschematypes.DBIndex{
				{TableName: "users", Name: "users_id_idx", Columns: []string{"id"}},
			}

			_, report, err := atlasfilter.ExcludeDatabaseReport(schema, test.patterns, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(report.Unmatched, qt.DeepEquals, test.want)
		})
	}
}

// TestExcludeGeneratedReport_NamesSelectorsThatMatchedNothing is the
// desired-side mirror, so a comparison can intersect two reports that were
// produced the same way.
func TestExcludeGeneratedReport_NamesSelectorsThatMatchedNothing(t *testing.T) {
	c := qt.New(t)

	_, report, err := atlasfilter.ExcludeGeneratedReport(
		generatedQualifiedKindsFixture(), []string{"public.mood", "nosuchobject"}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(report.Unmatched, qt.DeepEquals, []string{"nosuchobject"})
}

// TestExcludeGeneratedReport_ReportsEverySelectorForAnAbsentState pins the
// early-return path. A nil state matched nothing because there was nothing to
// match, and reporting an empty list there would let a two-sided intersection
// conclude that every selector had matched.
func TestExcludeGeneratedReport_ReportsEverySelectorForAnAbsentState(t *testing.T) {
	c := qt.New(t)

	_, report, err := atlasfilter.ExcludeGeneratedReport(nil, []string{"mood", "fn_audit"}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(report.Unmatched, qt.DeepEquals, []string{"mood", "fn_audit"})
}

// TestScopeDatabaseReport_MeasuresEmptinessBeforeTheIncludeProjection pins the
// decision that the report is taken from the unprojected state. An --include
// that already dropped the object is not the --exclude selector failing, and
// reporting it as one would refuse a correct `schema apply`.
//
// Red if the report is taken from the projection: "public.mood" is reported
// unmatched because --include users removed the enum before the exclusion ran.
func TestScopeDatabaseReport_MeasuresEmptinessBeforeTheIncludeProjection(t *testing.T) {
	c := qt.New(t)

	_, report, err := atlasfilter.ScopeDatabaseReport(qualifiedKindsFixture(), atlasfilter.Scope{
		Include:       []string{"users"},
		Exclude:       []string{"public.mood"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Unmatched, qt.IsNil)
}

// TestUnmatchedAcrossStates_IntersectsSides is the rule that keeps a comparison
// from crying wolf. A selector naming an object that exists on one side only
// matches on that side alone, which is what a CREATE or a DROP looks like, so
// only a selector empty on every side is a scope that failed.
func TestUnmatchedAcrossStates_IntersectsSides(t *testing.T) {
	tests := []struct {
		name    string
		reports []atlasfilter.ExcludeReport
		want    []string
	}{
		{
			name:    "no states at all",
			reports: nil,
			want:    nil,
		},
		{
			name:    "one state answers alone",
			reports: []atlasfilter.ExcludeReport{{Unmatched: []string{"a"}}},
			want:    []string{"a"},
		},
		{
			name: "matched on one side is not a failure",
			reports: []atlasfilter.ExcludeReport{
				{Unmatched: []string{"a", "b"}},
				{Unmatched: []string{"b"}},
			},
			want: []string{"b"},
		},
		{
			name: "matched on every side reports nothing",
			reports: []atlasfilter.ExcludeReport{
				{Unmatched: []string{"a"}},
				{},
			},
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlasfilter.UnmatchedAcrossStates(test.reports...), qt.DeepEquals, test.want)
		})
	}
}

// TestAllowUnmatchedExclude_ReadsTheOptIn pins the escape hatch's spelling and
// its default. It is an environment variable rather than a flag because the
// conformance cli-surface tier asserts flag parity with the pinned community
// binary.
func TestAllowUnmatchedExclude_ReadsTheOptIn(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "unset keeps the safe default", value: "", want: false},
		{name: "false keeps the safe default", value: "false", want: false},
		{name: "unparsable keeps the safe default", value: "maybe", want: false},
		{name: "1 opts in", value: "1", want: true},
		{name: "true opts in", value: "true", want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlasfilter.AllowUnmatchedExcludeEnvVar, test.value)

			c.Assert(atlasfilter.AllowUnmatchedExclude(), qt.Equals, test.want)
		})
	}
}

// TestUnmatchedExcludeError_NamesEverySelector keeps the diagnostic quoting the
// spelling the user typed. A message that named the glob left after a type
// selector was cut off would send the reader looking for text they never wrote.
func TestUnmatchedExcludeError_NamesEverySelector(t *testing.T) {
	c := qt.New(t)

	err := &atlasfilter.UnmatchedExcludeError{Selectors: []string{"public.nosuch", "other"}}

	c.Assert(err.Error(), qt.Equals, `the --exclude selection matched no objects: "public.nosuch", "other"`)
}

// TestExcludeDatabaseReport_TypeSelectorReportsItsWrittenSpelling covers the
// one selector shape whose glob differs from what the user typed.
func TestExcludeDatabaseReport_TypeSelectorReportsItsWrittenSpelling(t *testing.T) {
	c := qt.New(t)

	_, report, err := atlasfilter.ExcludeDatabaseReport(
		&dbschematypes.DBSchema{Enums: []dbschematypes.DBEnum{{Name: "mood"}}},
		[]string{"nosuch*[type=table]"},
		"public")

	c.Assert(err, qt.IsNil)
	c.Assert(report.Unmatched, qt.DeepEquals, []string{"nosuch*[type=table]"})
}

// TestExcludeGeneratedReport_ChildOfARemovedTableIsReportedEmpty records a
// measured asymmetry rather than an intention, so that nobody reads the
// database-side row above and assumes both sides behave alike.
//
// A generated child is reached through its parent struct, and the parent left
// the map when the table selector removed it, so there is no name left to test
// the child selector against. The database side keeps the schema and table on
// the index row itself, which is why it can answer.
//
// This never reaches a refusal: `schema apply` intersects this report with the
// introspected side, where the same selector does match, and `schema diff` only
// warns. Making the two sides agree would mean reporting against the
// unfiltered table map, which is a change to how the projection is built and
// belongs with the item that needs it.
func TestExcludeGeneratedReport_ChildOfARemovedTableIsReportedEmpty(t *testing.T) {
	c := qt.New(t)
	schema := &goschema.Database{
		Tables:  []goschema.Table{{StructName: "User", Name: "users"}},
		Indexes: []goschema.Index{{StructName: "User", Name: "users_id_idx", Fields: []string{"id"}}},
	}

	_, generated, err := atlasfilter.ExcludeGeneratedReport(
		schema, []string{"users", "users.users_id_idx"}, "public")
	c.Assert(err, qt.IsNil)
	c.Assert(generated.Unmatched, qt.DeepEquals, []string{"users.users_id_idx"})

	_, database, err := atlasfilter.ExcludeDatabaseReport(
		&dbschematypes.DBSchema{
			Tables:  []dbschematypes.DBTable{{Name: "users", Columns: []dbschematypes.DBColumn{{Name: "id"}}}},
			Indexes: []dbschematypes.DBIndex{{TableName: "users", Name: "users_id_idx", Columns: []string{"id"}}},
		},
		[]string{"users", "users.users_id_idx"}, "public")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Unmatched, qt.IsNil)

	// The intersection is what keeps the asymmetry harmless.
	c.Assert(atlasfilter.UnmatchedAcrossStates(database, generated), qt.IsNil)
}
