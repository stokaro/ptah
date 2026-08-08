package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

// typeObjectKindsFixture holds one object of each kind the exclusion used to
// clone and never filter, in the connection's own schema and in a second one.
//
// A table rides along as the control: it was always filtered, so a regression
// that reached it would show up here rather than being read as a fault in the
// four kinds this fixture exists for.
func typeObjectKindsFixture() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "users", Columns: []dbschematypes.DBColumn{{Name: "id"}}},
		},
		Sequences: []dbschematypes.DBSequence{
			{Name: "order_seq"},
			{Schema: "app", Name: "app_seq"},
		},
		Domains: []dbschematypes.DBDomain{
			{Name: "positive_int", BaseType: "integer"},
			{Schema: "app", Name: "app_int", BaseType: "integer"},
		},
		Composites: []dbschematypes.DBComposite{
			{Name: "addr"},
			{Schema: "app", Name: "app_addr"},
		},
		Ranges: []dbschematypes.DBRange{
			{Name: "intrange", Subtype: "integer"},
			{Schema: "app", Name: "app_range", Subtype: "integer"},
		},
	}
}

func databaseDomainNames(domains []dbschematypes.DBDomain) []string {
	names := make([]string, 0, len(domains))
	for _, domain := range domains {
		names = append(names, domain.QualifiedName())
	}
	return names
}

func databaseCompositeNames(composites []dbschematypes.DBComposite) []string {
	names := make([]string, 0, len(composites))
	for _, composite := range composites {
		names = append(names, composite.QualifiedName())
	}
	return names
}

func databaseRangeNames(ranges []dbschematypes.DBRange) []string {
	names := make([]string, 0, len(ranges))
	for _, value := range ranges {
		names = append(names, value.QualifiedName())
	}
	return names
}

// TestExcludeDatabase_SubtractsSequencesDomainsCompositesAndRanges closes the
// gap that made the unmatched-selector refusal lie.
//
// These four kinds were cloned into the filtered schema and never offered to a
// single pattern, so `--exclude positive_int` against a database that really
// holds the domain `positive_int` removed nothing, still planned
// `DROP DOMAIN IF EXISTS "positive_int" CASCADE`, and — once an unmatched
// selector became a refusal — made `schema apply` exit 1 asserting the selector
// named no object. The include projection already selects all four by name
// (scope_database.go), so the exclusion was the asymmetric half.
//
// Each row asserts the surviving object list, because a silent no-op and a
// correct filter share an exit code. Red without the fix on every row except
// the two controls.
func TestExcludeDatabase_SubtractsSequencesDomainsCompositesAndRanges(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		assert  func(*qt.C, *dbschematypes.DBSchema)
	}{
		{
			name:    "sequence, bare name in the default schema",
			pattern: "order_seq",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseSequenceNames(got.Sequences), qt.DeepEquals, []string{"app.app_seq"})
			},
		},
		{
			name:    "sequence, qualified with the default schema",
			pattern: "public.order_seq",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseSequenceNames(got.Sequences), qt.DeepEquals, []string{"app.app_seq"})
			},
		},
		{
			name:    "sequence, qualified with a non-default schema",
			pattern: "app.app_seq",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseSequenceNames(got.Sequences), qt.DeepEquals, []string{"order_seq"})
			},
		},
		{
			name:    "domain, bare name in the default schema",
			pattern: "positive_int",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseDomainNames(got.Domains), qt.DeepEquals, []string{"app.app_int"})
			},
		},
		{
			name:    "domain, qualified with the default schema",
			pattern: "public.positive_int",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseDomainNames(got.Domains), qt.DeepEquals, []string{"app.app_int"})
			},
		},
		{
			name:    "domain, qualified with a non-default schema",
			pattern: "app.app_int",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseDomainNames(got.Domains), qt.DeepEquals, []string{"positive_int"})
			},
		},
		{
			name:    "composite type, bare name in the default schema",
			pattern: "addr",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseCompositeNames(got.Composites), qt.DeepEquals, []string{"app.app_addr"})
			},
		},
		{
			name:    "composite type, qualified with a non-default schema",
			pattern: "app.app_addr",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseCompositeNames(got.Composites), qt.DeepEquals, []string{"addr"})
			},
		},
		{
			name:    "range type, bare name in the default schema",
			pattern: "intrange",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseRangeNames(got.Ranges), qt.DeepEquals, []string{"app.app_range"})
			},
		},
		{
			name:    "range type, qualified with a non-default schema",
			pattern: "app.app_range",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseRangeNames(got.Ranges), qt.DeepEquals, []string{"intrange"})
			},
		},
		{
			// Inverse mutant: a filter that dropped by kind rather than by name
			// would take every object of the kind with it.
			name:    "a selector naming none of them removes none of them",
			pattern: "nosuchobject",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseSequenceNames(got.Sequences), qt.DeepEquals, []string{"order_seq", "app.app_seq"})
				c.Assert(databaseDomainNames(got.Domains), qt.DeepEquals, []string{"positive_int", "app.app_int"})
				c.Assert(databaseCompositeNames(got.Composites), qt.DeepEquals, []string{"addr", "app.app_addr"})
				c.Assert(databaseRangeNames(got.Ranges), qt.DeepEquals, []string{"intrange", "app.app_range"})
			},
		},
		{
			// Control: the kind that was always filtered still is, and the four
			// new arms do not take it with them.
			name:    "the table control is unaffected",
			pattern: "users",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(got.Tables, qt.HasLen, 0)
				c.Assert(databaseDomainNames(got.Domains), qt.DeepEquals, []string{"positive_int", "app.app_int"})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(
				typeObjectKindsFixture(), []string{test.pattern}, "public")

			c.Assert(err, qt.IsNil)
			test.assert(c, got)
		})
	}
}

// TestExcludeDatabaseReport_SelectorNamingATypeObjectIsNotEmpty is the report
// half of the row above, and the shape that turned a silent no-op into a hard
// failure: once an unmatched selector refused `schema apply`, a selector naming
// a real domain reported as naming nothing and the command exited 1 asserting
// something factually false.
//
// Red without the fix: every row reports its own selector.
func TestExcludeDatabaseReport_SelectorNamingATypeObjectIsNotEmpty(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
	}{
		{name: "a sequence", patterns: []string{"order_seq"}},
		{name: "a domain", patterns: []string{"positive_int"}},
		{name: "a composite type", patterns: []string{"addr"}},
		{name: "a range type", patterns: []string{"intrange"}},
		{name: "a domain in a second schema", patterns: []string{"app.app_int"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, report, err := atlasfilter.ExcludeDatabaseReport(
				typeObjectKindsFixture(), test.patterns, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(report.Unmatched, qt.IsNil)
		})
	}
}

// TestExcludeDatabaseReport_StillNamesASelectorThatMatchedNoTypeObject is the
// inverse mutant of the test above. A fix that stopped reporting these kinds
// instead of filtering them — marking every pattern matched once a sequence,
// domain, composite or range was present — would pass every row above and turn
// this one red.
func TestExcludeDatabaseReport_StillNamesASelectorThatMatchedNoTypeObject(t *testing.T) {
	c := qt.New(t)

	_, report, err := atlasfilter.ExcludeDatabaseReport(
		typeObjectKindsFixture(), []string{"positive_int", "nosuch_domain"}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(report.Unmatched, qt.DeepEquals, []string{"nosuch_domain"})
}

// TestExcludeGenerated_SubtractsTheSameTypeObjectKinds is the desired-side
// mirror. Both sides of a comparison must subtract the same objects: a domain
// removed from the introspected side alone comes back as a CREATE.
func TestExcludeGenerated_SubtractsTheSameTypeObjectKinds(t *testing.T) {
	c := qt.New(t)
	schema := &goschema.Database{
		Sequences:      []goschema.Sequence{{Name: "order_seq"}, {Schema: "app", Name: "app_seq"}},
		Domains:        []goschema.Domain{{Name: "positive_int", BaseType: "integer"}, {Schema: "app", Name: "app_int", BaseType: "integer"}},
		CompositeTypes: []goschema.CompositeType{{Name: "addr"}, {Schema: "app", Name: "app_addr"}},
		Ranges:         []goschema.Range{{Name: "intrange", Subtype: "integer"}, {Schema: "app", Name: "app_range", Subtype: "integer"}},
	}

	got, report, err := atlasfilter.ExcludeGeneratedReport(
		schema,
		[]string{"public.positive_int", "order_seq", "app.app_addr", "intrange"},
		"public")

	c.Assert(err, qt.IsNil)
	c.Assert(report.Unmatched, qt.IsNil)
	c.Assert(generatedSequenceNames(got.Sequences), qt.DeepEquals, []string{"app.app_seq"})
	c.Assert(generatedDomainNames(got.Domains), qt.DeepEquals, []string{"app.app_int"})
	c.Assert(generatedCompositeNames(got.CompositeTypes), qt.DeepEquals, []string{"addr"})
	c.Assert(generatedRangeNames(got.Ranges), qt.DeepEquals, []string{"app.app_range"})
}

func generatedDomainNames(domains []goschema.Domain) []string {
	names := make([]string, 0, len(domains))
	for _, domain := range domains {
		names = append(names, dbschematypes.QualifyTableName(domain.Schema, domain.Name))
	}
	return names
}

func generatedCompositeNames(types []goschema.CompositeType) []string {
	names := make([]string, 0, len(types))
	for _, composite := range types {
		names = append(names, dbschematypes.QualifyTableName(composite.Schema, composite.Name))
	}
	return names
}

func generatedRangeNames(ranges []goschema.Range) []string {
	names := make([]string, 0, len(ranges))
	for _, value := range ranges {
		names = append(names, dbschematypes.QualifyTableName(value.Schema, value.Name))
	}
	return names
}

// TestExcludeDatabaseReport_ColumnSelectorUnderAnExcludedTableIsNotEmpty is the
// column half of the same rule.
//
// filterTables `continue`s the moment a table selector matches, so filterColumns
// was never reached for that table and the column patterns were never asked.
// `--exclude users --exclude users.id` therefore reported `users.id` as naming
// nothing, which the refusal turned into `schema apply` exit 1 on a database
// where the column plainly exists. The reorder the other child filters carry
// does not cover this one, because the parent does not merely short-circuit the
// decision here — it skips the pass entirely.
//
// Red without the fix: the first row reports ["users.id"].
func TestExcludeDatabaseReport_ColumnSelectorUnderAnExcludedTableIsNotEmpty(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		want     []string
	}{
		{
			name:     "a column of a table another selector removed",
			patterns: []string{"users", "users.id"},
			want:     nil,
		},
		{
			name:     "the qualified column spelling under the same removed table",
			patterns: []string{"public.users", "users.id"},
			want:     nil,
		},
		{
			// Inverse mutant: asking must stay a name test, not a blanket mark.
			// A fix that marked every pattern once a table was excluded would
			// pass the rows above and turn this one green-to-red.
			name:     "a column that does not exist is still reported",
			patterns: []string{"users", "users.nosuchcolumn"},
			want:     []string{"users.nosuchcolumn"},
		},
		{
			// Control: the column selector alone, with the table kept, was
			// already answered before this change.
			name:     "the column selector alone still matches",
			patterns: []string{"users.id"},
			want:     nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, report, err := atlasfilter.ExcludeDatabaseReport(
				&dbschematypes.DBSchema{
					Tables: []dbschematypes.DBTable{
						{Name: "users", Columns: []dbschematypes.DBColumn{{Name: "id"}, {Name: "name"}}},
					},
				},
				test.patterns, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(report.Unmatched, qt.DeepEquals, test.want)
		})
	}
}

// TestExcludeDatabase_ColumnSelectorUnderAnExcludedTableChangesNothing is the
// keep-decision control for the test above. Asking the column patterns must not
// alter what survives: the table and its columns leave together either way.
func TestExcludeDatabase_ColumnSelectorUnderAnExcludedTableChangesNothing(t *testing.T) {
	c := qt.New(t)
	fixture := func() *dbschematypes.DBSchema {
		return &dbschematypes.DBSchema{
			Tables: []dbschematypes.DBTable{
				{Name: "users", Columns: []dbschematypes.DBColumn{{Name: "id"}, {Name: "name"}}},
				{Name: "posts", Columns: []dbschematypes.DBColumn{{Name: "id"}, {Name: "body"}}},
			},
		}
	}

	withColumn, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(
		fixture(), []string{"users", "users.id"}, "public")
	c.Assert(err, qt.IsNil)

	withoutColumn, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(
		fixture(), []string{"users"}, "public")
	c.Assert(err, qt.IsNil)

	c.Assert(withColumn.Tables, qt.DeepEquals, withoutColumn.Tables)
	c.Assert(withColumn.Tables, qt.HasLen, 1)
}
