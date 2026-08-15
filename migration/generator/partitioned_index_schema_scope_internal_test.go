package generator

// White-box testing required: which table an index ref names is resolved by
// unexported selectors before any SQL exists, and the fixture this file is
// built around -- two tables with the same bare name in different schemas --
// produces a rendered migration that looks identical whichever table the
// selector picked. Only the ref lists say which one it read.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// concurrentIndexOutcome is what the three concurrent-index selectors made of
// one index ref against one read schema.
type concurrentIndexOutcome struct {
	heuristic []types.IndexRef
	policy    []types.IndexRef
	policyErr error
	dropRefs  []types.IndexRef
	dropErr   error
}

// TestConcurrentIndexSelectors_ResolveARefToTheOrdinaryTableItNames pins the
// identity the partitioned rule is keyed on.
//
// A read schema can hold two different tables with the same bare name, one per
// schema, and an index ref carries whichever spelling the side of the diff it
// came from produced -- bare from the PostgreSQL reader for the connection's
// own schema, "schema.table" from a Go entity that declares one. Keying the
// partitioned rule on a set that pools both spellings makes one partitioned
// parent answer for every ordinary table sharing its bare name: the ordinary
// table silently loses its concurrent build, and an explicit
// diff.concurrent_index request is refused with a diagnostic naming a table
// that is not partitioned at all.
//
// The rows here are the refs that must still reach an ordinary table. The refs
// that must still be refused live in
// [TestConcurrentIndexSelectors_RefuseARefNamingAPartitionedParent]: resolving
// and refusing are two different measurements, and a pooled set gets one of
// them right whichever way it errs.
func TestConcurrentIndexSelectors_ResolveARefToTheOrdinaryTableItNames(t *testing.T) {
	tests := []struct {
		name   string
		tables []dbschematypes.DBTable
		ref    types.IndexRef
		// One want per selector. The heuristic reads EstimatedRows and the two
		// policy selectors are explicit requests, so a single expectation would
		// let one selector stop answering behind another that still does.
		wantHeuristic []types.IndexRef
		wantPolicy    []types.IndexRef
		wantDropRefs  []types.IndexRef
	}{
		{
			// One partitioned table must not poison every ordinary table that
			// shares its bare name. A pooled set fails this row three times.
			name: "a partitioned parent in another schema does not answer for a bare ref",
			tables: []dbschematypes.DBTable{
				{Name: "events", Schema: "app", Partitioned: true, EstimatedRows: 5000},
				{Name: "events", Schema: "public", EstimatedRows: 5000},
			},
			ref:           types.IndexRef{Name: "idx_events_tenant", TableName: "events"},
			wantHeuristic: []types.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
			wantPolicy:    []types.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
			wantDropRefs:  []types.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
		},
		{
			// The shape the PostgreSQL reader actually produces: it blanks the
			// schema of the connection's own schema, so the ordinary table is
			// the one the bare spelling names exactly.
			name: "a bare ref prefers the table whose own spelling is bare",
			tables: []dbschematypes.DBTable{
				{Name: "events", EstimatedRows: 5000},
				{Name: "events", Schema: "app", Partitioned: true, EstimatedRows: 5000},
			},
			ref:           types.IndexRef{Name: "idx_events_tenant", TableName: "events"},
			wantHeuristic: []types.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
			wantPolicy:    []types.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
			wantDropRefs:  []types.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
		},
		{
			name: "a bare ref reaching one ordinary schema-qualified table builds concurrently",
			tables: []dbschematypes.DBTable{
				{Name: "events", Schema: "app", EstimatedRows: 5000},
			},
			ref:           types.IndexRef{Name: "idx_events_tenant", TableName: "events"},
			wantHeuristic: []types.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
			wantPolicy:    []types.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
			wantDropRefs:  []types.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
		},
		{
			// One populated candidate is enough to prefer the concurrent build:
			// a blocking build on a table that turns out to hold rows is the
			// unrecoverable side of the guess.
			name: "a bare ref with one populated candidate builds concurrently",
			tables: []dbschematypes.DBTable{
				{Name: "events", Schema: "app", EstimatedRows: 0},
				{Name: "events", Schema: "reporting", EstimatedRows: 5000},
			},
			ref:           types.IndexRef{Name: "idx_events_tenant", TableName: "events"},
			wantHeuristic: []types.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
			wantPolicy:    []types.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
			wantDropRefs:  []types.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := runConcurrentIndexSelectors(test.tables, test.ref)

			c.Assert(got.policyErr, qt.IsNil)
			c.Assert(got.dropErr, qt.IsNil)
			c.Assert(got.heuristic, qt.DeepEquals, test.wantHeuristic)
			c.Assert(got.policy, qt.DeepEquals, test.wantPolicy)
			c.Assert(got.dropRefs, qt.DeepEquals, test.wantDropRefs)
		})
	}
}

// TestConcurrentIndexSelectors_RefuseARefNamingAPartitionedParent is the other
// half of [TestConcurrentIndexSelectors_ResolveARefToTheOrdinaryTableItNames]:
// the refs that do reach a partitioned parent, in each spelling that can reach
// one. PostgreSQL answers a concurrent index statement on relkind 'p' with
// SQLSTATE 0A000, so publishing one writes a plan that fails against the
// production database instead of against the developer who generated it.
//
// Each row pins the table the diagnostic names, not just that one was raised.
// A pooled lookup refuses these rows too -- naming a table that is not
// partitioned at all -- so "refused" alone cannot tell the two apart.
func TestConcurrentIndexSelectors_RefuseARefNamingAPartitionedParent(t *testing.T) {
	tests := []struct {
		name   string
		tables []dbschematypes.DBTable
		ref    types.IndexRef
		// wantOffending is the offending entry the refusal lists, which is the
		// ref's own spelling of the table it resolved to.
		wantOffending string
	}{
		{
			// The other direction: resolving by bare name only would lose the
			// parent this ref names exactly, and publish a statement the server
			// answers with SQLSTATE 0A000.
			name: "a qualified ref finds its own partitioned parent",
			tables: []dbschematypes.DBTable{
				{Name: "events", EstimatedRows: 5000},
				{Name: "events", Schema: "app", Partitioned: true, EstimatedRows: 5000},
			},
			ref:           types.IndexRef{Name: "idx_events_tenant", TableName: "app.events"},
			wantOffending: `"idx_events_tenant" on "app.events"`,
		},
		{
			// Ambiguity is not a licence to publish. When every table the bare
			// spelling can name is a partitioned parent, the statement is
			// unexecutable whichever one it meant.
			name: "a bare ref every candidate answers as partitioned is still refused",
			tables: []dbschematypes.DBTable{
				{Name: "events", Schema: "app", Partitioned: true, EstimatedRows: 5000},
				{Name: "events", Schema: "reporting", Partitioned: true, EstimatedRows: 5000},
			},
			ref:           types.IndexRef{Name: "idx_events_tenant", TableName: "events"},
			wantOffending: `"idx_events_tenant" on "events"`,
		},
		{
			// The bare fallback still has to reach a schema-qualified table --
			// the two sides of a diff do not have to agree on the spelling.
			name: "a bare ref reaching one schema-qualified parent is refused",
			tables: []dbschematypes.DBTable{
				{Name: "events", Schema: "app", Partitioned: true, EstimatedRows: 5000},
			},
			ref:           types.IndexRef{Name: "idx_events_tenant", TableName: "events"},
			wantOffending: `"idx_events_tenant" on "events"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := runConcurrentIndexSelectors(test.tables, test.ref)

			c.Assert(got.heuristic, qt.IsNil)
			c.Assert(got.policy, qt.IsNil)
			c.Assert(got.dropRefs, qt.IsNil)
			c.Assert(got.policyErr, qt.IsNotNil)
			c.Assert(got.policyErr.Error(), qt.Contains,
				"CREATE INDEX CONCURRENTLY requested by diff.concurrent_index.create "+
					"cannot be generated for partitioned table(s): "+test.wantOffending)
			c.Assert(got.dropErr, qt.IsNotNil)
			c.Assert(got.dropErr.Error(), qt.Contains,
				"DROP INDEX CONCURRENTLY requested by diff.concurrent_index.drop "+
					"cannot be generated for partitioned table(s): "+test.wantOffending)
		})
	}
}

// runConcurrentIndexSelectors puts one ref through all three selectors against
// one read schema. Each selector gets its own diff because they read different
// halves of it.
func runConcurrentIndexSelectors(
	tables []dbschematypes.DBTable,
	ref types.IndexRef,
) concurrentIndexOutcome {
	dbSchema := func() *dbschematypes.DBSchema {
		return &dbschematypes.DBSchema{Tables: tables}
	}
	additions := func() *types.SchemaDiff {
		diff := &types.SchemaDiff{}
		diff.SetIndexAdditions([]types.IndexRef{ref})
		return diff
	}
	removals := &types.SchemaDiff{}
	removals.SetIndexRemovals([]types.IndexRef{ref})

	var out concurrentIndexOutcome
	out.heuristic = concurrentIndexRefsForPopulatedTables(
		additions(),
		dbSchema(),
		postgresIndexDBInfo(),
	)
	out.policy, out.policyErr = concurrentIndexRefsForPolicy(
		additions(),
		dbSchema(),
		postgresIndexDBInfo(),
		DiffPolicy{ConcurrentIndex: true},
	)
	out.dropRefs, out.dropErr = concurrentIndexDropRefsForPolicy(
		removals,
		dbSchema(),
		postgresIndexDBInfo(),
		DiffPolicy{ConcurrentIndexDrop: true},
	)
	return out
}
