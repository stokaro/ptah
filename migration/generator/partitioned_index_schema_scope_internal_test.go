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

// TestConcurrentIndexSelectors_ResolveTheTableTheRefNames pins the identity the
// partitioned rule is keyed on.
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
// The rows below separate that pooling from the rule in four directions: a
// qualified ref must still find its own partitioned parent, a bare ref must
// prefer the table that answers to it exactly, a bare ref every candidate
// answers as partitioned must still be refused, and a bare ref reaching a
// single schema-qualified parent must still be refused.
func TestConcurrentIndexSelectors_ResolveTheTableTheRefNames(t *testing.T) {
	tests := []struct {
		name   string
		tables []dbschematypes.DBTable
		ref    types.IndexRef
		assert func(c *qt.C, got concurrentIndexOutcome)
	}{
		{
			// One partitioned table must not poison every ordinary table that
			// shares its bare name. A pooled set fails this row three times.
			name: "a partitioned parent in another schema does not answer for a bare ref",
			tables: []dbschematypes.DBTable{
				{Name: "events", Schema: "app", Partitioned: true, EstimatedRows: 5000},
				{Name: "events", Schema: "public", EstimatedRows: 5000},
			},
			ref: types.IndexRef{Name: "idx_events_tenant", TableName: "events"},
			assert: func(c *qt.C, got concurrentIndexOutcome) {
				c.Assert(got.policyErr, qt.IsNil)
				c.Assert(got.dropErr, qt.IsNil)
				c.Assert(got.heuristic, qt.DeepEquals, []types.IndexRef{
					{Name: "idx_events_tenant", TableName: "events"},
				})
				c.Assert(got.policy, qt.DeepEquals, []types.IndexRef{
					{Name: "idx_events_tenant", TableName: "events"},
				})
				c.Assert(got.dropRefs, qt.DeepEquals, []types.IndexRef{
					{Name: "idx_events_tenant", TableName: "events"},
				})
			},
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
			ref: types.IndexRef{Name: "idx_events_tenant", TableName: "events"},
			assert: func(c *qt.C, got concurrentIndexOutcome) {
				c.Assert(got.policyErr, qt.IsNil)
				c.Assert(got.dropErr, qt.IsNil)
				c.Assert(got.heuristic, qt.DeepEquals, []types.IndexRef{
					{Name: "idx_events_tenant", TableName: "events"},
				})
				c.Assert(got.policy, qt.DeepEquals, []types.IndexRef{
					{Name: "idx_events_tenant", TableName: "events"},
				})
			},
		},
		{
			// The other direction: resolving by bare name only would lose the
			// parent this ref names exactly, and publish a statement the server
			// answers with SQLSTATE 0A000.
			name: "a qualified ref finds its own partitioned parent",
			tables: []dbschematypes.DBTable{
				{Name: "events", EstimatedRows: 5000},
				{Name: "events", Schema: "app", Partitioned: true, EstimatedRows: 5000},
			},
			ref: types.IndexRef{Name: "idx_events_tenant", TableName: "app.events"},
			assert: func(c *qt.C, got concurrentIndexOutcome) {
				c.Assert(got.heuristic, qt.IsNil)
				c.Assert(got.policy, qt.IsNil)
				c.Assert(got.policyErr, qt.IsNotNil)
				c.Assert(got.policyErr.Error(), qt.Contains, `"idx_events_tenant" on "app.events"`)
				c.Assert(got.dropRefs, qt.IsNil)
				c.Assert(got.dropErr, qt.IsNotNil)
				c.Assert(got.dropErr.Error(), qt.Contains, `"idx_events_tenant" on "app.events"`)
			},
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
			ref: types.IndexRef{Name: "idx_events_tenant", TableName: "events"},
			assert: func(c *qt.C, got concurrentIndexOutcome) {
				c.Assert(got.heuristic, qt.IsNil)
				c.Assert(got.policy, qt.IsNil)
				c.Assert(got.policyErr, qt.IsNotNil)
				c.Assert(got.dropRefs, qt.IsNil)
				c.Assert(got.dropErr, qt.IsNotNil)
			},
		},
		{
			// The bare fallback still has to reach a schema-qualified table --
			// the two sides of a diff do not have to agree on the spelling.
			name: "a bare ref reaching one schema-qualified parent is refused",
			tables: []dbschematypes.DBTable{
				{Name: "events", Schema: "app", Partitioned: true, EstimatedRows: 5000},
			},
			ref: types.IndexRef{Name: "idx_events_tenant", TableName: "events"},
			assert: func(c *qt.C, got concurrentIndexOutcome) {
				c.Assert(got.heuristic, qt.IsNil)
				c.Assert(got.policy, qt.IsNil)
				c.Assert(got.policyErr, qt.IsNotNil)
				c.Assert(got.dropRefs, qt.IsNil)
				c.Assert(got.dropErr, qt.IsNotNil)
			},
		},
		{
			name: "a bare ref reaching one ordinary schema-qualified table builds concurrently",
			tables: []dbschematypes.DBTable{
				{Name: "events", Schema: "app", EstimatedRows: 5000},
			},
			ref: types.IndexRef{Name: "idx_events_tenant", TableName: "events"},
			assert: func(c *qt.C, got concurrentIndexOutcome) {
				c.Assert(got.policyErr, qt.IsNil)
				c.Assert(got.heuristic, qt.DeepEquals, []types.IndexRef{
					{Name: "idx_events_tenant", TableName: "events"},
				})
			},
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
			ref: types.IndexRef{Name: "idx_events_tenant", TableName: "events"},
			assert: func(c *qt.C, got concurrentIndexOutcome) {
				c.Assert(got.heuristic, qt.DeepEquals, []types.IndexRef{
					{Name: "idx_events_tenant", TableName: "events"},
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tt.assert(c, runConcurrentIndexSelectors(tt.tables, tt.ref))
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
