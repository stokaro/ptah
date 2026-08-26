package generator

// White-box testing required: the concurrent-index decision is taken by
// unexported ref selectors before any SQL exists, and the distinctions under
// test -- a partitioned parent, and row statistics the database could not
// report -- are erased by the time the public API returns rendered migration
// files.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func postgresIndexDBInfo() catalog.ServerInfo {
	return catalog.ServerInfo{
		Dialect:      platform.Postgres,
		Capabilities: capability.Postgres17(),
	}
}

// TestConcurrentIndexRefsForPopulatedTables_PartitionedAndUnknownStats pins the
// two catalog facts the heuristic must read beyond "estimated rows".
//
// A partitioned parent has no concurrent index build at all: PostgreSQL answers
// CREATE INDEX CONCURRENTLY on relkind 'p' with SQLSTATE 0A000, measured on
// 17.10. Row statistics that the database could not report are not a row count
// of zero: pg_class.reltuples is -1 until something analyzes the relation, and
// n_live_tup is 0 after the cumulative counters are reset, so a table holding
// five thousand rows reports the same numbers an empty one does.
func TestConcurrentIndexRefsForPopulatedTables_PartitionedAndUnknownStats(t *testing.T) {
	tests := []struct {
		name   string
		tables []catalog.Table
		want   []difftypes.IndexRef
	}{
		{
			name:   "populated ordinary table builds concurrently",
			tables: []catalog.Table{{Name: "events", EstimatedRows: 5000}},
			want:   []difftypes.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
		},
		{
			// The discriminating row: an exclusion keyed on "the schema contains
			// a partitioned table" rather than on "this index names one" passes
			// every other row and fails here.
			name: "an unrelated partitioned table does not downgrade the build",
			tables: []catalog.Table{
				{Name: "events", EstimatedRows: 5000},
				{Name: "measurements", EstimatedRows: 5000, Partitioned: true},
			},
			want: []difftypes.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
		},
		{
			name:   "reported empty table stays transactional",
			tables: []catalog.Table{{Name: "events", EstimatedRows: 0}},
			want:   nil,
		},
		{
			name:   "unreported row statistics build concurrently",
			tables: []catalog.Table{{Name: "events", EstimatedRows: 0, RowStatsUnknown: true}},
			want:   []difftypes.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
		},
		{
			name:   "populated partitioned parent stays transactional",
			tables: []catalog.Table{{Name: "events", EstimatedRows: 5000, Partitioned: true}},
			want:   nil,
		},
		{
			name: "partitioned parent with unreported statistics stays transactional",
			tables: []catalog.Table{{
				Name:            "events",
				EstimatedRows:   0,
				RowStatsUnknown: true,
				Partitioned:     true,
			}},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}
			diff.SetIndexAdditions([]difftypes.IndexRef{
				{Name: "idx_events_tenant", TableName: "events"},
			})

			got := concurrentIndexRefsForPopulatedTables(
				diff,
				&catalog.Database{Tables: tt.tables},
				postgresIndexDBInfo(),
			)

			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

// TestConcurrentIndexPolicy_RefusesPartitionedParent covers the explicitly
// requested half. The heuristic downgrades because nothing asked for a
// concurrent statement; a project that set diff.concurrent_index is asking, and
// the answer PostgreSQL gives cannot be honored -- so generation fails here,
// before a migration file, its checksum, and its commit exist.
func TestConcurrentIndexPolicy_RefusesPartitionedParent(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions([]difftypes.IndexRef{
		{Name: "idx_events_tenant", TableName: "events"},
	})

	refs, err := concurrentIndexRefsForPolicy(
		diff,
		&catalog.Database{Tables: []catalog.Table{{Name: "events", Partitioned: true}}},
		postgresIndexDBInfo(),
		DiffPolicy{ConcurrentIndex: true},
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "CREATE INDEX CONCURRENTLY")
	c.Assert(err.Error(), qt.Contains, "diff.concurrent_index.create")
	c.Assert(err.Error(), qt.Contains, `"idx_events_tenant" on "events"`)
	c.Assert(err.Error(), qt.Contains, "0A000")
	c.Assert(refs, qt.IsNil)
}

// TestConcurrentIndexPolicy_HonorsATableTheBuildIsLegalOn is what keeps the
// refusal next door a statement about the index rather than about the schema.
func TestConcurrentIndexPolicy_HonorsATableTheBuildIsLegalOn(t *testing.T) {
	tests := []struct {
		name   string
		tables []catalog.Table
		want   []difftypes.IndexRef
	}{
		{
			name:   "create over an ordinary table is honored",
			tables: []catalog.Table{{Name: "events"}},
			want:   []difftypes.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
		},
		{
			// The discriminating row: a refusal keyed on "the schema contains a
			// partitioned table" rather than on "this index names one" passes
			// the refusal test and fails here.
			name: "an unrelated partitioned table does not refuse the run",
			tables: []catalog.Table{
				{Name: "events"},
				{Name: "measurements", Partitioned: true},
			},
			want: []difftypes.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}
			diff.SetIndexAdditions([]difftypes.IndexRef{
				{Name: "idx_events_tenant", TableName: "events"},
			})

			refs, err := concurrentIndexRefsForPolicy(
				diff,
				&catalog.Database{Tables: tt.tables},
				postgresIndexDBInfo(),
				DiffPolicy{ConcurrentIndex: true},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(refs, qt.DeepEquals, tt.want)
		})
	}
}

// TestConcurrentIndexDropPolicy_RefusesPartitionedParent is the drop half.
// PostgreSQL refuses DROP INDEX CONCURRENTLY on a partitioned index with its
// own message ("cannot drop partitioned index ... concurrently"), so the
// rollback of a partitioned build is unexecutable in exactly the same way the
// build is -- and a rollback nobody ran is where that goes unnoticed.
func TestConcurrentIndexDropPolicy_RefusesPartitionedParent(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexRemovals([]difftypes.IndexRef{
		{Name: "idx_events_tenant", TableName: "events"},
	})

	refs, err := concurrentIndexDropRefsForPolicy(
		diff,
		&catalog.Database{Tables: []catalog.Table{{Name: "events", Partitioned: true}}},
		postgresIndexDBInfo(),
		DiffPolicy{ConcurrentIndexDrop: true},
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "DROP INDEX CONCURRENTLY")
	c.Assert(err.Error(), qt.Contains, "diff.concurrent_index.drop")
	c.Assert(err.Error(), qt.Contains, `"idx_events_tenant" on "events"`)
	c.Assert(refs, qt.IsNil)
}

// TestConcurrentIndexDropPolicy_HonorsATableTheDropIsLegalOn is the drop half's
// counterpart to TestConcurrentIndexPolicy_HonorsATableTheBuildIsLegalOn, and
// carries the same discriminating row.
func TestConcurrentIndexDropPolicy_HonorsATableTheDropIsLegalOn(t *testing.T) {
	tests := []struct {
		name   string
		tables []catalog.Table
		want   []difftypes.IndexRef
	}{
		{
			name:   "drop over an ordinary table is honored",
			tables: []catalog.Table{{Name: "events"}},
			want:   []difftypes.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
		},
		{
			name: "an unrelated partitioned table does not refuse the run",
			tables: []catalog.Table{
				{Name: "events"},
				{Name: "measurements", Partitioned: true},
			},
			want: []difftypes.IndexRef{{Name: "idx_events_tenant", TableName: "events"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}
			diff.SetIndexRemovals([]difftypes.IndexRef{
				{Name: "idx_events_tenant", TableName: "events"},
			})

			refs, err := concurrentIndexDropRefsForPolicy(
				diff,
				&catalog.Database{Tables: tt.tables},
				postgresIndexDBInfo(),
				DiffPolicy{ConcurrentIndexDrop: true},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(refs, qt.DeepEquals, tt.want)
		})
	}
}
