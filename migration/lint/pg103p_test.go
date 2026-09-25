package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestPG103P_PerChunkBuildInATransaction pins where the TimescaleDB per-chunk
// build is reported (stokaro/ptah#3587). It commits one transaction per chunk,
// so TimescaleDB refuses it inside the migration's transaction, measured on
// 2.30.1 over PostgreSQL 18.6 as SQLSTATE 25001, the way PostgreSQL refuses
// CONCURRENTLY. Each row moves one fact away from the reporting case: the
// marker, the direction, the value of the parameter, and CONCURRENTLY, which
// stays PG103's.
func TestPG103P_PerChunkBuildInATransaction(t *testing.T) {
	const perChunk = "CREATE INDEX events_kind_idx ON events (kind) WITH (timescaledb.transaction_per_chunk);\n"
	tests := []struct {
		name string
		up   string
		down string
		want []string
	}{
		{
			name: "a transactional up file",
			up:   perChunk,
			down: "-- +ptah no_transaction\nDROP INDEX CONCURRENTLY events_kind_idx;\n",
			want: []string{"PG103P 0000000001_x.up.sql"},
		},
		{
			name: "the marker the finding names",
			up:   "-- +ptah no_transaction\n" + perChunk,
			down: "-- +ptah no_transaction\nDROP INDEX CONCURRENTLY events_kind_idx;\n",
			want: make([]string, 0),
		},
		{
			name: "a transactional down file",
			up:   "-- +ptah no_transaction\nDROP INDEX CONCURRENTLY events_kind_idx;\n",
			down: perChunk,
			want: []string{"PG103P 0000000001_x.down.sql"},
		},
		{
			name: "set to false, which builds like a plain index",
			up:   "CREATE INDEX events_kind_idx ON events (kind) WITH (timescaledb.transaction_per_chunk = false);\n",
			down: "-- +ptah no_transaction\nDROP INDEX CONCURRENTLY events_kind_idx;\n",
			want: []string{"PG101 0000000001_x.up.sql"},
		},
		{
			name: "CONCURRENTLY stays PG103's",
			up:   "CREATE INDEX CONCURRENTLY events_kind_idx ON events (kind);\n",
			down: "-- +ptah no_transaction\nDROP INDEX CONCURRENTLY events_kind_idx;\n",
			want: []string{"PG103 0000000001_x.up.sql"},
		},
		{
			name: "mixed with transactional DDL",
			up:   "ALTER TABLE events ADD COLUMN note text;\n" + perChunk,
			down: "-- +ptah no_transaction\nDROP INDEX CONCURRENTLY events_kind_idx;\n",
			want: []string{"PG103P 0000000001_x.up.sql", "TX101 0000000001_x.up.sql"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := lintPair(c, test.up, test.down)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
