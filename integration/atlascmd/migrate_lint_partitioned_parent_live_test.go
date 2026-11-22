//go:build integration

package atlas_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestMigrateLintNamesAnEarlierPartitionedParentLive is stokaro/ptah#3588
// through `migrate lint --dev-url`: an index on a partitioned parent an earlier
// migration created got locking/create-index, whose CONCURRENTLY remedy PostgreSQL refuses on
// a partitioned table. The control leaves the same table ordinary, where locking/create-index
// is the right answer and stays.
func TestMigrateLintNamesAnEarlierPartitionedParentLive(t *testing.T) {
	tests := []struct {
		name    string
		first   string
		want    string
		code    string
		wantNot string
	}{
		{
			name: "the parent is partitioned",
			first: "CREATE TABLE measurements (id bigint NOT NULL, taken_on date NOT NULL, value int) PARTITION BY RANGE (taken_on);\n" +
				"CREATE TABLE measurements_2025 PARTITION OF measurements FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');\n",
			want:    "L1: building this index takes a SHARE lock on measurements and on every one of its partitions",
			code:    "https://atlasgo.io/lint/analyzers#PG108",
			wantNot: "analyzers#PG101",
		},
		{
			name:    "control: the same table left ordinary",
			first:   "CREATE TABLE measurements (id bigint NOT NULL, taken_on date NOT NULL, value int);\n",
			want:    "L1: CREATE INDEX without CONCURRENTLY blocks writes",
			code:    "https://atlasgo.io/lint/analyzers#PG101",
			wantNot: "analyzers#PG108",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			devURL := newCleanGateDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL), nil)
			dir := writeLintDirectory(c, map[string]string{
				"001_measurements.sql": test.first,
				"002_value.sql":        "CREATE INDEX m_value ON measurements (value);\n",
			})

			out, err := runCompatLint(c, dir, devURL)
			// The report wraps a diagnostic across lines, so the words are
			// compared with the wrapping taken out.
			words := strings.Join(strings.Fields(out), " ")

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
			c.Assert(words, qt.Contains, test.want)
			c.Assert(words, qt.Contains, test.code)
			c.Assert(words, qt.Not(qt.Contains), test.wantNot)
		})
	}
}
