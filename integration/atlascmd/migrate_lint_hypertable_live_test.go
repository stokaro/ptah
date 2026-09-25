//go:build integration

package atlas_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestMigrateLintNamesTheHypertableRemedyLive is the directory
// stokaro/ptah#3551 reported through `migrate lint --dev-url`: PG101 advised
// CREATE INDEX CONCURRENTLY for an index on a hypertable, and TimescaleDB
// refuses that statement there. The control leaves the same table ordinary,
// where CONCURRENTLY is the right advice and stays.
func TestMigrateLintNamesTheHypertableRemedyLive(t *testing.T) {
	const events = "CREATE TABLE events (\n" +
		"    id          BIGINT GENERATED ALWAYS AS IDENTITY,\n" +
		"    happened_at TIMESTAMPTZ NOT NULL,\n" +
		"    kind        TEXT NOT NULL,\n" +
		"    PRIMARY KEY (id, happened_at)\n" +
		");\n"
	tests := []struct {
		name    string
		first   string
		want    string
		wantNot string
	}{
		{
			name:    "the table is a hypertable",
			first:   events + "\nSELECT create_hypertable('events', 'happened_at');\n",
			want:    "CREATE INDEX on the hypertable events blocks writes to it and to every chunk",
			wantNot: "use CREATE INDEX CONCURRENTLY",
		},
		{
			name:    "control: the same table left ordinary",
			first:   events,
			want:    "use CREATE INDEX CONCURRENTLY outside a transaction",
			wantNot: "on the hypertable",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			devURL := newCleanGateDatabase(c, dbtarget.URL(c, dbtarget.TimescaleDB),
				[]string{"CREATE EXTENSION IF NOT EXISTS timescaledb"})
			dir := writeLintDirectory(c, map[string]string{
				"001_events.sql":      test.first,
				"002_events_kind.sql": "CREATE INDEX events_kind_idx ON events (kind, happened_at DESC);\n",
			})

			out, err := runCompatLint(c, dir, devURL)
			// The report wraps a diagnostic across lines, so the words are
			// compared with the wrapping taken out.
			words := strings.Join(strings.Fields(out), " ")

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
			c.Assert(words, qt.Contains, "[PG101]: ")
			c.Assert(words, qt.Contains, test.want)
			c.Assert(words, qt.Not(qt.Contains), test.wantNot)
		})
	}
}
