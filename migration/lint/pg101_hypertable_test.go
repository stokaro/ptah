package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// PG101 names CREATE INDEX CONCURRENTLY as the remedy for a blocking build,
// and TimescaleDB refuses that statement on a hypertable (stokaro/ptah#3551).
// Only the starting state can say a table is a hypertable, so the state
// refines the advice, and TimescaleDB's own remedy, a per-chunk build, is not
// reported as the hazard it replaces.

const pg101OrdinaryAdvice = "CREATE INDEX without CONCURRENTLY blocks writes to the table for the whole build; " +
	"on a populated table use CREATE INDEX CONCURRENTLY outside a transaction"

// pg101HypertableAdvice is PG101's message for an index on the hypertable
// spelled name.
func pg101HypertableAdvice(name string) string {
	return "CREATE INDEX on the hypertable " + name + " blocks writes to it and to every chunk for the whole " +
		"build, and TimescaleDB refuses CONCURRENTLY on a hypertable; " +
		"WITH (timescaledb.transaction_per_chunk) in a no_transaction migration builds one chunk at a time, so only " +
		"writes to the chunk being indexed wait. A per-chunk build that is interrupted leaves the index invalid, and " +
		"IF NOT EXISTS then skips it, so drop the index before you retry"
}

// eventsFS is a directory whose first version creates events and whose second
// is the statement under test, so the index is built on a table an earlier
// migration made.
func eventsFS(migration string) map[string]string {
	return map[string]string{
		"1_events.sql": "CREATE TABLE events (id bigint, happened_at timestamptz NOT NULL, kind text NOT NULL);",
		"2_change.sql": migration,
	}
}

// eventsColumns is the starting state of version 2 for events in each schema
// named, the way the dev-database read reports it.
func eventsColumns(schemas ...string) []lint.BaselineColumn {
	var columns []lint.BaselineColumn
	for _, schema := range schemas {
		for _, name := range []string{"id", "happened_at", "kind"} {
			columns = append(columns, lint.BaselineColumn{Version: 2, Schema: schema, Table: "events", Name: name})
		}
	}
	return columns
}

func analyzeEvents(c *qt.C, migration string, columns []lint.BaselineColumn, hypertables []lint.BaselineHypertable) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fixture(eventsFS(migration)), lint.Options{
		Dialect:             "postgres",
		DirFormat:           migrationfile.DirFormatAtlas,
		Selection:           lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Baseline:            columns,
		BaselineHypertables: hypertables,
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// pg101Messages keeps the messages of PG101's findings, so a case asserts on
// this rule and not on the others a CREATE INDEX meets.
func pg101Messages(findings []lint.Finding) []string {
	var messages []string
	for _, finding := range findings {
		if finding.Rule == "PG101" {
			messages = append(messages, finding.Message)
		}
	}
	return messages
}

// pg101Unmet keeps PG101's entries of [lint.Analysis.UnmetInputs], spelled
// by input.
func pg101Unmet(analysis lint.Analysis) []string {
	var inputs []string
	for _, entry := range analysis.UnmetInputs() {
		if entry.Rule == "PG101" {
			inputs = append(inputs, entry.Input.String())
		}
	}
	return inputs
}

func TestPG101_AdviceFollowsTheStartingState(t *testing.T) {
	tests := []struct {
		name        string
		migration   string
		columns     []lint.BaselineColumn
		hypertables []lint.BaselineHypertable
		want        []string
	}{
		{
			name:      "an ordinary table keeps the CONCURRENTLY advice",
			migration: "CREATE INDEX events_kind_idx ON events (kind);",
			columns:   eventsColumns("public"),
			want:      []string{pg101OrdinaryAdvice},
		},
		{
			name:        "a hypertable gets TimescaleDB's remedy instead",
			migration:   "CREATE INDEX events_kind_idx ON events (kind, happened_at DESC);",
			columns:     eventsColumns("public"),
			hypertables: []lint.BaselineHypertable{{Version: 2, Schema: "public", Table: "events"}},
			want:        []string{pg101HypertableAdvice("events")},
		},
		{
			name:        "a qualified reference names the hypertable as written",
			migration:   "CREATE UNIQUE INDEX events_id_uq ON public.events (id, happened_at);",
			columns:     eventsColumns("public"),
			hypertables: []lint.BaselineHypertable{{Version: 2, Schema: "public", Table: "events"}},
			want:        []string{pg101HypertableAdvice("public.events")},
		},
		{
			name:        "a hypertable of the same name in another schema is not this table",
			migration:   "CREATE INDEX events_kind_idx ON public.events (kind);",
			columns:     eventsColumns("public", "archive"),
			hypertables: []lint.BaselineHypertable{{Version: 2, Schema: "archive", Table: "events"}},
			want:        []string{pg101OrdinaryAdvice},
		},
		{
			name:        "a bare name two schemas carry is placed in neither",
			migration:   "CREATE INDEX events_kind_idx ON events (kind);",
			columns:     eventsColumns("public", "archive"),
			hypertables: []lint.BaselineHypertable{{Version: 2, Schema: "public", Table: "events"}},
			want:        []string{pg101OrdinaryAdvice},
		},
		{
			name:        "the state of another version says nothing about this one",
			migration:   "CREATE INDEX events_kind_idx ON events (kind);",
			columns:     eventsColumns("public"),
			hypertables: []lint.BaselineHypertable{{Version: 3, Schema: "public", Table: "events"}},
			want:        []string{pg101OrdinaryAdvice},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeEvents(c, test.migration, test.columns, test.hypertables)
			c.Assert(pg101Messages(analysis.Findings()), qt.DeepEquals, test.want)
		})
	}
}

// TestPG101_PerChunkBuildIsTheRemedy pins the storage parameter PG101 reads.
// Measured on TimescaleDB 2.30.1: without a value or with a value PostgreSQL
// reads as true the build locks one chunk at a time; `false`, `off` and `0`
// lock the hypertable and every chunk for the whole build, the hazard PG101
// reports, and `o` is refused as not a valid bool.
func TestPG101_PerChunkBuildIsTheRemedy(t *testing.T) {
	tests := []struct {
		name      string
		migration string
		want      []string
	}{
		{
			name:      "the parameter without a value",
			migration: "CREATE INDEX events_kind_idx ON events (kind) WITH (timescaledb.transaction_per_chunk);",
		},
		{
			name:      "set to true",
			migration: "CREATE INDEX events_kind_idx ON events (kind) WITH (timescaledb.transaction_per_chunk = true);",
		},
		{
			name:      "set to a prefix of true",
			migration: "CREATE INDEX events_kind_idx ON events (kind) WITH (timescaledb.transaction_per_chunk = t);",
		},
		{
			name:      "set to yes",
			migration: "CREATE INDEX events_kind_idx ON events (kind) WITH (timescaledb.transaction_per_chunk = yes);",
		},
		{
			name:      "set to a quoted on, with the namespace quoted and another parameter first",
			migration: `CREATE INDEX events_kind_idx ON events (kind) WITH (fillfactor = 70, "timescaledb".TRANSACTION_PER_CHUNK = 'on');`,
		},
		{
			name:      "set to false",
			migration: "CREATE INDEX events_kind_idx ON events (kind) WITH (timescaledb.transaction_per_chunk = false);",
			want:      []string{pg101OrdinaryAdvice},
		},
		{
			name:      "set to off",
			migration: "CREATE INDEX events_kind_idx ON events (kind) WITH (timescaledb.transaction_per_chunk = off);",
			want:      []string{pg101OrdinaryAdvice},
		},
		{
			name:      "set to 0",
			migration: "CREATE INDEX events_kind_idx ON events (kind) WITH (timescaledb.transaction_per_chunk = 0);",
			want:      []string{pg101OrdinaryAdvice},
		},
		{
			name:      "set to o, which the engine refuses as ambiguous",
			migration: "CREATE INDEX events_kind_idx ON events (kind) WITH (timescaledb.transaction_per_chunk = 'o');",
			want:      []string{pg101OrdinaryAdvice},
		},
		{
			name:      "another storage parameter alone",
			migration: "CREATE INDEX events_kind_idx ON events (kind) WITH (fillfactor = 70);",
			want:      []string{pg101OrdinaryAdvice},
		},
		{
			name:      "the parameter's name in a predicate, not in WITH",
			migration: "CREATE INDEX events_kind_idx ON events (kind) WHERE kind <> 'timescaledb.transaction_per_chunk';",
			want:      []string{pg101OrdinaryAdvice},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeEvents(c, test.migration, nil, nil)
			c.Assert(pg101Messages(analysis.Findings()), qt.DeepEquals, test.want)
		})
	}
}

// TestPG101_AsksForTheStateOfWhatItReports: the statements PG101 asks the
// starting state for are the ones it reports, so a run without a dev database
// names the refinement it went without, and a statement it does not report
// costs no catalog read.
func TestPG101_AsksForTheStateOfWhatItReports(t *testing.T) {
	tests := []struct {
		name         string
		migration    string
		columns      []lint.BaselineColumn
		wantVersions []int64
		wantUnmet    []string
	}{
		{
			name:         "a blocking build on an earlier table, with no state",
			migration:    "CREATE INDEX events_kind_idx ON events (kind);",
			wantVersions: []int64{2},
			wantUnmet:    []string{"baseline schema that refines the statement text"},
		},
		{
			name:         "the same build, with the state supplied",
			migration:    "CREATE INDEX events_kind_idx ON events (kind);",
			columns:      eventsColumns("public"),
			wantVersions: []int64{2},
		},
		{
			name:      "a per-chunk build",
			migration: "CREATE INDEX events_kind_idx ON events (kind) WITH (timescaledb.transaction_per_chunk);",
		},
		{
			name:      "a concurrent build",
			migration: "CREATE INDEX CONCURRENTLY events_kind_idx ON events (kind);",
		},
		{
			name:      "an index on a table the same file creates",
			migration: "CREATE TABLE notes (id int, body text);\nCREATE INDEX notes_body_idx ON notes (body);",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeEvents(c, test.migration, test.columns, nil)
			c.Assert(analysis.BaselineVersions(), qt.DeepEquals, test.wantVersions)
			c.Assert(pg101Unmet(analysis), qt.DeepEquals, test.wantUnmet)
		})
	}
}
