package clickhouse

// White-box testing required: the clause split is package-local and the fields
// it fills are not reachable through an exported API on their own.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestEngineClause_TakesTheEngineWithItsParameters pins the engine.
//
// system.tables.engine reports the bare family name, so a ReplacingMergeTree
// keyed on a version column comes back as "ReplacingMergeTree" and replays
// without the column it deduplicates by -- silently, because the result is a
// valid ReplacingMergeTree that merges on nothing (stokaro/ptah#2198).
func TestEngineClause_TakesTheEngineWithItsParameters(t *testing.T) {
	tests := []struct {
		name       string
		engineFull string
		want       string
	}{
		{
			name:       "an engine with a parameter and every clause",
			engineFull: "ReplacingMergeTree(ver) PARTITION BY toYYYYMM(day) ORDER BY (day, id) SAMPLE BY id TTL day + toIntervalDay(90) SETTINGS index_granularity = 4096",
			want:       "ReplacingMergeTree(ver)",
		},
		{
			name:       "a bare engine",
			engineFull: "MergeTree ORDER BY id SETTINGS index_granularity = 8192",
			want:       "MergeTree",
		},
		{
			// The control: an engine with nothing after it must not be cut.
			name:       "an engine with no clauses at all",
			engineFull: "Memory",
			want:       "Memory",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(engineClause(test.engineFull), qt.Equals, test.want)
		})
	}
}

// TestEngineFullClause_TakesOneClauseBody pins the TTL and SETTINGS split.
//
// Neither has a column of its own in system.tables, so engine_full is the only
// place they exist. A table replayed without its TTL keeps rows it was
// configured to delete.
func TestEngineFullClause_TakesOneClauseBody(t *testing.T) {
	const full = "ReplacingMergeTree(ver) PARTITION BY toYYYYMM(day) ORDER BY (day, id) SAMPLE BY id TTL day + toIntervalDay(90) SETTINGS index_granularity = 4096"

	tests := []struct {
		name    string
		keyword string
		want    string
	}{
		{name: "the TTL", keyword: "TTL", want: "day + toIntervalDay(90)"},
		{name: "the settings", keyword: "SETTINGS", want: "index_granularity = 4096"},
		{name: "the partition key", keyword: "PARTITION BY", want: "toYYYYMM(day)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(engineFullClause(full, test.keyword), qt.Equals, test.want)
		})
	}
}

// TestEngineFullClause_AnswersEmptyForAClauseTheTableHasNot is the control.
//
// A table without a TTL must not gain one: the renderer emits a clause for every
// non-empty value, and a TTL nobody declared would start deleting rows.
func TestEngineFullClause_AnswersEmptyForAClauseTheTableHasNot(t *testing.T) {
	c := qt.New(t)

	c.Assert(engineFullClause("MergeTree ORDER BY id SETTINGS index_granularity = 8192", "TTL"), qt.Equals, "")
	c.Assert(engineFullClause("MergeTree ORDER BY id", "SETTINGS"), qt.Equals, "")
}
