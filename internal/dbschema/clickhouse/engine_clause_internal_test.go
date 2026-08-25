package clickhouse

// White-box testing required: the clause split is package-local and the fields
// it fills are not reachable through an exported API on their own.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// engineFullWithEveryClause is a table that declares all six, in the order the
// server emits them. Measured on ClickHouse 26.7.5.10.
const engineFullWithEveryClause = "ReplacingMergeTree(ver) PARTITION BY toYYYYMM(day) " +
	"PRIMARY KEY (day) ORDER BY (day, id) SAMPLE BY id TTL day + toIntervalDay(90) " +
	"SETTINGS index_granularity = 4096"

// TestParseEngineFull_TakesTheEngineWithItsParameters pins the engine.
//
// system.tables.engine reports the bare family name, so a ReplacingMergeTree
// keyed on a version column comes back as "ReplacingMergeTree" and replays
// without the column it deduplicates by -- silently, because the result is a
// valid ReplacingMergeTree that merges on nothing (stokaro/ptah#2198).
func TestParseEngineFull_TakesTheEngineWithItsParameters(t *testing.T) {
	tests := []struct {
		name       string
		engineFull string
		want       string
	}{
		{
			name:       "an engine with a parameter and every clause",
			engineFull: engineFullWithEveryClause,
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
		{
			// A parameter that is itself a clause word lives inside the
			// parentheses, so nesting alone already answered this one.
			name:       "a parameter spelled like a clause",
			engineFull: "ReplacingMergeTree(ttl) ORDER BY id",
			want:       "ReplacingMergeTree(ttl)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(parseEngineFull(test.engineFull).Engine, qt.Equals, test.want)
		})
	}
}

// TestParseEngineFull_SplitsEveryClause pins the whole split.
//
// TTL and SETTINGS have no column of their own in system.tables, so engine_full
// is the only place they exist; a table replayed without its TTL keeps rows it
// was configured to delete. The four key clauses are pinned beside them because
// the parse has to recognize them to find where the other two begin.
func TestParseEngineFull_SplitsEveryClause(t *testing.T) {
	c := qt.New(t)

	c.Assert(parseEngineFull(engineFullWithEveryClause), qt.Equals, engineFullClauses{
		Engine:      "ReplacingMergeTree(ver)",
		PartitionBy: "toYYYYMM(day)",
		PrimaryKey:  "(day)",
		OrderBy:     "(day, id)",
		SampleBy:    "id",
		TTL:         "day + toIntervalDay(90)",
		Settings:    "index_granularity = 4096",
	})
}

// TestParseEngineFull_AColumnNamedLikeAClauseIsAColumn is the defect this parse
// replaced a keyword search to fix.
//
// A clause keyword is a legal column name, and searching the text for one finds
// the column. Measured on ClickHouse 26.7.5.10, each of these is a table the
// server created and reported:
//
//   - `ORDER BY settings` was read as a settings clause, and the description
//     replayed `SETTINGS SETTINGS index_granularity = 8192` -- a statement no
//     server accepts;
//   - `PARTITION BY settings ORDER BY id` lost the settings clause entirely,
//     because the body found after the column began with the next keyword;
//   - `ORDER BY ttl` offered a TTL clause where the table has none, and a TTL
//     nobody declared deletes rows.
//
// What tells them apart is position: a clause keyword never stands where an
// expression is expected (stokaro/ptah#2198).
func TestParseEngineFull_AColumnNamedLikeAClauseIsAColumn(t *testing.T) {
	tests := []struct {
		name       string
		engineFull string
		want       engineFullClauses
	}{
		{
			name:       "a column named settings in the sorting key",
			engineFull: "MergeTree ORDER BY settings SETTINGS index_granularity = 8192",
			want: engineFullClauses{
				Engine:   "MergeTree",
				OrderBy:  "settings",
				Settings: "index_granularity = 8192",
			},
		},
		{
			name:       "a column named settings in the partition key",
			engineFull: "MergeTree PARTITION BY settings ORDER BY id SETTINGS index_granularity = 8192",
			want: engineFullClauses{
				Engine:      "MergeTree",
				PartitionBy: "settings",
				OrderBy:     "id",
				Settings:    "index_granularity = 8192",
			},
		},
		{
			name:       "a column named ttl in the sorting key",
			engineFull: "MergeTree ORDER BY ttl SETTINGS index_granularity = 8192",
			want: engineFullClauses{
				Engine:   "MergeTree",
				OrderBy:  "ttl",
				Settings: "index_granularity = 8192",
			},
		},
		{
			name:       "a column named settings in the sampling key",
			engineFull: "MergeTree ORDER BY (settings, id) SAMPLE BY settings SETTINGS index_granularity = 8192",
			want: engineFullClauses{
				Engine:   "MergeTree",
				OrderBy:  "(settings, id)",
				SampleBy: "settings",
				Settings: "index_granularity = 8192",
			},
		},
		{
			name:       "a column named settings inside a TTL expression",
			engineFull: "MergeTree ORDER BY id TTL d + toIntervalDay(settings) SETTINGS index_granularity = 8192",
			want: engineFullClauses{
				Engine:   "MergeTree",
				OrderBy:  "id",
				TTL:      "d + toIntervalDay(settings)",
				Settings: "index_granularity = 8192",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(parseEngineFull(test.engineFull), qt.Equals, test.want)
		})
	}
}

// TestParseEngineFull_AnswersEmptyForAClauseTheTableHasNot is the control.
//
// A table without a TTL must not gain one: the renderer emits a clause for every
// non-empty value, and a TTL nobody declared would start deleting rows.
func TestParseEngineFull_AnswersEmptyForAClauseTheTableHasNot(t *testing.T) {
	c := qt.New(t)

	withoutTTL := parseEngineFull("MergeTree ORDER BY id SETTINGS index_granularity = 8192")
	c.Assert(withoutTTL.TTL, qt.Equals, "")

	withoutSettings := parseEngineFull("MergeTree ORDER BY id")
	c.Assert(withoutSettings.Settings, qt.Equals, "")
	c.Assert(withoutSettings.OrderBy, qt.Equals, "id")
}

// TestParseEngineFull_AClauseWordInsideALiteralIsText is the literal control.
//
// A settings value is arbitrary text, and a TTL may name a volume by string.
// Reading either as a clause would cut the statement in the middle of a quote.
func TestParseEngineFull_AClauseWordInsideALiteralIsText(t *testing.T) {
	c := qt.New(t)

	clauses := parseEngineFull(
		"MergeTree ORDER BY id TTL d + toIntervalDay(1) TO VOLUME 'settings ttl' " +
			"SETTINGS index_granularity = 8192")

	c.Assert(clauses.TTL, qt.Equals, "d + toIntervalDay(1) TO VOLUME 'settings ttl'")
	c.Assert(clauses.Settings, qt.Equals, "index_granularity = 8192")
}
