//go:build integration

package integration_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbtarget"
)

// TestCompatMigrateDiffE2E_AClickHouseDefaultHoldingAnEscapedQuote writes a default with `\'` and
// reads what the server gives a new row. ClickHouse reads a backslash inside a literal as an
// escape. A renderer that does not quotes the literal a second time, as `”'it\”s;x”'`, and the
// server answers a syntax error at the `s`.
func TestCompatMigrateDiffE2E_AClickHouseDefaultHoldingAnEscapedQuote(t *testing.T) {
	const from = "CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id;\n"
	tests := []struct {
		name  string
		to    string
		table string
		want  string
	}{
		{
			name:  "a column added",
			to:    "CREATE TABLE t (id UInt64, v String DEFAULT 'it\\'s;x', w String DEFAULT 'y;z') ENGINE = MergeTree ORDER BY id;\n",
			table: "t",
			want:  "it's;x|y;z",
		},
		{
			name:  "a new table",
			to:    from + "CREATE TABLE u (id UInt64, v String DEFAULT 'it\\'s;x', w String DEFAULT 'y;z') ENGINE = MergeTree ORDER BY id;\n",
			table: "u",
			want:  "it's;x|y;z",
		},
		{
			name:  "a doubled quote, which the server reports with a backslash",
			to:    "CREATE TABLE t (id UInt64, v String DEFAULT 'it''s', w String DEFAULT 'y;z') ENGINE = MergeTree ORDER BY id;\n",
			table: "t",
			want:  "it's|y;z",
		},
		{
			name:  "a line break and a tab",
			to:    "CREATE TABLE t (id UInt64, v String DEFAULT 'a\\nb', w String DEFAULT 'c\\td') ENGINE = MergeTree ORDER BY id;\n",
			table: "t",
			want:  "a\nb|c\td",
		},
		{
			name:  "an escaped backslash",
			to:    "CREATE TABLE t (id UInt64, v String DEFAULT 'a\\\\b', w String DEFAULT 'y;z') ENGINE = MergeTree ORDER BY id;\n",
			table: "t",
			want:  "a\\b|y;z",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := func() string {
				return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			}
			dev, target := scratch(), scratch()
			pairs := readMigrationPairs(c, diffEngineSteps(c, "", dev, []string{from, test.to}))
			c.Assert(pairs, qt.HasLen, 2)

			applyEngineFile(c, target, platform.ClickHouse, pairs[0].Up)
			applyEngineFile(c, target, platform.ClickHouse, pairs[1].Up)
			applyEngineFile(c, target, platform.ClickHouse, fmt.Sprintf("INSERT INTO %s (id) VALUES (1);\n", test.table))

			read := fmt.Sprintf("SELECT concat(v, '|', w) FROM %s", test.table)
			c.Assert(catalogReadBack(c, target, []string{read}), qt.DeepEquals, [][]string{{test.want}})
			c.Assert(nextPlanOf(c, target, dev, test.to), qt.Contains, "synced")

			applyEngineFile(c, target, platform.ClickHouse, pairs[1].Down)

			c.Assert(nextPlanOf(c, target, dev, from), qt.Contains, "synced")
		})
	}
}
