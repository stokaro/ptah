package lint_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// A table rebuild as Ptah's ClickHouse planner writes it: it creates the copy,
// fills it, swaps it in with EXCHANGE TABLES and drops the old table under the
// copy's name.
const (
	clickhouseItems = "CREATE TABLE items (id UInt64, n UInt32) ENGINE = MergeTree ORDER BY id;\n"

	clickhouseRebuild = "CREATE TABLE ptah_rebuild_items (id UInt64, n UInt32) ENGINE = ReplacingMergeTree ORDER BY id;\n" +
		"INSERT INTO `ptah_rebuild_items` (`id`, `n`) SELECT `id`, `n` FROM `items`;\n" +
		"EXCHANGE TABLES `items` AND `ptah_rebuild_items`;\n" +
		"DROP TABLE `ptah_rebuild_items` SYNC;\n"
)

// After the swap the copy's name holds the old table and its rows, so the drop
// that ends the rebuild is reported as the drop of a table the file did not
// create. The name deployed code reads stays, so it retires none.
func TestClickHouseRebuild_TheDropAfterTheSwapIsReported(t *testing.T) {
	tests := []struct {
		rebuildCase
		want []string
	}{
		{
			rebuildCase: rebuildCase{name: "the rebuild", dialect: "clickhouse", first: clickhouseItems, second: clickhouseRebuild},
			want:        []string{"0000000002_change.up.sql:4:destructive/drop-table"},
		},
		{
			rebuildCase: rebuildCase{name: "the tables named the other way round", dialect: "clickhouse", first: clickhouseItems,
				second: strings.Replace(clickhouseRebuild, "EXCHANGE TABLES `items` AND `ptah_rebuild_items`", "EXCHANGE TABLES ptah_rebuild_items AND items", 1)},
			want: []string{"0000000002_change.up.sql:4:destructive/drop-table"},
		},
		{
			// Two swaps put each table back under its own name, so the drop
			// is of the copy the file created.
			rebuildCase: rebuildCase{name: "a second swap undoes the first", dialect: "clickhouse", first: clickhouseItems,
				second: strings.Replace(clickhouseRebuild, "DROP TABLE", "EXCHANGE TABLES `items` AND `ptah_rebuild_items`;\nDROP TABLE", 1)},
			want: nil,
		},
		{
			// Without the swap the copy is dropped, which loses no row.
			rebuildCase: rebuildCase{name: "the copy dropped without a swap", dialect: "clickhouse", first: clickhouseItems,
				second: strings.Replace(clickhouseRebuild, "EXCHANGE TABLES `items` AND `ptah_rebuild_items`;\n", "", 1)},
			want: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(destructiveSites(c, test.rebuildCase), qt.CmpEquals(cmpopts.EquateEmpty()), test.want)
		})
	}
}
