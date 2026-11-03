//go:build integration

package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/ydb/ydbschema"
)

// TestYDBChangefeeds_WaitsOutAnInitialScan adds two changefeeds to a table of
// 300000 rows, the first with an initial scan.
//
// The scan holds the table's path until it has read every row, and YDB
// refuses the second ALTER TABLE in the meantime with OVERLOADED: `path ...
// has been locked by tx`. The refusal changed nothing, so the writer runs the
// statement again once the scan is done. Run once, the second changefeed is
// never added, and the plan fails halfway.
func TestYDBChangefeeds_WaitsOutAnInitialScan(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, changefeedSchemas)
			c.Cleanup(func() { dropTables(c, conn, changefeedSchemas) })
			apply(c, conn, planAgainst(c, conn, changefeedDeclaration(), changefeedSchemas))
			apply(c, conn, []string{"UPSERT INTO `" + changefeedSchema + "/events` " +
				"SELECT x AS id, CAST(CAST(x AS String) AS Utf8) AS payload " +
				"FROM (SELECT ListFromRange(0ul, 300000ul) AS x) FLATTEN LIST BY x"})
			changefeeds := []ydbschema.ChangefeedSpec{
				{Name: "audit", Mode: "NEW_AND_OLD_IMAGES", Format: "JSON", InitialScan: true},
				{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON"},
			}
			declared := changefeedDeclaration(changefeeds...)

			planned := planAgainst(c, conn, declared, changefeedSchemas)

			c.Assert(planned, qt.HasLen, 2)
			c.Assert(planned[0], qt.Contains, "INITIAL_SCAN = TRUE")
			apply(c, conn, planned)
			c.Assert(planAgainst(c, conn, declared, changefeedSchemas), qt.HasLen, 0)
		})
	}
}
