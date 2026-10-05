//go:build integration

package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlschema"
)

// The desired file is parsed without replaying it. Applying its plan twice
// proves that literal defaults and inline index settings survive readback.
func TestYDBDesiredYQL_AppliesAndSettles(t *testing.T) {
	const directory = "ptah_ydb_desired_yql"
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			schemas := []string{directory}
			dropTables(c, conn, schemas)
			c.Cleanup(func() { dropTables(c, conn, schemas) })
			desired, _, err := sqlschema.Read([]byte("CREATE TABLE `"+directory+"/items` (id Int64 NOT NULL, body Utf8 DEFAULT 'active'u, n Int64 DEFAULT 42l, PRIMARY KEY (id), INDEX by_body GLOBAL SYNC ON (body) COVER (n) WITH (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)) WITH (KEY_BLOOM_FILTER = ENABLED);"), "ydb")
			c.Assert(err, qt.IsNil)
			plan := planAgainst(c, conn, &desired, schemas)
			c.Assert(plan, qt.Not(qt.HasLen), 0)
			apply(c, conn, plan)
			c.Assert(planAgainst(c, conn, &desired, schemas), qt.HasLen, 0)
		})
	}
}
