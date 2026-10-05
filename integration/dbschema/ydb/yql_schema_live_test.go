//go:build integration

package ydb_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/schemafile"
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
			source := []byte("CREATE TABLE `" + directory + "/items` (id Int64 NOT NULL, body Utf8 DEFAULT 'active'u, n Int64 DEFAULT 42l, PRIMARY KEY (id), INDEX by_body GLOBAL SYNC ON (body) COVER (n) WITH (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)) WITH (KEY_BLOOM_FILTER = ENABLED);")
			path := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(path, source, 0o600), qt.IsNil)
			desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: "ydb"})
			c.Assert(err, qt.IsNil)
			plan := planAgainst(c, conn, desired, schemas)
			c.Assert(plan, qt.Not(qt.HasLen), 0)
			apply(c, conn, plan)
			execute(c, conn, "CREATE VIEW `"+directory+"/summary` WITH (security_invoker = TRUE) AS SELECT id FROM `"+directory+"/items`")
			c.Cleanup(func() { execute(c, conn, "DROP VIEW `"+directory+"/summary`") })
			execute(c, conn, "CREATE TOPIC `"+directory+"/events`")
			c.Cleanup(func() { execute(c, conn, "DROP TOPIC `"+directory+"/events`") })
			c.Assert(planAgainst(c, conn, desired, schemas), qt.HasLen, 0)
		})
	}
}
