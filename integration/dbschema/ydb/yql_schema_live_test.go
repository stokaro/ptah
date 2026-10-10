//go:build integration

package ydb_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/internal/builtintest"
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
			desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: "ydb"})
			c.Assert(err, qt.IsNil)
			plan := planAgainst(c, conn, desired, schemas)
			c.Assert(plan, qt.Not(qt.HasLen), 0)
			apply(c, conn, plan)

			c.Assert(planAgainst(c, conn, desired, schemas), qt.HasLen, 0)
		})
	}
}

// A file describes both the TTL and the family assignment. Removing only TTL
// must reset it while leaving the table and its family settings unchanged.
func TestYDBDesiredYQL_TTLAndColumnFamilies(t *testing.T) {
	const directory = "ptah_ydb_yql_clauses"
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			schemas := []string{directory}
			dropTables(c, conn, schemas)
			c.Cleanup(func() { dropTables(c, conn, schemas) })
			path := filepath.Join(c.TempDir(), "schema.sql")
			table := "CREATE TABLE `" + directory + "/events` (id Int64 NOT NULL, ts Timestamp, body Utf8 FAMILY payload, PRIMARY KEY (id), FAMILY payload (COMPRESSION = 'lz4'))"
			for _, suffix := range []string{" WITH (TTL = Interval('PT1H') ON ts);", ";"} {
				c.Assert(os.WriteFile(path, []byte(table+suffix), 0o600), qt.IsNil)
				desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: "ydb"})
				c.Assert(err, qt.IsNil)
				statements := planAgainst(c, conn, desired, schemas)
				c.Assert(statements, qt.Not(qt.HasLen), 0)
				apply(c, conn, statements)
				c.Assert(planAgainst(c, conn, desired, schemas), qt.HasLen, 0)
			}
		})
	}
}

// File loading must carry declared objects through create, update, and removal,
// while a coordination node declared beside them stays unchanged.
func TestYDBDesiredYQL_ViewsAndTopics(t *testing.T) {
	const table = "CREATE TABLE items (id Int64 NOT NULL, PRIMARY KEY (id)); CREATE COORDINATION NODE locks;"
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connect(c, enterRealm(c, line))
			path := filepath.Join(c.TempDir(), "schema.sql")
			for _, source := range []string{
				table + "CREATE VIEW summary WITH (security_invoker = TRUE) AS SELECT id FROM items; CREATE TOPIC events (CONSUMER worker WITH (important = TRUE)) WITH (retention_period = Interval('P1D'));",
				table + "CREATE VIEW summary WITH (security_invoker = TRUE) AS SELECT id FROM items WHERE id > 0; CREATE TOPIC events (CONSUMER audit WITH (read_from = Timestamp('2026-01-01T00:00:00Z'))) WITH (retention_period = Interval('P2D'));",
				table,
			} {
				c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)
				desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: "ydb"})
				c.Assert(err, qt.IsNil)
				statements := planAgainst(c, conn, desired, nil)
				c.Assert(statements, qt.Not(qt.HasLen), 0)
				apply(c, conn, statements)
				c.Assert(planAgainst(c, conn, desired, nil), qt.HasLen, 0)
			}
			live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, nil)
			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(live.FeatureObjects.All()), qt.DeepEquals, []schemaext.Object{
				ydbcoordination.ObservedObject("", "locks", ydbcoordination.Spec{}),
			})
			c.Assert(live.Views, qt.HasLen, 0)
			c.Assert(liveTopics(c, live), qt.HasLen, 0)
		})
	}
}
