//go:build integration

package ydb_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"

	"ptah.run/internal/dbtarget"
)

// coordinationE2ESchema is the directory the binary's coordination node runs
// write into.
const coordinationE2ESchema = "ptah_ydb_coordination_e2e"

// coordinationEntities declares a table and a coordination node in Go
// annotations, so the run goes through the parser, the comparison, the
// planner, the migration writer and the connection the way an operator's
// does.
const coordinationEntities = `package entities

//ptah:schema:table name="jobs" schema="ptah_ydb_coordination_e2e"
type Job struct {
	//ptah:schema:field name="id" type="BIGINT" primary
	ID int64
}

//ptah:schema:coordinationnode name="limits" schema="ptah_ydb_coordination_e2e" self_check_period="PT2S" read_consistency_mode="strict"
type Limits struct{}
`

// TestYDBBinary_GeneratesAndAppliesCoordinationNodes drives the shipped binary
// through the two ways a coordination node reaches a database. `migrations
// generate` writes Ptah's statement into the up and the down file,
// `migrations up` runs it, `schema compare` finds nothing left to change, `db
// read` describes the node, and `migrations down` drops it. Then `schema
// apply` creates it directly, and a second apply plans nothing.
func TestYDBBinary_GeneratesAndAppliesCoordinationNodes(t *testing.T) {
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			driver := coordinationDriver(c, line)
			dropCoordinationDirectory(c, conn, coordinationE2ESchema)
			c.Cleanup(func() { dropCoordinationDirectory(c, conn, coordinationE2ESchema) })
			entities := c.TempDir()
			migrations := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(entities, "nodes.go"), []byte(coordinationEntities), 0o600), qt.IsNil)
			scope := []string{"--db-url", url, "--schemas", coordinationE2ESchema}
			target := []string{"--db-url", url, "--migrations-dir", migrations, "--migrations-schema", coordinationE2ESchema}

			generated, generateErr := runBinary(ctx, binary, append([]string{"migrations", "generate", "--root-dir", entities,
				"--migrations-dir", migrations, "--name", "nodes"}, scope...)...)
			c.Assert(generateErr, qt.IsNil, qt.Commentf("generate:\n%s", generated))
			up, down := readMigrationPair(c, migrations)
			c.Assert(up, qt.Contains, "CREATE COORDINATION NODE `ptah_ydb_coordination_e2e/limits` WITH "+
				"(self_check_period = Interval('PT2S'), read_consistency_mode = 'strict');")
			c.Assert(down, qt.Contains, "DROP COORDINATION NODE `ptah_ydb_coordination_e2e/limits`;")

			applied, upErr := runBinary(ctx, binary, append([]string{"migrations", "up"}, target...)...)
			compared, compareErr := runBinary(ctx, binary, append([]string{"schema", "compare", "--root-dir", entities,
				"--exit-code"}, scope...)...)
			read, readErr := runBinary(ctx, binary, append([]string{"db", "read"}, scope...)...)
			served, servedErr := nodeConfig(c, driver, coordinationE2ESchema+"/limits")
			rolledBack, downErr := runBinary(ctx, binary,
				append([]string{"migrations", "down", "--target", "0", "--confirm"}, target...)...)
			_, goneErr := nodeConfig(c, driver, coordinationE2ESchema+"/limits")

			c.Assert(upErr, qt.IsNil, qt.Commentf("up:\n%s", applied))
			c.Assert(compareErr, qt.IsNil, qt.Commentf("schema compare:\n%s", compared))
			c.Assert(readErr, qt.IsNil, qt.Commentf("db read:\n%s", read))
			c.Assert(read, qt.Contains, "CREATE COORDINATION NODE `ptah_ydb_coordination_e2e/limits` WITH "+
				"(self_check_period = Interval('PT2S'), read_consistency_mode = 'strict');")
			c.Assert(servedErr, qt.IsNil)
			c.Assert(served, qt.DeepEquals, coordination.NodeConfig{
				SelfCheckPeriodMillis: 2000, ReadConsistencyMode: coordination.ConsistencyModeStrict,
			})
			c.Assert(downErr, qt.IsNil, qt.Commentf("down:\n%s", rolledBack))
			c.Assert(goneErr, qt.ErrorMatches, noNode)

			first, firstErr := runBinary(ctx, binary, "schema", "apply", "--db-url", url, "--root-dir", entities,
				"--schemas", coordinationE2ESchema, "--auto-approve")
			second, secondErr := runBinary(ctx, binary, "schema", "apply", "--db-url", url, "--root-dir", entities,
				"--schemas", coordinationE2ESchema, "--auto-approve")
			_, appliedErr := nodeConfig(c, driver, coordinationE2ESchema+"/limits")

			c.Assert(firstErr, qt.IsNil, qt.Commentf("first apply:\n%s", first))
			c.Assert(first, qt.Contains, "CREATE COORDINATION NODE `ptah_ydb_coordination_e2e/limits`")
			c.Assert(secondErr, qt.IsNil, qt.Commentf("second apply:\n%s", second))
			c.Assert(second, qt.Not(qt.Contains), "COORDINATION NODE")
			c.Assert(appliedErr, qt.IsNil)
		})
	}
}

// readMigrationPair reads the up files and the down files a generation wrote
// into dir.
func readMigrationPair(c *qt.C, dir string) (up, down string) {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	for _, entry := range entries {
		body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		c.Assert(err, qt.IsNil)
		switch {
		case strings.HasSuffix(entry.Name(), ".up.sql"):
			up += string(body)
		case strings.HasSuffix(entry.Name(), ".down.sql"):
			down += string(body)
		}
	}
	return up, down
}
