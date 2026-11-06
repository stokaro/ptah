//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbtarget"
)

// sqlServerKeyChangeCases are plans that change a table's constraints beside another
// constraint it holds. The name of a constraint has to be one the server answered for, or
// it may collide with any other name on the table and the plan is refused.
var sqlServerKeyChangeCases = []struct {
	name   string
	first  string
	second string
	before string
	after  string
}{
	{
		name:   "a column's key moves to another column",
		first:  "CREATE TABLE t (id int PRIMARY KEY, k int NOT NULL, u int UNIQUE);\n",
		second: "CREATE TABLE t (id int NOT NULL, k int PRIMARY KEY, u int UNIQUE);\n",
		before: "PK:id,UQ:u",
		after:  "PK:k,UQ:u",
	},
	{
		name:   "a named key widens and keeps its name",
		first:  "CREATE TABLE t (id int NOT NULL, k int NOT NULL, u int UNIQUE, CONSTRAINT t_pk PRIMARY KEY (id));\n",
		second: "CREATE TABLE t (id int NOT NULL, k int NOT NULL, u int UNIQUE, CONSTRAINT t_pk PRIMARY KEY (id, k));\n",
		before: "PK:id,UQ:u",
		after:  "PK:id,PK:k,UQ:u",
	},
	{
		name:   "a named constraint joins a table with a key",
		first:  "CREATE TABLE t (id int PRIMARY KEY, a int);\n",
		second: "CREATE TABLE t (id int PRIMARY KEY, a int, CONSTRAINT x UNIQUE (a));\n",
		before: "PK:id",
		after:  "PK:id,UQ:a",
	},
}

// TestCompatMigrateDiffE2E_ChangesAConstraintBesideAnotherOnSQLServer writes each plan
// against a dev database, runs both directions on another, reads the keys back after each
// and finds the next plan synced in each direction.
func TestCompatMigrateDiffE2E_ChangesAConstraintBesideAnotherOnSQLServer(t *testing.T) {
	const keys = "SELECT COALESCE(STRING_AGG(CONCAT(kc.type COLLATE DATABASE_DEFAULT, ':', c.name COLLATE DATABASE_DEFAULT), ',') WITHIN GROUP (ORDER BY kc.type, c.name), '') FROM sys.key_constraints kc " +
		"JOIN sys.index_columns ic ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id " +
		"JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id " +
		"WHERE kc.parent_object_id = OBJECT_ID('t')"
	for _, test := range sqlServerKeyChangeCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			newTarget := func() string {
				return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
					"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
			}
			dev := newTarget()
			work := c.TempDir()
			first := filepath.Join(work, "first.sql")
			second := filepath.Join(work, "second.sql")
			c.Assert(os.WriteFile(first, []byte(test.first), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(second, []byte(test.second), 0o600), qt.IsNil)
			synced := func(target, schema string) string {
				output, err := runCompatVerb("schema", "diff", "--from", target, "--to", "file://"+filepath.ToSlash(schema), "--dev-url", dev)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				return output
			}

			pairs := readMigrationPairs(c, diffEngineSteps(c, "", dev, []string{test.first, test.second}))
			target := newTarget()

			c.Assert(pairs, qt.HasLen, 2)
			applyEngineFile(c, target, platform.SQLServer, pairs[0].Up)
			c.Assert(heldColumns(c, target, keys), qt.Equals, test.before)
			applyEngineFile(c, target, platform.SQLServer, pairs[1].Up)
			c.Assert(heldColumns(c, target, keys), qt.Equals, test.after)
			c.Assert(synced(target, second), qt.Contains, "synced")
			applyEngineFile(c, target, platform.SQLServer, pairs[1].Down)
			c.Assert(heldColumns(c, target, keys), qt.Equals, test.before)
			c.Assert(synced(target, first), qt.Contains, "synced")
		})
	}
}
