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

// sqlServerUnnamedUniqueCases are schema files whose UNIQUE on the table has no name. The
// server would name one with a hash that differs by database, and a plan needs a name to
// carry through a rollback.
var sqlServerUnnamedUniqueCases = []struct {
	name   string
	first  string
	second string
	before string
	after  string
}{
	{
		name:   "created over two columns and dropped",
		first:  "CREATE TABLE t (id int PRIMARY KEY, v int, w int, UNIQUE (v, w));\n",
		second: "CREATE TABLE t (id int PRIMARY KEY, v int, w int);\n",
		before: "t_v_w_key",
		after:  "",
	},
	{
		name:   "added to a table that stands",
		first:  "CREATE TABLE t (id int PRIMARY KEY, v int, w int);\n",
		second: "CREATE TABLE t (id int PRIMARY KEY, v int, w int, UNIQUE (v, w));\n",
		before: "",
		after:  "t_v_w_key",
	},
	{
		name:   "beside a plan about something else",
		first:  "CREATE TABLE t (id int PRIMARY KEY, v int, w int, UNIQUE (v, w));\n",
		second: "CREATE TABLE t (id int PRIMARY KEY, v int, w int, UNIQUE (v, w));\nCREATE TABLE other (id int PRIMARY KEY);\n",
		before: "t_v_w_key",
		after:  "t_v_w_key",
	},
	{
		name:   "beside a constraint that holds the name",
		first:  "CREATE TABLE t (id int PRIMARY KEY, v int, w int, CONSTRAINT t_v_w_key UNIQUE (w), UNIQUE (v, w));\n",
		second: "CREATE TABLE t (id int PRIMARY KEY, v int, w int, CONSTRAINT t_v_w_key UNIQUE (w), UNIQUE (v, w));\nCREATE TABLE other (id int PRIMARY KEY);\n",
		before: "t_v_w_key,t_v_w_key1",
		after:  "t_v_w_key,t_v_w_key1",
	},
}

// sqlServerUnnamedCheckCases are schema files whose CHECK on the table has no name. Left to
// the server, the name differs on the next replay, so a second file adds the check again
// and drops one that is gone: a third stops at `is not a constraint`.
var sqlServerUnnamedCheckCases = []struct {
	name   string
	first  string
	second string
	before string
	after  string
}{
	{
		name:   "kept beside a plan about something else",
		first:  "CREATE TABLE t (id int PRIMARY KEY, w int, CHECK (w > 0));\n",
		second: "CREATE TABLE t (id int PRIMARY KEY, w int, CHECK (w > 0));\nCREATE TABLE other (id int PRIMARY KEY);\n",
		before: "t_w_check",
		after:  "t_w_check",
	},
	{
		name:   "dropped",
		first:  "CREATE TABLE t (id int PRIMARY KEY, w int, CHECK (w > 0));\n",
		second: "CREATE TABLE t (id int PRIMARY KEY, w int);\n",
		before: "t_w_check",
		after:  "",
	},
	{
		name:   "added to a table that stands",
		first:  "CREATE TABLE t (id int PRIMARY KEY, w int);\n",
		second: "CREATE TABLE t (id int PRIMARY KEY, w int, CHECK (w > 0));\n",
		before: "",
		after:  "t_w_check",
	},
}

// TestCompatMigrateDiffE2E_AnUnnamedCheckOnSQLServer writes each file against a dev
// database, runs both directions on another, reads the names back after each, and finds
// the next plan synced.
func TestCompatMigrateDiffE2E_AnUnnamedCheckOnSQLServer(t *testing.T) {
	const checks = "SELECT COALESCE(STRING_AGG(name, ',') WITHIN GROUP (ORDER BY name), '') FROM sys.check_constraints"
	for _, test := range sqlServerUnnamedCheckCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			newTarget := func() string {
				return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
					"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
			}
			dev := newTarget()
			second := filepath.Join(c.TempDir(), "second.sql")
			c.Assert(os.WriteFile(second, []byte(test.second), 0o600), qt.IsNil)

			pairs := readMigrationPairs(c, diffEngineSteps(c, "", dev, []string{test.first, test.second}))
			target := newTarget()

			c.Assert(pairs, qt.HasLen, 2)
			applyEngineFile(c, target, platform.SQLServer, pairs[0].Up)
			c.Assert(heldColumns(c, target, checks), qt.Equals, test.before)
			applyEngineFile(c, target, platform.SQLServer, pairs[1].Up)
			c.Assert(heldColumns(c, target, checks), qt.Equals, test.after)
			next, err := runCompatVerb("schema", "diff", "--from", target, "--to", "file://"+filepath.ToSlash(second), "--dev-url", dev)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", next))
			c.Assert(next, qt.Contains, "synced")
			applyEngineFile(c, target, platform.SQLServer, pairs[1].Down)
			c.Assert(heldColumns(c, target, checks), qt.Equals, test.before)
		})
	}
}

// TestCompatMigrateDiffE2E_AnUnnamedUniqueOnSQLServer writes each file against a dev
// database, runs both directions on another, reads the keys' names back after each, and
// finds the next plan synced. The names are the file's, so they read the same on any
// database.
func TestCompatMigrateDiffE2E_AnUnnamedUniqueOnSQLServer(t *testing.T) {
	const keys = "SELECT COALESCE(STRING_AGG(name, ',') WITHIN GROUP (ORDER BY name), '') FROM sys.key_constraints WHERE type = 'UQ'"
	for _, test := range sqlServerUnnamedUniqueCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			newTarget := func() string {
				return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
					"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
			}
			dev := newTarget()
			second := filepath.Join(c.TempDir(), "second.sql")
			c.Assert(os.WriteFile(second, []byte(test.second), 0o600), qt.IsNil)

			pairs := readMigrationPairs(c, diffEngineSteps(c, "", dev, []string{test.first, test.second}))
			target := newTarget()

			c.Assert(pairs, qt.HasLen, 2)
			applyEngineFile(c, target, platform.SQLServer, pairs[0].Up)
			c.Assert(heldColumns(c, target, keys), qt.Equals, test.before)
			applyEngineFile(c, target, platform.SQLServer, pairs[1].Up)
			c.Assert(heldColumns(c, target, keys), qt.Equals, test.after)
			next, err := runCompatVerb("schema", "diff", "--from", target, "--to", "file://"+filepath.ToSlash(second), "--dev-url", dev)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", next))
			c.Assert(next, qt.Contains, "synced")
			applyEngineFile(c, target, platform.SQLServer, pairs[1].Down)
			c.Assert(heldColumns(c, target, keys), qt.Equals, test.before)
		})
	}
}
