//go:build integration

package ydb_test

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// gateMigrationsDir is the directory the gate test's migrations write to.
const gateMigrationsDir = "ptah_ydb_lint_gate"

// writeGateMigrations writes a directory whose second version adds a unique
// index to the table its first version created, under the lint policy given.
func writeGateMigrations(c *qt.C, policy string) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		".ptah-lint.yaml": policy,
		"0000000001_users.up.sql": "CREATE TABLE `" + gateMigrationsDir + "/users` " +
			"(id Uint64 NOT NULL, email Utf8, PRIMARY KEY (id));\n",
		"0000000001_users.down.sql":  "DROP TABLE `" + gateMigrationsDir + "/users`;\n",
		"0000000002_unique.up.sql":   "ALTER TABLE `" + gateMigrationsDir + "/users` ADD INDEX users_email GLOBAL UNIQUE SYNC ON (email);\n",
		"0000000002_unique.down.sql": "ALTER TABLE `" + gateMigrationsDir + "/users` DROP INDEX users_email;\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	return dir
}

// `ptah migrations up` lints the pending migrations of a YDB database as YQL
// before it runs any of them. A policy that gates on the YD family refuses the
// unique index the server would refuse, and nothing is applied; without the
// gate section the first version runs and the server refuses the second, in
// the words YD101 quotes.
func TestYDBBinary_MigrationsUpGatesOnTheYDFamily(t *testing.T) {
	url := dbtarget.URL(t, dbtarget.YDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	conn := openYDB(c)
	refusals := map[string]string{
		"26.2": "Adding a unique index to an existing table is disabled",
		"25.1": "Unknown index type: syncGlobalUnique",
	}
	refusal, measured := refusals[lineOf(conn.Info().Version)]
	c.Assert(measured, qt.IsTrue)
	dropDirectory(c, conn, gateMigrationsDir, "users")
	c.Cleanup(func() { dropDirectory(c, conn, gateMigrationsDir, "users") })
	binary := buildBinary(c, ctx)
	target := []string{"--db-url", url, "--migrations-schema", gateMigrationsDir}

	gated, gatedErr := runBinary(ctx, binary, append([]string{"migrations", "up", "--migrations-dir",
		writeGateMigrations(c, "dialect: ydb\ngate:\n  families: [YD]\n")}, target...)...)
	tablesAfterGate := tableNames(readScoped(c, conn, []string{gateMigrationsDir}))
	ungated, ungatedErr := runBinary(ctx, binary, append([]string{"migrations", "up", "--migrations-dir",
		writeGateMigrations(c, "dialect: ydb\n")}, target...)...)

	c.Assert(gatedErr, qt.IsNotNil)
	c.Assert(gated, qt.Matches, `(?s).*pending migrations carry lint findings the policy's gate section blocks on.*`+
		`0000000002_unique\.up\.sql:1 YD101 error: ADD INDEX users_email adds a unique index to `+gateMigrationsDir+`/users.*`)
	c.Assert(tablesAfterGate, qt.HasLen, 0)
	c.Assert(ungatedErr, qt.IsNotNil)
	c.Assert(ungated, qt.Matches, `(?s).*`+regexp.QuoteMeta(refusal)+`.*`)
	c.Assert(tableNames(readScoped(c, conn, []string{gateMigrationsDir})), qt.DeepEquals, []string{gateMigrationsDir + "|users"})
}
