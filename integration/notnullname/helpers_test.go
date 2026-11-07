//go:build integration

// Package notnullname_test holds the live tests of the names a plan gives a NOT NULL
// constraint on PostgreSQL 18, read back from the catalog. Each row compares two
// databases of its own, which keeps these apart from the root integration package.
package notnullname_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver these helpers open

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/dbtarget"
)

// surfaces are the commands a plan comes from.
var surfaces = []struct {
	name string
	run  func(args ...string) (string, error)
}{
	{name: "ptah", run: runPtah},
	{name: "ptah-compat", run: runCompat},
	{name: "ptah-compat strict", run: runStrictCompat},
}

// runPtah runs one native command in-process and returns its combined output.
func runPtah(args ...string) (string, error) {
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// runCompat runs one ptah-compat command in-process and returns its combined output.
func runCompat(args ...string) (string, error) {
	return runCompatPolicy(atlascompatpolicy.Full(), args...)
}

// runStrictCompat is [runCompat] under PTAH_ATLAS_STRICT_COMPAT=1. The binary resolves
// the variable in main, so in-process the command takes the policy itself.
func runStrictCompat(args ...string) (string, error) {
	return runCompatPolicy(atlascompatpolicy.StrictCE(), args...)
}

func runCompatPolicy(policy atlascompatpolicy.Policy, args ...string) (string, error) {
	cmd := atlas.NewCompatCommandWithPolicy("atlas", policy)
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// newDatabase creates a database on the engine's server, dropped when the test ends,
// runs schema on it and returns its URL. template0 keeps out what the server's
// template holds, such as the timescaledb extension.
func newDatabase(c *qt.C, engine dbtarget.Engine, schema string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, engine)
	admin, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("ptah_notnullname_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), `CREATE DATABASE "`+name+`" TEMPLATE template0`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), `DROP DATABASE "`+name+`" WITH (FORCE)`)
		c.Check(dropErr, qt.IsNil)
	})
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	database := parsed.String()
	execScript(c, database, schema)
	return database
}

// tryScript runs script on database as one simple-protocol string and returns the
// server's answer.
func tryScript(c *qt.C, database, script string) error {
	c.Helper()
	db, err := sql.Open("pgx", database)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	_, err = db.ExecContext(c.Context(), script)
	return err
}

// execScript runs script on database and expects the server to take it.
func execScript(c *qt.C, database, script string) {
	c.Helper()
	c.Assert(tryScript(c, database, script), qt.IsNil, qt.Commentf("%s", script))
}

// queryString returns the one text value query answers on database.
func queryString(c *qt.C, database, query string) string {
	c.Helper()
	db, err := sql.Open("pgx", database)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	var value string
	c.Assert(db.QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil)
	return value
}

// notNullNames reads the NOT NULL constraints of public.t by column, in column name
// order, as `column=name` pairs.
func notNullNames(c *qt.C, database string) string {
	c.Helper()
	return queryString(c, database, `
		SELECT COALESCE(string_agg(a.attname || '=' || k.conname, ',' ORDER BY a.attname), '')
		FROM pg_constraint k
		JOIN pg_attribute a ON a.attrelid = k.conrelid AND a.attnum = k.conkey[1]
		WHERE k.contype = 'n' AND k.conrelid = 'public.t'::regclass`)
}

// plan runs schema diff from left to right and returns the script.
func plan(c *qt.C, run func(args ...string) (string, error), left, right string) string {
	c.Helper()
	script, err := run("schema", "diff", "--from", left, "--to", right)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", script))
	return script
}

// assertSynced says the next plan from left to right has nothing to do.
func assertSynced(c *qt.C, run func(args ...string) (string, error), left, right string) {
	c.Helper()
	c.Assert(strings.TrimSpace(plan(c, run, left, right)), qt.Equals, "Schemas are synced, no changes to be made.")
}
