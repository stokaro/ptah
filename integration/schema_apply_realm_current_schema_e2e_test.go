//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A role whose search_path is the default `"$user", public` resolves to the
// schema of its own name in a database that holds one, and to public in a
// sibling database that does not. Both URLs name no schema, so both
// connections are realm-scoped and their current schemas differ: `rebase` on
// the target, `public` on the dev database. This is the layout a reporter met
// on PostgreSQL 18, with the target's schema created by the application's
// bootstrap. The role is a superuser so that the dev database's claim can read
// every catalog it reads.

// realmCurrentSchemaPassword is the password of every role the fixture
// creates.
const realmCurrentSchemaPassword = "realm-pw"

// realmCurrentSchemaFixture is a target database holding a schema named for
// its owner, and an empty dev database of the same owner, on the PostgreSQL
// test server.
type realmCurrentSchemaFixture struct {
	role      string
	targetURL string
	devURL    string
	target    *sql.DB
}

// realmNotes creates public.notes in the target and gives it one row.
const realmNotes = "CREATE TABLE public.notes (id bigint PRIMARY KEY, body text NOT NULL); INSERT INTO public.notes VALUES (1, 'kept')"

// newRealmCurrentSchemaFixture creates the role, the two databases and the
// role's schema in the target, and runs seed in the target as the role.
// Everything is dropped when the test ends.
func newRealmCurrentSchemaFixture(c *qt.C, seed ...string) realmCurrentSchemaFixture {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	admin, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = admin.Close() })

	role := fmt.Sprintf("ptah_realm_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(ctx, "CREATE ROLE "+quoteE2EIdent(role)+" LOGIN SUPERUSER PASSWORD '"+realmCurrentSchemaPassword+"'")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoteE2EIdent(role))
	})
	targetName, devName := role+"_target", role+"_dev"
	for _, name := range []string{targetName, devName} {
		_, err := admin.ExecContext(ctx, "CREATE DATABASE "+quoteE2EIdent(name)+" OWNER "+quoteE2EIdent(role))
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
	}

	fixture := realmCurrentSchemaFixture{
		role:      role,
		targetURL: realmCurrentSchemaURL(c, adminURL, role, targetName, nil),
		devURL:    realmCurrentSchemaURL(c, adminURL, role, devName, nil),
	}
	fixture.target, err = sql.Open("pgx", fixture.targetURL)
	c.Assert(err, qt.IsNil)
	// Registered after the database drops, so it runs before them.
	c.Cleanup(func() { _ = fixture.target.Close() })
	_, err = fixture.target.ExecContext(ctx, "CREATE SCHEMA "+quoteE2EIdent(role))
	c.Assert(err, qt.IsNil)
	for _, statement := range seed {
		_, err = fixture.target.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil)
	}
	var current string
	c.Assert(fixture.target.QueryRowContext(ctx, "SELECT current_schema()").Scan(&current), qt.IsNil)
	c.Assert(current, qt.Equals, role)
	return fixture
}

// realmCurrentSchemaURL is adminURL connecting as role to database, with query
// added to its parameters.
func realmCurrentSchemaURL(c *qt.C, adminURL, role, database string, query url.Values) string {
	c.Helper()
	parsed, err := url.Parse(replaceDatabaseName(c, adminURL, database))
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(role, realmCurrentSchemaPassword)
	values := parsed.Query()
	maps.Copy(values, query)
	parsed.RawQuery = values.Encode()
	return parsed.String()
}

// columns lists every column of a table named notes in the target, as
// schema.column, in order.
func (f realmCurrentSchemaFixture) columns(c *qt.C) []string {
	c.Helper()
	return realmCurrentSchemaNames(c, f.target, `
		SELECT table_schema || '.' || column_name
		FROM information_schema.columns
		WHERE table_name = 'notes'
		ORDER BY table_schema, ordinal_position`)
}

// rows lists the bodies of public.notes, in id order.
func (f realmCurrentSchemaFixture) rows(c *qt.C) []string {
	c.Helper()
	return realmCurrentSchemaNames(c, f.target, "SELECT body FROM public.notes ORDER BY id")
}

// realmCurrentSchemaRoot writes schema as schema.sql into a new directory and
// returns it.
func realmCurrentSchemaRoot(c *qt.C, schema string) string {
	c.Helper()
	root := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "schema.sql"), []byte(schema), 0o600), qt.IsNil)
	return root
}

// realmApplyArgs is the apply command line of target against targetURL, with
// the plan rehearsed on devURL unless it is empty.
func realmApplyArgs(target clirun.Target, targetURL, devURL string) []string {
	args := []string{"schema", "apply", "--schema-file", "schema.sql", "--db-url", targetURL, "--auto-approve"}
	if target == clirun.Compat {
		args = []string{"schema", "apply", "--to", "file://schema.sql", "--url", targetURL, "--auto-approve"}
	}
	if devURL != "" {
		args = append(args, "--dev-url", devURL)
	}
	return args
}

// applyArgs is [realmApplyArgs] against the fixture's target, rehearsed on its
// dev database.
func (f realmCurrentSchemaFixture) applyArgs(target clirun.Target) []string {
	return realmApplyArgs(target, f.targetURL, f.devURL)
}

// applyArgsWithoutDev is [realmApplyArgs] against the fixture's target, with
// no rehearsal.
func (f realmCurrentSchemaFixture) applyArgsWithoutDev(target clirun.Target) []string {
	return realmApplyArgs(target, f.targetURL, "")
}

// TestSchemaApplyRehearsesOnADevDatabaseOfAnotherCurrentSchema_HappyPathE2E
// applies a desired state that names its table with a schema. The plan does
// not depend on which schema an unqualified name resolves to, and the dev
// database rehearses it with the target's search path, so the apply runs on
// both surfaces. The rows read back from the target are the measure: the
// column is added to public.notes and its row is kept, and a table the target
// lacks is created in public. The trusted replay row runs the same rehearsal
// with the replayed SQL trusted, as on a shared dev server.
func TestSchemaApplyRehearsesOnADevDatabaseOfAnotherCurrentSchema_HappyPathE2E(t *testing.T) {
	const addColumn = `CREATE TABLE "public"."notes" (id bigint PRIMARY KEY, body text NOT NULL, title text);` + "\n"
	const create = `CREATE TABLE "public"."notes" (id bigint PRIMARY KEY, body text NOT NULL);` + "\n"
	for _, test := range []struct {
		name     string
		target   clirun.Target
		env      []string
		seed     []string
		schema   string
		wantCols []string
		wantRows []string
	}{
		{
			name:     "ptah-compat adds a column",
			target:   clirun.Compat,
			seed:     []string{realmNotes},
			schema:   addColumn,
			wantCols: []string{"public.id", "public.body", "public.title"},
			wantRows: []string{"kept"},
		},
		{
			name:     "ptah adds a column",
			target:   clirun.Ptah,
			seed:     []string{realmNotes},
			schema:   addColumn,
			wantCols: []string{"public.id", "public.body", "public.title"},
			wantRows: []string{"kept"},
		},
		{
			name:     "ptah-compat adds a column with the replay trusted",
			target:   clirun.Compat,
			env:      []string{"PTAH_TRUST_REPLAY_SQL=1"},
			seed:     []string{realmNotes},
			schema:   addColumn,
			wantCols: []string{"public.id", "public.body", "public.title"},
			wantRows: []string{"kept"},
		},
		{
			name:     "ptah-compat creates the table",
			target:   clirun.Compat,
			schema:   create,
			wantCols: []string{"public.id", "public.body"},
		},
		{
			name:     "ptah creates the table",
			target:   clirun.Ptah,
			schema:   create,
			wantCols: []string{"public.id", "public.body"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newRealmCurrentSchemaFixture(c, test.seed...)
			root := realmCurrentSchemaRoot(c, test.schema)
			env := append(environmentWithoutPtahVariables(), test.env...)

			result := clirun.Run(c, test.target, clirun.Options{Dir: root, Env: env}, fixture.applyArgs(test.target)...)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(fixture.columns(c), qt.DeepEquals, test.wantCols)
			c.Assert(fixture.rows(c), qt.DeepEquals, test.wantRows)
		})
	}
}

// TestSchemaApplyRefusesADevURLBoundToAnotherSchema_FailurePathE2E keeps the
// refusal where a rehearsal would resolve names elsewhere: both URLs pin a
// schema, and they pin different ones. A URL that pins a schema claims that
// schema alone, so the dev database cannot be given the target's.
func TestSchemaApplyRefusesADevURLBoundToAnotherSchema_FailurePathE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newRealmCurrentSchemaFixture(c, realmNotes)
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	targetURL := realmCurrentSchemaURL(c, adminURL, fixture.role, fixture.role+"_target", url.Values{"search_path": {fixture.role}})
	devURL := realmCurrentSchemaURL(c, adminURL, fixture.role, fixture.role+"_dev", url.Values{"search_path": {"public"}})
	root := realmCurrentSchemaRoot(c, `CREATE TABLE "public"."notes" (id bigint PRIMARY KEY, body text NOT NULL, title text);`+"\n")

	result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
		realmApplyArgs(clirun.Compat, targetURL, devURL)...)

	c.Assert(result.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	c.Assert(result.Stderr, qt.Contains, `Error: --dev-url schema scope "public" does not match --url schema scope "`+
		fixture.role+`"; point --dev-url at a dev database using the same schema`)
	c.Assert(fixture.columns(c), qt.DeepEquals, []string{"public.id", "public.body"})
}

// tables lists every table of the target outside the system schemas, as
// schema.table, in order.
func (f realmCurrentSchemaFixture) tables(c *qt.C) []string {
	c.Helper()
	return realmCurrentSchemaNames(c, f.target, `
		SELECT table_schema || '.' || table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		ORDER BY 1`)
}

// devSchemas lists the schemas of the dev database outside the system ones,
// in order.
func (f realmCurrentSchemaFixture) devSchemas(c *qt.C) []string {
	c.Helper()
	dev, err := sql.Open("pgx", f.devURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = dev.Close() }()
	return realmCurrentSchemaNames(c, dev, `
		SELECT nspname FROM pg_namespace
		WHERE nspname NOT LIKE 'pg\_%' AND nspname <> 'information_schema'
		ORDER BY 1`)
}

// realmCurrentSchemaNames runs query on db and returns its one text column, in
// the order the query gives.
func realmCurrentSchemaNames(c *qt.C, db *sql.DB, query string) []string {
	c.Helper()
	rows, err := db.QueryContext(context.Background(), query)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// TestSchemaApplyRehearsesASavedPlanOnADevDatabaseOfAnotherCurrentSchema_HappyPathE2E
// saves a plan with `schema plan` and applies it with `schema apply --plan`,
// which replays the plan on the dev database and compares the state it reaches
// with the desired one before the target is touched, and compares the target
// with it again after. The plan alters public.notes, outside the target's
// current schema, so the replay needs public.notes in its baseline; the target
// also holds items in its current schema, which the replay builds where the
// target has it. The desired state names a new table without a schema, which
// the plan creates in public, where the dev database resolves it, so both
// comparisons have to place the name there too. The target is read back, and
// the dev database is read back to show the rehearsal handed it back as it was.
func TestSchemaApplyRehearsesASavedPlanOnADevDatabaseOfAnotherCurrentSchema_HappyPathE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newRealmCurrentSchemaFixture(c, realmNotes, "CREATE TABLE items (id bigint PRIMARY KEY)")
	root := realmCurrentSchemaRoot(c, "CREATE TABLE "+quoteE2EIdent(fixture.role)+".items (id bigint PRIMARY KEY);\n"+
		"CREATE TABLE fresh (id bigint PRIMARY KEY);\n"+
		`CREATE TABLE "public"."notes" (id bigint PRIMARY KEY, body text NOT NULL, title text);`+"\n")
	options := clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()}

	planned := clirun.Run(c, clirun.Compat, options, "schema", "plan", "--from", fixture.targetURL,
		"--to", "file://schema.sql", "--dev-url", fixture.devURL, "--output", "realm.plan.hcl")
	c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
	applied := clirun.Run(c, clirun.Compat, options, "schema", "apply", "--url", fixture.targetURL,
		"--to", "file://schema.sql", "--dev-url", fixture.devURL, "--plan", "file://realm.plan.hcl", "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(fixture.columns(c), qt.DeepEquals, []string{"public.id", "public.body", "public.title"})
	c.Assert(fixture.rows(c), qt.DeepEquals, []string{"kept"})
	c.Assert(fixture.tables(c), qt.DeepEquals, []string{fixture.role + ".items", "public.fresh", "public.notes"})
	c.Assert(fixture.devSchemas(c), qt.DeepEquals, []string{"public"})
}
