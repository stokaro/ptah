//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// applyWithPrepended runs `ptah schema apply --edit` of schemaFile against
// dbURL, with an editor that writes statement before the planned SQL, so the
// plan that runs carries a statement no schema declares.
func applyWithPrepended(c *qt.C, dbURL, schemaFile, statement string) clirun.Result {
	c.Helper()
	dir := c.TempDir()
	prepended := filepath.Join(dir, "prepended.sql")
	c.Assert(os.WriteFile(prepended, []byte(statement+";\n"), 0o600), qt.IsNil)
	editor := filepath.Join(dir, "prepend.sh")
	script := fmt.Sprintf("#!/bin/sh\ncat %q \"$1\" > \"$1.new\" && mv \"$1.new\" \"$1\"\n", prepended)
	c.Assert(os.WriteFile(editor, []byte(script), 0o700), qt.IsNil) // #nosec G306 -- the editor must be executable
	env := append(os.Environ(), "VISUAL="+editor, "PTAH_ALLOW_NONINTERACTIVE_EDIT=1")
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: env},
		"schema", "apply", "--db-url", dbURL, "--schema-file", schemaFile, "--edit", "--auto-approve")
}

// viewOwnerGrantHeld sets the target up for an applier that owns my_docs and
// holds ADMIN on {owner} but not SET, so a GRANT it runs can give itself SET.
var viewOwnerGrantHeld = []string{
	"ALTER VIEW my_docs OWNER TO {applier}",
	"GRANT {owner} TO {applier} WITH ADMIN TRUE, INHERIT FALSE, SET FALSE",
	"GRANT CREATE ON SCHEMA public TO {owner}",
}

// TestSchemaApplyCountsAGrantOfTheOwnerBeforeItE2E applies a plan that grants
// {owner} to the applier before it gives my_docs to {owner}, as an applier the
// server answers lacks SET on {owner} until the grant runs. A grant that
// states no SET option grants SET, as for a new membership, and so do the
// session's role and SET TRUE, so the server accepts the ALTER VIEW ... OWNER
// TO and the check must not refuse it. Read back, my_docs belongs to {owner}.
func TestSchemaApplyCountsAGrantOfTheOwnerBeforeItE2E(t *testing.T) {
	engine := viewOwnerEngines[0]
	for _, grant := range []string{"GRANT {owner} TO {applier}", "GRANT {owner} TO CURRENT_USER WITH INHERIT FALSE", "GRANT {owner} TO {applier} WITH SET TRUE"} {
		t.Run(grant, func(t *testing.T) {
			c := qt.New(t)
			applier := newViewOwnerApplier(c, engine)
			target := newViewOwnerTarget(c, engine, applier.statements(slices.Concat(viewOwnerApplierGrants, viewOwnerGrantHeld))...)

			got := applyWithPrepended(c, applier.url(c, target), target.schemaFile(c, viewOwnerBody, "ALTER VIEW my_docs OWNER TO {owner};\n"),
				target.expand(applier.expand(grant)))

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(target.owners(c), qt.DeepEquals, target.ownersWant("Doc Totals={admin}", "my_docs={owner}"))
		})
	}
}

// TestSchemaApplyRefusesAGrantTheServerDoesNotFollowE2E is the control: a
// grant WITH SET FALSE gives the applier membership in {owner} but not SET,
// which PostgreSQL 16 and later require of the new owner, so the server would
// refuse the ALTER VIEW ... OWNER TO after the GRANT ran. The check refuses
// before either is sent: my_docs keeps its owner, and the applier holds no
// grant of {owner} it made.
func TestSchemaApplyRefusesAGrantTheServerDoesNotFollowE2E(t *testing.T) {
	c := qt.New(t)
	engine := viewOwnerEngines[0]
	applier := newViewOwnerApplier(c, engine)
	target := newViewOwnerTarget(c, engine, applier.statements(slices.Concat(viewOwnerApplierGrants, viewOwnerGrantHeld))...)

	got := applyWithPrepended(c, applier.url(c, target), target.schemaFile(c, viewOwnerBody, "ALTER VIEW my_docs OWNER TO {owner};\n"),
		target.expand(applier.expand("GRANT {owner} TO {applier} WITH INHERIT TRUE, SET FALSE")))

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, target.expand(applier.expand(`ALTER VIEW "public"."my_docs" OWNER TO "{owner}" gives the view to role "{owner}", `+
		`which connected role "{applier}" may not give an object to: `+refusalSET)))
	c.Assert(got.Stderr, qt.Not(qt.Contains), "42501")
	c.Assert(target.owners(c), qt.DeepEquals, target.ownersWant("Doc Totals={admin}", applier.expand("my_docs={applier}")))
	var made int
	c.Assert(target.db.QueryRowContext(c.Context(), "SELECT count(*) FROM pg_auth_members WHERE roleid = to_regrole($1) AND grantor = to_regrole($2)",
		target.names["{owner}"], applier.name).Scan(&made), qt.IsNil)
	c.Assert(made, qt.Equals, 0)
}

// TestSchemaApplyGivesAViewToARoleThePlanCreatesE2E applies, as a superuser,
// a plan that creates a role and gives my_docs to it, in each spelling the
// server accepts and the schema parser does not read: CREATE USER, CREATE
// GROUP, and CREATE ROLE with an option Ptah does not model. The role does not
// exist when the check asks, and the statement before the ALTER VIEW ...
// OWNER TO creates it, so the check must not refuse the plan. Read back,
// my_docs belongs to the new role.
func TestSchemaApplyGivesAViewToARoleThePlanCreatesE2E(t *testing.T) {
	engine := viewOwnerEngines[0]
	for _, create := range []string{"CREATE USER {new}", "CREATE GROUP {new}", "CREATE ROLE {new} CONNECTION LIMIT 3"} {
		t.Run(create, func(t *testing.T) {
			c := qt.New(t)
			created := fmt.Sprintf("ptah_vo_new_%d", time.Now().UnixNano())
			admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbtarget.URL(c, engine.engine)))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Assert(admin.Close(), qt.IsNil) })
			// Registered before the target's, so it runs after the database
			// that holds the view the role owns is gone.
			c.Cleanup(func() {
				_, err := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+created)
				c.Assert(err, qt.IsNil)
			})
			target := newViewOwnerTarget(c, engine)
			target.names["{new}"] = created

			got := applyWithPrepended(c, target.url, target.schemaFile(c, viewOwnerBody, "ALTER VIEW my_docs OWNER TO {new};\n"), target.expand(create))

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(target.owners(c), qt.DeepEquals, target.ownersWant("Doc Totals={admin}", "my_docs={new}"))
		})
	}
}
