//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A role belongs to the server. A target that already holds the role a
// planned policy names gets no CREATE ROLE in its plan, and a dev server of
// its own does not hold the role. The rehearsal creates it there, on a server
// the run owns, whose cleanup drops it with the run; without that, the policy
// fails on the dev server with `role "..." does not exist` (SQLSTATE 42704)
// where the target applies it.
//
// The dev server is one Ptah provisions from a docker URL, so it shares no
// role with the target's server.

// rehearsalTargetRoleDevURL names a database no fallback produces, so a run
// that passes can only have rehearsed on the provisioned server.
const rehearsalTargetRoleDevURL = "docker://postgres/16-alpine/ptahrolerehearse"

// rehearsalTargetRoleDesired is the Rebase shape: a DO block that creates the
// role where it is missing, and a policy for it on a table the target holds.
const rehearsalTargetRoleDesired = `DO $r$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
    CREATE ROLE {role} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION;
  END IF;
END $r$;
CREATE TABLE public.tasks (id uuid PRIMARY KEY);
ALTER TABLE public.tasks ENABLE ROW LEVEL SECURITY;
CREATE POLICY tasks_admin ON public.tasks FOR SELECT TO {role} USING (true);
`

// TestRehearsalCreatesARoleTheTargetHoldsOnAProvisionedDevServerE2E plans a
// policy for a role the target holds, which the plan therefore does not
// create, and rehearses it on a dev server that does not hold the role.
func TestRehearsalCreatesARoleTheTargetHoldsOnAProvisionedDevServerE2E(t *testing.T) {
	for _, surface := range scopedRehearsalSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			target, desired, role := newRehearsalTargetRoleFixture(c)
			env := scopedRehearsalEnv{target: target, dev: rehearsalTargetRoleDevURL, desired: desired}

			got := env.applyIn(c, surface, []string{"--include", "public.tasks"}, nil)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, fmt.Sprintf(`CREATE POLICY "tasks_admin" ON "public"."tasks" FOR SELECT TO %q`, role))
			c.Assert(got.Stdout, qt.Not(qt.Contains), "CREATE ROLE")
		})
	}
}

// newRehearsalTargetRoleFixture creates a role on the PostgreSQL test server
// and a target database there holding the table, and writes the desired file
// for the role. The role is dropped after the database.
func newRehearsalTargetRoleFixture(c *qt.C) (target, desired, role string) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	suffix := time.Now().UnixNano()
	role = fmt.Sprintf("ptah_rehearsal_admin_%d", suffix)
	_, err = admin.ExecContext(c.Context(),
		"CREATE ROLE "+quoteE2EIdent(role)+" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoteE2EIdent(role))
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	name := fmt.Sprintf("ptah_rehearsal_role_target_%d", suffix)
	createE2EDatabase(c, c.Context(), admin, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
	target = replaceDatabaseName(c, adminURL, name)
	execPostgresFamilySQL(c, c.Context(), target, "CREATE TABLE public.tasks (id uuid PRIMARY KEY);")
	desired = filepath.Join(c.TempDir(), "desired.sql")
	c.Assert(os.WriteFile(desired, []byte(strings.ReplaceAll(rehearsalTargetRoleDesired, "{role}", role)), 0o600), qt.IsNil)
	return target, desired, role
}
