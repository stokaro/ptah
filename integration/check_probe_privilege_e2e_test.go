//go:build integration

package integration_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// roleWithoutTemporary creates a login role on the server of adminURL with
// create, whose %s is the name, and drops it when the test ends. It is created
// before the scratch database whose grants it receives, so it is dropped after
// that database.
func roleWithoutTemporary(c *qt.C, adminURL, create string) string {
	c.Helper()
	admin := connectDevDialect(c, adminURL)
	role := "ptah_no_temporary_" + strings.ToLower(rand.Text())
	_, err := admin.ExecContext(c.Context(), fmt.Sprintf(create, role))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(dropErr, qt.IsNil)
	})
	return role
}

// asRole is databaseURL with the credentials of role and password, which may
// be empty on a server that asks for none.
func asRole(c *qt.C, databaseURL, role, password string) string {
	c.Helper()
	parsed, err := url.Parse(databaseURL)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(role, password)
	return parsed.String()
}

// databaseOf is the name of the database databaseURL names in its path.
func databaseOf(c *qt.C, databaseURL string) string {
	c.Helper()
	parsed, err := url.Parse(databaseURL)
	c.Assert(err, qt.IsNil)
	return strings.TrimPrefix(parsed.Path, "/")
}

// TestPlanWarnsWhereTheRoleMayNotCreateTheCheckProbeTableE2E plans a CHECK
// the database holds, first as the owner and then as a role without TEMPORARY
// on the database. The probe that spells the declared CHECK as the server
// does needs a temporary table, which PostgreSQL 19 and CockroachDB v26.4.1
// refuse that role with SQLSTATE 42501. The CHECK is then compared as written,
// and the plan says so and why. Without the warning a CHECK the server spells
// otherwise is planned again on every run with nothing to say why. The
// owner's plan is the control: its probe runs, and it warns about nothing.
func TestPlanWarnsWhereTheRoleMayNotCreateTheCheckProbeTableE2E(t *testing.T) {
	const warning = "warning: CHECK t_amount_check on t is compared as written, not in the form the server stores: " +
		"the probe that spells it does not run, because the role lacks a privilege the probe statement needs: "
	for _, test := range []struct {
		name     string
		engine   dbtarget.Engine
		create   string
		password string
	}{
		{
			name:     "PostgreSQL",
			engine:   dbtarget.PostgreSQL,
			create:   "CREATE ROLE %s LOGIN PASSWORD 'no_temporary_pw'",
			password: "no_temporary_pw",
		},
		// The CockroachDB server CI starts is insecure, and refuses to set a
		// password there.
		{name: "CockroachDB", engine: dbtarget.CockroachDB, create: "CREATE USER %s"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			role := roleWithoutTemporary(c, dbtarget.URL(c, test.engine), test.create)
			target := probeEscapeTargets[test.engine](c)
			execProbeEscapeSQL(c, target, []string{
				"REVOKE TEMPORARY ON DATABASE " + databaseOf(c, target.url) + " FROM public",
				"CREATE TABLE t (id INT PRIMARY KEY, amount INT NOT NULL CONSTRAINT t_amount_check CHECK (amount > 0))",
				"GRANT SELECT ON t TO " + role,
			})
			models := filepath.Join(c.TempDir(), "models")
			c.Assert(os.MkdirAll(models, 0o750), qt.IsNil)
			source := probeEscapeSource(probeEscapeTable, "ATTRIBUTES", `check="amount > 0" check_name="t_amount_check"`)
			c.Assert(os.WriteFile(filepath.Join(models, "models.go"), []byte(source), 0o600), qt.IsNil)

			owner := runPtahIn(c, "migrations", "plan", "--db-url", target.url, "--root-dir", models)
			restricted := runPtahIn(c, "migrations", "plan", "--db-url", asRole(c, target.url, role, test.password),
				"--root-dir", models)

			c.Assert(owner.ExitCode, qt.Equals, 0, historyComment(owner))
			c.Assert(owner.Stderr, qt.Not(qt.Contains), "compared as written", historyComment(owner))
			c.Assert(restricted.ExitCode, qt.Equals, 0, historyComment(restricted))
			c.Assert(restricted.Stderr, qt.Contains, warning, historyComment(restricted))
		})
	}
}
