//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/pgpolicyprovider"
)

// policyProbeFixture is a table whose name needs quoting, with a varchar
// column the server casts in a policy clause, and one policy on it TO
// CURRENT_USER, which the catalog records as the role that created it.
type policyProbeFixture struct {
	admin    *dbschema.DatabaseConnection
	schema   string
	creator  string
	declared schemaext.Object
	observed schemaext.Object
}

const probedTable = "Orders"

func newPolicyProbeFixture(c *qt.C) policyProbeFixture {
	c.Helper()
	admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	schema := fmt.Sprintf("ptah_policy_probe_%d", time.Now().UnixNano())
	c.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+postgresIdentifier(schema)+` CASCADE`)
		dbschema.CloseAndWarn(admin)
	})
	table := postgresIdentifier(schema) + "." + postgresIdentifier(probedTable)
	_, err = admin.ExecContext(c.Context(), fmt.Sprintf(`
		CREATE SCHEMA %s;
		CREATE TABLE %s (id integer, tenant varchar(20));
		CREATE POLICY tenant ON %s TO CURRENT_USER USING (tenant = 'x')`, postgresIdentifier(schema), table, table))
	c.Assert(err, qt.IsNil)

	var creator, stored string
	c.Assert(admin.QueryRowContext(c.Context(), `
		SELECT r.rolname, pg_get_expr(p.polqual, p.polrelid)
		FROM pg_policy p JOIN pg_roles r ON r.oid = ANY(p.polroles)
		WHERE p.polrelid = $1::regclass`, table).Scan(&creator, &stored), qt.IsNil)
	ref := pgpolicy.PolicyRef(schema, probedTable, "tenant")
	return policyProbeFixture{
		admin: admin, schema: schema, creator: creator,
		declared: must.Must(pgpolicy.DesiredPolicyObject(ref, pgpolicy.DesiredPolicy{
			Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.CurrentUser}}, Using: new("tenant = 'x'")})),
		observed: must.Must(pgpolicy.ObservedPolicyObject(ref, pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll,
			Roles: []pgpolicy.RoleSelector{{Name: creator}}, Using: &stored, Composition: pgpolicy.Permissive})),
	}
}

// probeRuntime selects the row-security owner beside the PostgreSQL target.
func probeRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	var targets []engine.Target
	for _, name := range pgpolicyprovider.Targets() {
		targets = append(targets, engine.Target{Name: name})
	}
	return must.Must(engine.New(engine.Provider{ID: "example.org/targets", Targets: targets}, pgpolicyprovider.Provider()))
}

// normalize asks the server, through session, for its spelling of the
// fixture's declaration and returns what was attached.
func (f policyProbeFixture) normalize(c *qt.C, session schemaext.ProbeSession) schemaext.Objects {
	c.Helper()
	result, err := probeRuntime(c).NormalizeObjects(c.Context(), schemaext.NormalizationRequest{
		Target: "postgres", Identifiers: identifier.ForDialect("postgres"), Session: session,
		Desired: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(f.declared)), Coverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Desired))},
		Current: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(f.observed)), Coverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Observed))},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	return result.Desired.Objects
}

func attached(c *qt.C, declared schemaext.Objects, ref schemaext.Object) *pgpolicy.NormalizedPolicy {
	c.Helper()
	object, found := must.Must2(declared.Get(ref.Ref))
	c.Assert(found, qt.IsTrue)
	return object.Value.(*pgpolicy.DesiredPolicy).Normalized
}

// changes compares desired with the fixture's observation on postgres.
func (f policyProbeFixture) changes(c *qt.C, desired schemaext.Objects) []schemaext.ChangeRecord {
	c.Helper()
	result, err := probeRuntime(c).CompareObjects(c.Context(), schemaext.ObjectComparisonRequest{
		Target: "postgres", Identifiers: identifier.ForDialect("postgres"),
		Parents: []schemaext.ParentState{{Subject: pgpolicy.Table(f.declared.Ref), Desired: true, Current: true}},
		Desired: schemaext.ObjectState{Objects: desired, Coverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Desired))},
		Current: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(f.observed)), Coverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Observed))},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Undecided, qt.HasLen, 0)
	return result.Changes
}

// TestPolicyProbe_LivePostgresMatchesTheCatalog pins what the probe exists
// for: the server's spelling of an unchanged policy -- the cast it inserts
// over a varchar column and the role CURRENT_USER resolved to -- equals the
// catalog's, so the comparison plans nothing. The control compares the same
// declaration without the probe, and it is a change.
func TestPolicyProbe_LivePostgresMatchesTheCatalog(t *testing.T) {
	c := qt.New(t)
	fixture := newPolicyProbeFixture(c)
	observed := fixture.observed.Value.(*pgpolicy.ObservedPolicy)

	declared := fixture.normalize(c, fixture.admin)

	c.Assert(attached(c, declared, fixture.declared), qt.DeepEquals,
		&pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{{Name: fixture.creator}}, Using: observed.Using})
	c.Assert(*observed.Using, qt.Not(qt.Equals), "tenant = 'x'")
	c.Assert(fixture.changes(c, declared), qt.HasLen, 0)
	c.Assert(fixture.changes(c, must.Must(schemaext.NewObjects(fixture.declared))), qt.HasLen, 1)
}

// TestPolicyProbe_LivePostgresLeavesNothingBehind pins the transaction: on a
// session pinned outside any transaction, the probe's temporary table, its
// policy and its search path are gone when it returns, and the real table
// keeps exactly its own policy.
func TestPolicyProbe_LivePostgresLeavesNothingBehind(t *testing.T) {
	c := qt.New(t)
	fixture := newPolicyProbeFixture(c)

	c.Assert(fixture.admin.WithSession(c.Context(), func(pinned *dbschema.DatabaseConnection) error {
		var before string
		c.Assert(pinned.QueryRowContext(c.Context(), `SELECT current_setting('search_path')`).Scan(&before), qt.IsNil)

		declared := fixture.normalize(c, pinned)

		c.Assert(attached(c, declared, fixture.declared), qt.IsNotNil)
		var temporary, probes int
		var after string
		c.Assert(pinned.QueryRowContext(c.Context(), `
			SELECT (SELECT count(*) FROM pg_class WHERE relname = $1 AND relpersistence = 't'),
			       (SELECT count(*) FROM pg_policy WHERE polname = 'ptah_policy_probe'),
			       current_setting('search_path')`, probedTable).Scan(&temporary, &probes, &after), qt.IsNil)
		c.Assert(temporary, qt.Equals, 0)
		c.Assert(probes, qt.Equals, 0)
		c.Assert(after, qt.Equals, before)
		return nil
	}), qt.IsNil)
	var policies string
	c.Assert(fixture.admin.QueryRowContext(c.Context(), `SELECT string_agg(polname, ',') FROM pg_policy WHERE polrelid = $1::regclass`,
		postgresIdentifier(fixture.schema)+"."+postgresIdentifier(probedTable)).Scan(&policies), qt.IsNil)
	c.Assert(policies, qt.Equals, "tenant")
}

// TestPolicyProbe_LivePostgresReadOnlyLeavesItUnanswered pins a session whose
// transactions are read-only, as on a standby: the server refuses the
// temporary table, and the probe answers nothing rather than failing the
// comparison.
func TestPolicyProbe_LivePostgresReadOnlyLeavesItUnanswered(t *testing.T) {
	c := qt.New(t)
	fixture := newPolicyProbeFixture(c)

	c.Assert(fixture.admin.WithSession(c.Context(), func(pinned *dbschema.DatabaseConnection) error {
		_, err := pinned.ExecContext(c.Context(), `SET default_transaction_read_only = on`)
		c.Assert(err, qt.IsNil)
		defer func() { _, _ = pinned.ExecContext(context.Background(), `RESET default_transaction_read_only`) }()

		declared := fixture.normalize(c, pinned)

		c.Assert(attached(c, declared, fixture.declared), qt.IsNil)
		return nil
	}), qt.IsNil)
}

// TestPolicyProbe_LivePostgresWithoutSelectLeavesItUnanswered pins a role
// without SELECT on the table: the server refuses the column copy and the probe
// answers nothing. The control grants SELECT to the same role, and the probe
// answers, so the privilege is what decided it.
func TestPolicyProbe_LivePostgresWithoutSelectLeavesItUnanswered(t *testing.T) {
	c := qt.New(t)
	fixture := newPolicyProbeFixture(c)
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	role := fmt.Sprintf("ptah_policy_probe_%d", time.Now().UnixNano())
	const password = "PtahPolicyProbe_42" // #nosec G101 -- the password of a role this test creates and drops
	_, err := fixture.admin.ExecContext(c.Context(), fmt.Sprintf(`
		CREATE ROLE %[1]s WITH LOGIN PASSWORD '%[2]s' NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
		GRANT USAGE ON SCHEMA %[3]s TO %[1]s`, postgresIdentifier(role), password, postgresIdentifier(fixture.schema)))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = fixture.admin.ExecContext(context.Background(), fmt.Sprintf(`DROP OWNED BY %[1]s; DROP ROLE IF EXISTS %[1]s`, postgresIdentifier(role)))
	})
	reader, err := dbschema.ConnectToDatabase(c.Context(), postgresURLWithCredentials(c, adminURL, role, password))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(reader) })

	refused := fixture.normalize(c, reader)

	c.Assert(attached(c, refused, fixture.declared), qt.IsNil)

	_, err = fixture.admin.ExecContext(c.Context(), fmt.Sprintf(`GRANT SELECT ON %s.%s TO %s`,
		postgresIdentifier(fixture.schema), postgresIdentifier(probedTable), postgresIdentifier(role)))
	c.Assert(err, qt.IsNil)

	answered := fixture.normalize(c, reader)

	c.Assert(attached(c, answered, fixture.declared), qt.DeepEquals,
		&pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{{Name: role}}, Using: fixture.observed.Value.(*pgpolicy.ObservedPolicy).Using})
}
