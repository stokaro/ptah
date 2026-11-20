//go:build integration

package dbschema_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// The live half of stokaro/ptah#6331: a membership of one role in another is
// declared with the options it carries, applied, and read back from the server.
//
// Every option asserted here is read from the server's own catalog after the
// plan ran, so a plan that wrote the wrong clause, or none, fails against what
// the server holds and not against the statement the test sent. Each test
// compares the declaration with a fresh read after every step and requires an
// empty plan: a membership option that the planner writes and the reader does
// not see would be planned again on every run.

// membershipRun is one connection and the two roles a test creates for itself.
// Roles are server-wide on every engine here, so the names carry the run.
type membershipRun struct {
	c       *qt.C
	conn    *dbschema.DatabaseConnection
	group   string
	member  string
	dropSQL func(string) string
}

func openMembershipRun(c *qt.C, engine dbtarget.Engine, quote func(string) string) *membershipRun {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	suffix := time.Now().UnixNano() % 1_000_000_000
	run := &membershipRun{
		c: c, conn: conn,
		group:  fmt.Sprintf("ptah_grp_%d", suffix),
		member: fmt.Sprintf("ptah_mem_%d", suffix),
		dropSQL: func(role string) string {
			return "DROP ROLE IF EXISTS " + quote(role)
		},
	}
	c.Cleanup(func() {
		_ = conn.Writer().ExecuteSQL(c.Context(), run.dropSQL(run.member))
		_ = conn.Writer().ExecuteSQL(c.Context(), run.dropSQL(run.group))
	})
	return run
}

// declare states the two roles and the membership with the given options. A
// zero options value states none.
func (r *membershipRun) declare(options schemamodel.RoleMembershipOptions) *schemamodel.Database {
	app := schemamodel.Role{StructName: "R", Name: r.member, Inherit: true, MemberOf: []string{r.group}}
	if !options.IsZero() {
		app.MemberOptions = map[string]schemamodel.RoleMembershipOptions{r.group: options}
	}
	return &schemamodel.Database{Roles: []schemamodel.Role{{StructName: "R", Name: r.group, Inherit: true}, app}}
}

// declareNoMembership states the two roles and no membership.
func (r *membershipRun) declareNoMembership() *schemamodel.Database {
	return &schemamodel.Database{Roles: []schemamodel.Role{
		{StructName: "R", Name: r.group, Inherit: true}, {StructName: "R", Name: r.member, Inherit: true},
	}}
}

// plan compares the declaration with the server and returns the statements the
// production path writes for the difference.
func (r *membershipRun) plan(declared *schemamodel.Database) []string {
	r.c.Helper()
	runtime := must.Must(builtin.New())
	live, err := r.conn.Reader().ReadSchemaContext(r.c.Context())
	r.c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabaseInfo(r.c.Context(), declared, live, r.conn.Info(), nil, runtime)
	r.c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		r.c.Context(), runtime, diff, r.conn.Info().Dialect, planext.Options{Capabilities: r.conn.Info().Capabilities},
	)
	r.c.Assert(err, qt.IsNil)
	return statements
}

// apply plans the declaration and runs every statement of the plan.
func (r *membershipRun) apply(declared *schemamodel.Database) []string {
	r.c.Helper()
	statements := r.plan(declared)
	for _, statement := range statements {
		r.c.Assert(r.conn.Writer().ExecuteSQL(r.c.Context(), statement), qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
	return statements
}

// held reads the membership of the test's member in its group from the server.
func (r *membershipRun) held() (catalog.RoleMembership, bool) {
	r.c.Helper()
	live, err := r.conn.Reader().ReadSchemaContext(r.c.Context())
	r.c.Assert(err, qt.IsNil)
	for _, membership := range live.RoleMemberships {
		if membership.Role == r.group && membership.Member == r.member {
			return membership, true
		}
	}
	return catalog.RoleMembership{}, false
}

// onlyMembership keeps the statements that touch the membership: the plan also
// creates the two roles the first time, which this test is not about.
func onlyMembership(statements []string) string {
	var kept []string
	for _, statement := range statements {
		if strings.Contains(statement, "GRANT") || strings.Contains(statement, "REVOKE") {
			kept = append(kept, strings.TrimSuffix(strings.TrimSpace(statement), ";"))
		}
	}
	return strings.Join(kept, "\n")
}

func postgresQuote(role string) string { return `"` + role + `"` }
func backtickQuote(role string) string { return "`" + role + "`" }

func TestRoleMembershipOptionsLive_PostgreSQL16AndLater(t *testing.T) {
	for _, engine := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "postgresql 16", engine: dbtarget.PostgreSQL16},
		{name: "postgresql default line", engine: dbtarget.PostgreSQL},
	} {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := openMembershipRun(c, engine.engine, postgresQuote)

			// A membership declared with every option is granted with all
			// three, and the server holds what was declared.
			stated := schemamodel.RoleMembershipOptions{Admin: new(true), Inherit: new(false), Set: new(false)}
			created := run.apply(run.declare(stated))
			c.Assert(onlyMembership(created), qt.Equals,
				fmt.Sprintf(`GRANT "%s" TO "%s" WITH ADMIN TRUE, INHERIT FALSE, SET FALSE`, run.group, run.member))
			edge, found := run.held()
			c.Assert(found, qt.IsTrue)
			c.Assert(edge.AdminOption, qt.IsTrue)
			c.Assert(*edge.InheritOption, qt.IsFalse)
			c.Assert(*edge.SetOption, qt.IsFalse)
			c.Assert(run.plan(run.declare(stated)), qt.HasLen, 0)

			// Dropping the stated options changes the membership back to the
			// defaults in place, with one GRANT, and keeps the membership.
			reset := run.apply(run.declare(schemamodel.RoleMembershipOptions{}))
			c.Assert(onlyMembership(reset), qt.Equals,
				fmt.Sprintf(`GRANT "%s" TO "%s" WITH ADMIN FALSE, INHERIT TRUE, SET TRUE`, run.group, run.member))
			edge, found = run.held()
			c.Assert(found, qt.IsTrue)
			c.Assert(edge.AdminOption, qt.IsFalse)
			c.Assert(*edge.InheritOption, qt.IsTrue)
			c.Assert(*edge.SetOption, qt.IsTrue)

			// A declaration that states nothing leaves a membership that holds
			// the defaults alone, and so does one that states the defaults.
			c.Assert(run.plan(run.declare(schemamodel.RoleMembershipOptions{})), qt.HasLen, 0)
			defaults := schemamodel.RoleMembershipOptions{Admin: new(false), Inherit: new(true), Set: new(true)}
			c.Assert(run.plan(run.declare(defaults)), qt.HasLen, 0)

			// Only the option that differs is written.
			setOnly := run.apply(run.declare(schemamodel.RoleMembershipOptions{Set: new(false)}))
			c.Assert(onlyMembership(setOnly), qt.Equals,
				fmt.Sprintf(`GRANT "%s" TO "%s" WITH SET FALSE`, run.group, run.member))
			edge, _ = run.held()
			c.Assert(*edge.SetOption, qt.IsFalse)
			c.Assert(*edge.InheritOption, qt.IsTrue)

			// A membership the declaration drops is revoked.
			dropped := run.apply(run.declareNoMembership())
			c.Assert(onlyMembership(dropped), qt.Equals, fmt.Sprintf(`REVOKE "%s" FROM "%s"`, run.group, run.member))
			_, found = run.held()
			c.Assert(found, qt.IsFalse)
			c.Assert(run.plan(run.declareNoMembership()), qt.HasLen, 0)
		})
	}
}

func TestRoleMembershipOptionsLive_PostgreSQL13(t *testing.T) {
	c := qt.New(t)
	run := openMembershipRun(c, dbtarget.PostgreSQL13, postgresQuote)

	// The admin option is the only one this line holds, written as WITH ADMIN
	// OPTION, and the server reports no INHERIT or SET option for it.
	admin := schemamodel.RoleMembershipOptions{Admin: new(true)}
	created := run.apply(run.declare(admin))
	c.Assert(onlyMembership(created), qt.Equals,
		fmt.Sprintf(`GRANT "%s" TO "%s" WITH ADMIN OPTION`, run.group, run.member))
	edge, found := run.held()
	c.Assert(found, qt.IsTrue)
	c.Assert(edge.AdminOption, qt.IsTrue)
	c.Assert(edge.InheritOption, qt.IsNil)
	c.Assert(edge.SetOption, qt.IsNil)
	c.Assert(run.plan(run.declare(admin)), qt.HasLen, 0)

	// The option is withdrawn in place: the membership stays.
	withdrawn := run.apply(run.declare(schemamodel.RoleMembershipOptions{}))
	c.Assert(onlyMembership(withdrawn), qt.Equals,
		fmt.Sprintf(`REVOKE ADMIN OPTION FOR "%s" FROM "%s"`, run.group, run.member))
	edge, found = run.held()
	c.Assert(found, qt.IsTrue)
	c.Assert(edge.AdminOption, qt.IsFalse)
	c.Assert(run.plan(run.declare(schemamodel.RoleMembershipOptions{})), qt.HasLen, 0)

	// An option this line does not have is refused before the server is asked.
	live, err := run.conn.Reader().ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	_, err = schemadiff.CompareWithDatabaseInfo(c.Context(),
		run.declare(schemamodel.RoleMembershipOptions{Inherit: new(false)}), live, run.conn.Info(), nil, must.Must(builtin.New()))
	c.Assert(err, qt.ErrorMatches, `.*role_membership_inherit_set.*`)
}

func TestRoleMembershipOptionsLive_MySQL(t *testing.T) {
	c := qt.New(t)
	run := openMembershipRun(c, dbtarget.MySQL, backtickQuote)

	admin := schemamodel.RoleMembershipOptions{Admin: new(true)}
	created := run.apply(run.declare(admin))
	c.Assert(onlyMembership(created), qt.Equals,
		fmt.Sprintf("GRANT `%s` TO `%s` WITH ADMIN OPTION", run.group, run.member))
	edge, found := run.held()
	c.Assert(found, qt.IsTrue)
	c.Assert(edge.AdminOption, qt.IsTrue)
	c.Assert(run.plan(run.declare(admin)), qt.HasLen, 0)

	// MySQL has no statement that revokes the option alone, so the membership
	// is revoked and granted again, and the server holds it without the option.
	withdrawn := run.apply(run.declare(schemamodel.RoleMembershipOptions{}))
	c.Assert(onlyMembership(withdrawn), qt.Equals,
		fmt.Sprintf("REVOKE `%s` FROM `%s`\nGRANT `%s` TO `%s`", run.group, run.member, run.group, run.member))
	edge, found = run.held()
	c.Assert(found, qt.IsTrue)
	c.Assert(edge.AdminOption, qt.IsFalse)
	c.Assert(run.plan(run.declare(schemamodel.RoleMembershipOptions{})), qt.HasLen, 0)

	dropped := run.apply(run.declareNoMembership())
	c.Assert(onlyMembership(dropped), qt.Equals, fmt.Sprintf("REVOKE `%s` FROM `%s`", run.group, run.member))
	_, found = run.held()
	c.Assert(found, qt.IsFalse)

	// An option MySQL does not have is refused.
	live, err := run.conn.Reader().ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	_, err = schemadiff.CompareWithDatabaseInfo(c.Context(),
		run.declare(schemamodel.RoleMembershipOptions{Set: new(false)}), live, run.conn.Info(), nil, must.Must(builtin.New()))
	c.Assert(err, qt.ErrorMatches, `.*role_membership_inherit_set.*`)
}

func TestRoleMembershipOptionsLive_ClickHouse(t *testing.T) {
	c := qt.New(t)
	run := openMembershipRun(c, dbtarget.ClickHouse, backtickQuote)

	admin := schemamodel.RoleMembershipOptions{Admin: new(true)}
	created := run.apply(run.declare(admin))
	c.Assert(onlyMembership(created), qt.Equals,
		fmt.Sprintf("GRANT `%s` TO `%s` WITH ADMIN OPTION", run.group, run.member))
	edge, found := run.held()
	c.Assert(found, qt.IsTrue)
	c.Assert(edge.AdminOption, qt.IsTrue)
	c.Assert(run.plan(run.declare(admin)), qt.HasLen, 0)

	// The option is withdrawn in place: the membership stays.
	withdrawn := run.apply(run.declare(schemamodel.RoleMembershipOptions{}))
	c.Assert(onlyMembership(withdrawn), qt.Equals,
		fmt.Sprintf("REVOKE ADMIN OPTION FOR `%s` FROM `%s`", run.group, run.member))
	edge, found = run.held()
	c.Assert(found, qt.IsTrue)
	c.Assert(edge.AdminOption, qt.IsFalse)
	c.Assert(run.plan(run.declare(schemamodel.RoleMembershipOptions{})), qt.HasLen, 0)

	dropped := run.apply(run.declareNoMembership())
	c.Assert(onlyMembership(dropped), qt.Equals, fmt.Sprintf("REVOKE `%s` FROM `%s`", run.group, run.member))
	_, found = run.held()
	c.Assert(found, qt.IsFalse)
}
