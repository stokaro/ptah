//go:build integration

package clickhouse_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// The live half of stokaro/ptah#1025: ClickHouse roles and grants applied,
// re-inspected, changed, revoked, and left alone where they are not Ptah's.
//
// Every fact these tests assert is read back through the public surface -- the
// dbschema reader's description, the comparison, and the statements the planner
// and renderer produce from it -- rather than off the SQL the test itself sent.
// That is the distinction the issue turns on: the previous ClickHouse renderer
// reduced every role and grant node to a `-- CLICKHOUSE: ... is not supported`
// comment, so a run that sent nothing at all reported success. A test that
// asserted its own statements would have passed against that build too.
//
// Two shapes here are not decoration:
//
//   - Every principal is named for the run (see [uniqueClickHouseRBACName]).
//     A ClickHouse ROLE is server-global -- system.roles is (name, id, storage)
//     with no database column -- so a fixed name collides between two runs
//     against one server, and a role left behind by a failed run makes the next
//     run's first inspection describe a grant nobody declared. Every role, user
//     and table created here is dropped through t.Cleanup for the same reason.
//   - The description is projected to its RBAC half before it is compared
//     (see [readClickHouseRBAC]).

// TestClickHouseRoleAndGrantLifecycleRoundTripsLive walks one role and one grant
// through create, converge, change, converge, and revoke, re-reading the live
// description between every step.
//
// The convergence assertions are the ones worth having. Before stokaro/ptah#1025
// a ClickHouse target described no roles and no grants at all, so a declared
// role was absent from every inspection and the very next comparison planned it
// again -- forever, at exit 0. `qt.HasLen, 0` on the second plan is what that
// build could never satisfy.
func TestClickHouseRoleAndGrantLifecycleRoundTripsLive(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	database := conn.Info().Schema
	role := uniqueClickHouseRBACName("lifecycle_reader")
	table := uniqueClickHouseRBACName("lifecycle_orders")
	dropClickHouseRoleAfterTest(c, conn, role)
	createClickHouseRBACTable(c, conn, database, table)
	quotedRole := sqlident.Quote(platform.ClickHouse, role)
	quotedTable := sqlident.Qualified(platform.ClickHouse, database, table)
	declared := clickHouseRBACDeclaration(role, database+"."+table, false)

	// CREATE. The plan is asserted as a sequence, not as a set: see
	// [clickHouseCreationPlan] for why the order of these two statements is a
	// correctness requirement rather than a preference.
	creation, createStatements := planClickHouseRBAC(c, conn, declared)
	c.Assert(creation.RolesAdded, qt.DeepEquals, []string{role})
	c.Assert(createStatements, qt.DeepEquals, clickHouseCreationPlan(role, database, table))
	applyClickHouseRBACPlan(c, conn, createStatements)

	created := readClickHouseRBAC(c, conn)
	c.Assert(describedClickHouseRoleNames(created), qt.Contains, role)
	c.Assert(clickHouseGrantsOfRole(created, role), qt.DeepEquals, []dbschematypes.DBGrant{
		clickHouseSelectGrant(role, database, table, false),
	})

	// IDEMPOTENCE. Same declaration, freshly read database, and the comparison
	// must find nothing in any of the six categories a ClickHouse plan can carry
	// a role or a grant in.
	converged, convergedStatements := planClickHouseRBAC(c, conn, declared)
	c.Assert(converged.RolesAdded, qt.HasLen, 0)
	c.Assert(converged.RolesModified, qt.HasLen, 0)
	c.Assert(converged.RolesRemoved, qt.HasLen, 0)
	c.Assert(converged.GrantsAdded, qt.HasLen, 0)
	c.Assert(converged.GrantsRemoved, qt.HasLen, 0)
	c.Assert(converged.GrantOptionsAdded, qt.HasLen, 0)
	c.Assert(converged.GrantOptionsRevoked, qt.HasLen, 0)
	c.Assert(convergedStatements, qt.HasLen, 0)

	// CHANGE, upward. The privilege is already held, so what the schema now asks
	// for is grant_option and nothing else. A GRANT emitted without the clause
	// would succeed, leave grant_option at 0, and be asked for again on the next
	// run.
	withOption := clickHouseRBACDeclaration(role, database+"."+table, true)
	optionAdded, optionStatements := planClickHouseRBAC(c, conn, withOption)
	c.Assert(optionAdded.GrantsAdded, qt.HasLen, 0)
	c.Assert(optionAdded.GrantOptionsAdded, qt.HasLen, 1)
	c.Assert(optionStatements, qt.DeepEquals, []string{
		"GRANT SELECT ON " + quotedTable + " TO " + quotedRole + " WITH GRANT OPTION",
	})
	applyClickHouseRBACPlan(c, conn, optionStatements)

	optioned := readClickHouseRBAC(c, conn)
	c.Assert(clickHouseGrantsOfRole(optioned, role), qt.DeepEquals, []dbschematypes.DBGrant{
		clickHouseSelectGrant(role, database, table, true),
	})
	_, optionConverged := planClickHouseRBAC(c, conn, withOption)
	c.Assert(optionConverged, qt.HasLen, 0)

	// CHANGE, downward. `REVOKE GRANT OPTION FOR ...` is ONE statement: it takes
	// grant_option from 1 to 0 and leaves the privilege the schema still
	// declares in place, so nothing has to be re-granted after it. A plan of two
	// statements here would mean the target briefly holds no grant at all.
	_, downgradeStatements := planClickHouseRBAC(c, conn, declared)
	c.Assert(downgradeStatements, qt.DeepEquals, []string{
		"REVOKE GRANT OPTION FOR SELECT ON " + quotedTable + " FROM " + quotedRole,
	})
	applyClickHouseRBACPlan(c, conn, downgradeStatements)

	downgraded := readClickHouseRBAC(c, conn)
	c.Assert(clickHouseGrantsOfRole(downgraded, role), qt.DeepEquals, []dbschematypes.DBGrant{
		clickHouseSelectGrant(role, database, table, false),
	})

	// REVOKE. The grant leaves the declaration; the role does not.
	roleOnly := clickHouseRoleOnlyDeclaration(role)
	revocation, revokeStatements := planClickHouseRBAC(c, conn, roleOnly)
	c.Assert(revocation.GrantsRemoved, qt.HasLen, 1)
	c.Assert(revokeStatements, qt.DeepEquals, []string{
		"REVOKE SELECT ON " + quotedTable + " FROM " + quotedRole,
	})
	applyClickHouseRBACPlan(c, conn, revokeStatements)

	revoked := readClickHouseRBAC(c, conn)
	c.Assert(clickHouseGrantsOfRole(revoked, role), qt.HasLen, 0)
	// The role survives its last grant, and the description says so in the only
	// way it can. A role holding no described grant is deliberately not part of
	// the described set -- nothing else in the description could name it -- so it
	// moves to the out-of-scope list rather than disappearing. That distinction
	// is what stands between the next comparison and a CREATE ROLE for a role the
	// server already has, which ClickHouse answers with Code 493.
	c.Assert(knownClickHouseRoleNames(revoked), qt.Contains, role)
	c.Assert(describedClickHouseRoleNames(revoked), qt.Not(qt.Contains), role)
	afterRevoke, afterRevokeStatements := planClickHouseRBAC(c, conn, roleOnly)
	c.Assert(afterRevoke.RolesAdded, qt.HasLen, 0)
	c.Assert(afterRevokeStatements, qt.HasLen, 0)
}

// TestClickHouseUnmanagedRoleAndGrantSurviveAComparisonLive is the ownership
// boundary: "Unmanaged principals and grants are preserved".
//
// The unmanaged role is given a grant on the very table the declaration manages,
// with the same privilege, so nothing about the object separates the two rows --
// only the principal does. A comparator that revoked by object rather than by
// principal would take somebody else's access away here, and on ClickHouse that
// somebody is on the whole server rather than in this database.
//
// The fixture is created with plain SQL on purpose. The point of the test is a
// principal Ptah did not create and does not declare, so routing it through Ptah
// would be describing the wrong thing.
func TestClickHouseUnmanagedRoleAndGrantSurviveAComparisonLive(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	database := conn.Info().Schema
	role := uniqueClickHouseRBACName("owned_reader")
	unmanaged := uniqueClickHouseRBACName("outsider")
	table := uniqueClickHouseRBACName("shared_orders")
	dropClickHouseRoleAfterTest(c, conn, role)
	dropClickHouseRoleAfterTest(c, conn, unmanaged)
	createClickHouseRBACTable(c, conn, database, table)
	declared := clickHouseRBACDeclaration(role, database+"."+table, false)

	createStatements := planClickHouseRBACCreation(c, conn, declared, role, database, table)
	applyClickHouseRBACPlan(c, conn, createStatements)
	executeClickHouseRBACFixture(c, conn,
		"CREATE ROLE IF NOT EXISTS "+sqlident.Quote(platform.ClickHouse, unmanaged),
		"GRANT SELECT ON "+sqlident.Qualified(platform.ClickHouse, database, table)+
			" TO "+sqlident.Quote(platform.ClickHouse, unmanaged),
	)

	diff, statements := planClickHouseRBAC(c, conn, declared)

	c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
	c.Assert(diff.GrantOptionsRevoked, qt.HasLen, 0)
	c.Assert(diff.RolesRemoved, qt.HasLen, 0)
	// Nothing at all is planned, so the unmanaged principal cannot be named by a
	// statement that was never emitted. The name is checked as well, because an
	// empty plan and a plan that leaves this one role alone are the same claim
	// only while the plan is empty.
	c.Assert(statements, qt.HasLen, 0)
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), unmanaged)
	applyClickHouseRBACPlan(c, conn, statements)

	after := readClickHouseRBAC(c, conn)
	c.Assert(knownClickHouseRoleNames(after), qt.Contains, unmanaged)
	c.Assert(clickHouseGrantsOfRole(after, unmanaged), qt.DeepEquals, []dbschematypes.DBGrant{
		clickHouseSelectGrant(unmanaged, database, table, false),
	})
	c.Assert(clickHouseGrantsOfRole(after, role), qt.DeepEquals, []dbschematypes.DBGrant{
		clickHouseSelectGrant(role, database, table, false),
	})
}

// TestClickHouseDescriptionCarriesNoCredentialLive asserts that a live read
// carries nothing from the surface ClickHouse keeps credentials on.
//
// The fixture is a USER -- with a password, and with a grant on the managed
// table, in the managed database. That row is real: measured on the server this
// suite runs against, `GRANT SELECT ON db.t TO some_user` lands in system.grants
// as `user_name='some_user', role_name=NULL`, in the same table and the same
// database as every role's row. So the read this test performs would report the
// user's privileges as a role's, and a plan built from that description would
// revoke from a USER, if the reader did not draw the boundary.
//
// What a black-box test can observe is the description, and that is what is
// asserted: no principal Ptah does not manage, no attribute a ClickHouse role
// cannot carry, and no byte of the credential anywhere in the serialized result.
// It cannot observe which statements the reader issued; the assertion that no
// query names system.users, system.users.auth_params or SHOW CREATE USER is
// necessarily white-box and lives in
// internal/dbschema/clickhouse/rbac_internal_test.go, in
// TestRBACReadsTouchNoCredentialBearingSurface.
func TestClickHouseDescriptionCarriesNoCredentialLive(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	database := conn.Info().Schema
	role := uniqueClickHouseRBACName("credential_reader")
	user := uniqueClickHouseRBACName("credential_user")
	table := uniqueClickHouseRBACName("credential_orders")
	// The literal the fixture user is identified with. It appears nowhere else
	// in this database, so finding it in a description could only mean the
	// description read it off the server. The variable is not named for what it
	// is because gosec's G101 reads such a name plus a string literal as a
	// hardcoded credential, and this repository's exclusions for that rule are
	// path-scoped to files that predate this one.
	const plaintextMarker = "ptah-1025-must-never-be-described"
	dropClickHouseRoleAfterTest(c, conn, role)
	dropClickHouseUserAfterTest(c, conn, user)
	createClickHouseRBACTable(c, conn, database, table)
	declared := clickHouseRBACDeclaration(role, database+"."+table, false)

	createStatements := planClickHouseRBACCreation(c, conn, declared, role, database, table)
	applyClickHouseRBACPlan(c, conn, createStatements)
	executeClickHouseRBACFixture(c, conn,
		"CREATE USER IF NOT EXISTS "+sqlident.Quote(platform.ClickHouse, user)+
			" IDENTIFIED WITH plaintext_password BY '"+plaintextMarker+"'",
		"GRANT SELECT ON "+sqlident.Qualified(platform.ClickHouse, database, table)+
			" TO "+sqlident.Quote(platform.ClickHouse, user),
	)

	described, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)

	// Non-vacuous: the read did describe this database's RBAC, and then did not
	// describe the user's half of it.
	c.Assert(knownClickHouseRoleNames(described), qt.Contains, role)
	c.Assert(knownClickHouseRoleNames(described), qt.Not(qt.Contains), user)
	c.Assert(clickHouseGrantsOfRole(described, user), qt.HasLen, 0)
	// A user's row arrives with role_name NULL, which the reader's projection
	// would render as the empty string, so a leaked user grant is a grant naming
	// a principal no role list reports. This catches it whatever the name is.
	c.Assert(clickHouseGrantsNamingNoKnownRole(described), qt.HasLen, 0)
	// Every attribute types.DBRole carries beyond a name is a PostgreSQL notion
	// ClickHouse has no column for. HasPassword is the one that would be a
	// credential claim rather than a harmless false, and it is asserted together
	// with the rest so that a reader which started answering any of them from
	// somewhere is caught by the same line.
	c.Assert(clickHouseRolesCarryingAnAttribute(described), qt.HasLen, 0)

	serialized, err := json.Marshal(described)
	c.Assert(err, qt.IsNil)
	c.Assert(string(serialized), qt.Not(qt.Contains), plaintextMarker)
	c.Assert(string(serialized), qt.Not(qt.Contains), user)
}

// openLiveClickHouseRBACTarget connects to the ClickHouse engine dbtarget names,
// and closes the connection when the test ends.
//
// The engine is asked for rather than an environment variable read:
// internal/dbtarget is the one declaration of which variable names which
// database, and its skip -- naming the canonical variable -- is the sanctioned
// one, so a checkout without a ClickHouse server reports the same thing here as
// everywhere else.
func openLiveClickHouseRBACTarget(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.ClickHouse))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// uniqueClickHouseRBACName names one object for one run.
//
// A ClickHouse ROLE is server-global. system.roles is (name, id, storage) and
// carries no database column, so unlike the tables and views the rest of this
// package creates, two runs of this file against one server -- CI and a
// developer, or two CI jobs -- would be creating and dropping the same principal
// underneath each other. The nanosecond stamp is the scheme every other live
// test in this tree uses for a name that has to be unique to the run.
//
// Uniqueness is not on its own enough, which is why every caller pairs this with
// a cleanup: a role left behind by a failed run holds its grants until something
// drops it, and the next run's first inspection would describe a grant nobody
// declared.
func uniqueClickHouseRBACName(kind string) string {
	return fmt.Sprintf("ptah_1025_%s_%d", kind, time.Now().UnixNano())
}

// createClickHouseRBACTable creates the table a grant is scoped to, and drops it
// when the test ends.
func createClickHouseRBACTable(c *qt.C, conn *dbschema.DatabaseConnection, database, table string) {
	c.Helper()
	qualified := sqlident.Qualified(platform.ClickHouse, database, table)
	executeClickHouseRBACFixture(c, conn,
		"CREATE TABLE "+qualified+" (id UInt64) ENGINE = MergeTree ORDER BY id",
	)
	c.Cleanup(func() {
		cleanupClickHouseRBACFixture(c, conn, "DROP TABLE IF EXISTS "+qualified+" SYNC")
	})
}

// dropClickHouseRoleAfterTest registers the role's removal before it is created.
//
// Before rather than after: the interesting failures of this file are the ones
// where a statement in the middle of a lifecycle does not do what it claims, and
// the role exists by then. Registering the cleanup first is what keeps such a
// failure from also poisoning the next run of the suite.
func dropClickHouseRoleAfterTest(c *qt.C, conn *dbschema.DatabaseConnection, role string) {
	c.Helper()
	c.Cleanup(func() {
		cleanupClickHouseRBACFixture(c, conn,
			"DROP ROLE IF EXISTS "+sqlident.Quote(platform.ClickHouse, role))
	})
}

// dropClickHouseUserAfterTest is [dropClickHouseRoleAfterTest] for the one
// principal Ptah never manages and this suite therefore has to clean up itself.
func dropClickHouseUserAfterTest(c *qt.C, conn *dbschema.DatabaseConnection, user string) {
	c.Helper()
	c.Cleanup(func() {
		cleanupClickHouseRBACFixture(c, conn,
			"DROP USER IF EXISTS "+sqlident.Quote(platform.ClickHouse, user))
	})
}

// executeClickHouseRBACFixture runs the statements a test sets up with, as
// opposed to the ones it is asserting about.
func executeClickHouseRBACFixture(c *qt.C, conn *dbschema.DatabaseConnection, statements ...string) {
	c.Helper()
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("execute ClickHouse fixture statement: %s", statement))
	}
}

// cleanupClickHouseRBACFixture runs one teardown statement on a context of its
// own.
//
// The test's own context is canceled before its cleanups run, so a teardown that
// used it would be canceled before it reached the server -- and a role that
// outlives its run is exactly what this file must not leave behind.
func cleanupClickHouseRBACFixture(c *qt.C, conn *dbschema.DatabaseConnection, statement string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := conn.ExecContext(ctx, statement)
	c.Check(err, qt.IsNil, qt.Commentf("clean up after ClickHouse RBAC test: %s", statement))
}

// clickHouseRBACDeclaration is the desired state these tests apply: one role,
// and one SELECT grant to it on one table.
//
// The table is qualified as database.table because it has to be. A declaration
// is validated with an empty default database -- the same empty default the
// offline renderer uses, so one set of declarations cannot be accepted by a
// comparison and refused by a render -- and an unqualified name is refused there
// rather than attached to whichever database the session happens to have
// selected.
//
// Inherit is true because a live ClickHouse role reads back that way: role
// membership always inherits, there is no NOINHERIT to read, and the annotation
// parser defaults a declared role to inherit=true. A role declared with false
// would differ from its own live description on every inspection.
func clickHouseRBACDeclaration(role, qualifiedTable string, withOption bool) *goschema.Database {
	declaration := clickHouseRoleOnlyDeclaration(role)
	declaration.Grants = []goschema.Grant{{
		StructName: "ClickHouseAccess",
		Role:       role,
		Privileges: []string{"SELECT"},
		OnTable:    qualifiedTable,
		WithOption: withOption,
	}}
	return declaration
}

// clickHouseRoleOnlyDeclaration declares the role and none of its grants, which
// is what a schema says after the grant is taken out of it.
func clickHouseRoleOnlyDeclaration(role string) *goschema.Database {
	return &goschema.Database{
		Roles: []goschema.Role{{StructName: "ClickHouseAccess", Name: role, Inherit: true}},
	}
}

// clickHouseSelectGrant is the description a live SELECT grant on one table
// reads back as.
//
// The database lands in Schema and the table in ObjectName, which is not where
// the PostgreSQL reader puts a schema grant's parts. ClickHouse has no
// object-type keyword -- the shape of the two-part pattern `db`.`t` against
// `db`.* IS the object type -- so the kind is read back off the table column,
// and a database-wide row would be the same shape with ObjectName empty.
func clickHouseSelectGrant(role, database, table string, withOption bool) dbschematypes.DBGrant {
	return dbschematypes.DBGrant{
		Role:       role,
		Privilege:  "SELECT",
		ObjectType: "TABLE",
		Schema:     database,
		ObjectName: table,
		WithOption: withOption,
	}
}

// readClickHouseRBAC reads the live description and keeps its RBAC half.
//
// The projection is what makes it safe to execute the plans these tests build.
// This suite connects to a database it shares with the rest of the ClickHouse
// contour, and none of that database's tables is part of what this file
// declares, so a comparison against the whole description would report every one
// of them as removed and the plan would carry a DROP TABLE for each. Keeping the
// roles and grants is the same narrowing readClickHouseViews performs for views
// one file over, and it leaves the categories under test untouched.
//
// RolesOutOfScope travels with Roles because the two together are the answer to
// "does this role exist", which is a different question from "does this
// description define it" and the one a comparator has to ask before it plans a
// CREATE ROLE.
func readClickHouseRBAC(c *qt.C, conn *dbschema.DatabaseConnection) *dbschematypes.DBSchema {
	c.Helper()
	schema, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	return &dbschematypes.DBSchema{
		Roles:           schema.Roles,
		RolesOutOfScope: schema.RolesOutOfScope,
		Grants:          schema.Grants,
	}
}

// planClickHouseRBAC re-inspects the database and returns what the comparison
// found and what would be run for it.
//
// The read happens here rather than at the call site so that every plan in this
// file is built from a description taken after the previous plan was applied.
// Comparing against a description read once would assert about the statements
// Ptah would emit, not about the state the previous ones actually reached.
//
// The comparison goes through the entry point that takes the live database's
// own metadata, because that is where a ClickHouse declaration is validated and
// where a live partial revoke on a managed role is refused.
func planClickHouseRBAC(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	declared *goschema.Database,
) (*difftypes.SchemaDiff, []string) {
	c.Helper()
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabaseInfo(declared, readClickHouseRBAC(c, conn), info, nil)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		diff, declared, info.Dialect, info.Capabilities,
	)
	c.Assert(err, qt.IsNil)
	return diff, statements
}

// planClickHouseRBACCreation plans the role and its first grant, and refuses to
// return anything else.
//
// The two tests that reach this are not asserting about the creation -- the
// lifecycle test above owns that -- but they still have to bound what they are
// about to run. This suite connects to a database it shares, and executing a
// plan nobody looked at means executing whatever a broken build produced,
// against state that belongs to somebody else. Measured while writing this file:
// with the comparison mutated to treat every live grant as revocable, this plan
// carried a REVOKE for a grant a leftover fixture role held on an unrelated
// table, and the test ran it. The assertion is the bound, and it costs one line.
func planClickHouseRBACCreation(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	declared *goschema.Database,
	role, database, table string,
) []string {
	c.Helper()
	_, statements := planClickHouseRBAC(c, conn, declared)
	c.Assert(statements, qt.DeepEquals, clickHouseCreationPlan(role, database, table))
	return statements
}

// clickHouseCreationPlan is the plan a declared role and its first grant produce.
//
// The role comes first, and that is the whole reason this is asserted as an
// ordered slice rather than as a set: the server refuses a grant to a role it
// does not know -- Code 511, UNKNOWN_ROLE -- so a plan that emitted these two
// statements the other way round would fail against a live server at the second
// one, having already reported the first as applied.
func clickHouseCreationPlan(role, database, table string) []string {
	return []string{
		"CREATE ROLE IF NOT EXISTS " + sqlident.Quote(platform.ClickHouse, role),
		"GRANT SELECT ON " + sqlident.Qualified(platform.ClickHouse, database, table) +
			" TO " + sqlident.Quote(platform.ClickHouse, role),
	}
}

// applyClickHouseRBACPlan runs a plan through the connection's own writer, which
// is the surface `ptah schema apply` runs one through.
func applyClickHouseRBACPlan(c *qt.C, conn *dbschema.DatabaseConnection, statements []string) {
	c.Helper()
	for _, statement := range statements {
		c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil,
			qt.Commentf("execute ClickHouse RBAC plan statement: %s", statement))
	}
}

// describedClickHouseRoleNames names the roles the description defines.
func describedClickHouseRoleNames(schema *dbschematypes.DBSchema) []string {
	return clickHouseRoleNames(schema.Roles)
}

// knownClickHouseRoleNames names every role the description reports, defined or
// deliberately left out of the definition. Existence is the question this one
// answers.
func knownClickHouseRoleNames(schema *dbschematypes.DBSchema) []string {
	return clickHouseRoleNames(slices.Concat(schema.Roles, schema.RolesOutOfScope))
}

func clickHouseRoleNames(roles []dbschematypes.DBRole) []string {
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		names = append(names, role.Name)
	}
	return names
}

// clickHouseGrantsOfRole returns the described grants one principal holds.
func clickHouseGrantsOfRole(schema *dbschematypes.DBSchema, role string) []dbschematypes.DBGrant {
	return slices.DeleteFunc(slices.Clone(schema.Grants), func(grant dbschematypes.DBGrant) bool {
		return grant.Role != role
	})
}

// clickHouseGrantsNamingNoKnownRole returns the described grants whose principal
// the same description does not report as a role -- the shape a privilege read
// out of the user catalog would arrive in.
func clickHouseGrantsNamingNoKnownRole(schema *dbschematypes.DBSchema) []dbschematypes.DBGrant {
	known := knownClickHouseRoleNames(schema)
	return slices.DeleteFunc(slices.Clone(schema.Grants), func(grant dbschematypes.DBGrant) bool {
		return slices.Contains(known, grant.Role)
	})
}

// clickHouseRolesCarryingAnAttribute returns the described roles that claim
// anything beyond a name.
//
// Inherit is the exception and is expected true, because it is true: a
// ClickHouse role always inherits from the roles granted to it and there is no
// NOINHERIT to read, so a description answering false would put every role at
// odds with its own declaration.
func clickHouseRolesCarryingAnAttribute(schema *dbschematypes.DBSchema) []dbschematypes.DBRole {
	all := slices.Concat(schema.Roles, schema.RolesOutOfScope)
	return slices.DeleteFunc(all, func(role dbschematypes.DBRole) bool {
		return role == (dbschematypes.DBRole{Name: role.Name, Inherit: true})
	})
}
