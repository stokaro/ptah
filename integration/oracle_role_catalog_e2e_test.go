//go:build integration

package integration_test

import (
	"context"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/sijms/go-ora/v3" // registers the Oracle driver for database/sql

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestOracleRoleCatalogIsNotDescribedWithoutPrivilegeE2E pins the answer an
// ordinary Oracle account gets, which is the answer most accounts get.
//
// Oracle has no ALL_ROLES. DBA_ROLES is the only view naming the server's
// roles, and it needs SELECT_CATALOG_ROLE; without it the server answers
// ORA-00942 -- table or view does not exist -- because an invisible view is
// reported as an absent one. So "this account may not look" and "this server
// has no roles" arrive as the same failure, and telling them apart is the
// whole point of this test.
//
// The distinction is not academic. Reported as "no roles", a declared role is
// planned as a CREATE ROLE, which the same account answers ORA-01031 to: a
// plan that cannot apply, regenerated on every run. Recorded as not described,
// the comparator declines to conclude anything and reports the role as an
// undecided addition instead.
//
// The account is created here rather than borrowed from ORACLE_TEST_URL, so
// that what it may do is stated by this test rather than inherited from the
// suite's configuration -- and so that a later change granting the shared
// account more privileges cannot quietly delete this coverage.
func TestOracleRoleCatalogIsNotDescribedWithoutPrivilegeE2E(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.OracleAdmin)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)

	const account = "PTAH_RC_PLAIN"
	dropOracleUser(ctx, c, admin, account)
	createOracleUser(ctx, c, admin, account)
	defer dropOracleUser(context.WithoutCancel(ctx), c, admin, account)

	// A role that exists on the server while this account reads. Its absence
	// from the description below is what makes the coverage record load
	// bearing: the description is not empty because the server is.
	const role = "PTAH_RC_UNSEEN"
	dropOracleRole(ctx, c, admin, role)
	execOracle(ctx, c, admin, "CREATE ROLE "+role)
	defer dropOracleRole(context.WithoutCancel(ctx), c, admin, role)

	conn, err := dbschema.ConnectToDatabase(ctx, oracleURLAs(c, adminURL, account))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	read, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	// Three assertions, and the third is the one a wrong reader passes the
	// first two without.
	c.Assert(read.Roles, qt.HasLen, 0)
	c.Assert(read.Grants, qt.HasLen, 0)
	c.Assert(read.NotDescribed.Describes(coverage.Role, role), qt.IsFalse)

	// And the consequence, at the seam that decides what happens next: a
	// declaration naming the role plans nothing, rather than planning a
	// CREATE ROLE this account cannot execute.
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, oracleRoleDeclaration(role, "", nil), read, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.RolesAdded, qt.HasLen, 0)
	c.Assert(diff.RolesRemoved, qt.HasLen, 0)
}

// TestOracleRolesAndGrantsAreReadWithPrivilegeE2E is the other half: the same
// reader, on an account that may see DBA_ROLES, describing what is there.
//
// The round trip is the assertion. A reader can report a role and still hand
// the comparator something it plans a change against -- a name in the wrong
// case, an object type spelled the catalog's way rather than the declaration's
// -- and only the comparison says whether there is anything left to do. The
// VIEW row is in the declaration for exactly that reason: ALL_TAB_PRIVS calls
// its type VIEW, schemamodel.Grant spells every relation target OnTable, and a
// reader passing the type through unmapped converges on the table and not on
// the view.
func TestOracleRolesAndGrantsAreReadWithPrivilegeE2E(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.OracleAdmin)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)

	const account = "PTAH_RC_CATALOG"
	const role = "PTAH_RC_READER"
	dropOracleUser(ctx, c, admin, account)
	dropOracleRole(ctx, c, admin, role)
	createOracleUser(ctx, c, admin, account)
	defer dropOracleUser(context.WithoutCancel(ctx), c, admin, account)
	execOracle(ctx, c, admin, "GRANT SELECT_CATALOG_ROLE TO "+account)
	execOracle(ctx, c, admin, "CREATE ROLE "+role)
	defer dropOracleRole(context.WithoutCancel(ctx), c, admin, role)

	conn, err := dbschema.ConnectToDatabase(ctx, oracleURLAs(c, adminURL, account))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	execOracle(ctx, c, conn, "CREATE TABLE rc_docs (id NUMBER(10) NOT NULL, title VARCHAR2(200) NOT NULL, CONSTRAINT pk_rc_docs PRIMARY KEY (id))")
	execOracle(ctx, c, conn, "CREATE VIEW rc_titles AS SELECT title FROM rc_docs")
	execOracle(ctx, c, conn, "GRANT SELECT, INSERT ON rc_docs TO "+role)
	execOracle(ctx, c, conn, "GRANT SELECT ON rc_titles TO "+role)

	read, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	c.Assert(read.NotDescribed.Describes(coverage.Role, role), qt.IsTrue)
	c.Assert(oracleRoleNames(read.Roles), qt.Contains, role)

	// Inherit is the one attribute an Oracle role does not report false for:
	// a grantee holds the role's privileges while the role is enabled, and
	// there is no NOINHERIT to read. HasPassword is false because this role is
	// not IDENTIFIED BY one -- DBA_ROLES.AUTHENTICATION_TYPE says NONE.
	described := oracleRoleByName(c, read.Roles, role)
	c.Assert(described.Inherit, qt.IsTrue)
	c.Assert(described.HasPassword, qt.IsFalse)
	c.Assert(described.Login, qt.IsFalse)
	c.Assert(described.Superuser, qt.IsFalse)

	c.Assert(oracleGrantSummary(read.Grants, role), qt.DeepEquals, []string{
		"TABLE " + account + ".RC_DOCS INSERT",
		"TABLE " + account + ".RC_DOCS SELECT",
		"TABLE " + account + ".RC_TITLES SELECT",
	})

	// The round trip. The declaration names the same role and the same four
	// privileges, and the comparison reports nothing about either.
	declared := oracleRoleDeclaration(role, account, []oracleDeclaredGrant{
		{table: "rc_docs", privileges: []string{"SELECT", "INSERT"}},
		{table: "rc_titles", privileges: []string{"SELECT"}},
	})
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, read, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleRoleDiffSummary(diff), qt.DeepEquals, []string(nil))
}

// oracleDeclaredGrant is one table the declared role holds privileges on.
type oracleDeclaredGrant struct {
	table      string
	privileges []string
}

// oracleRoleDeclaration builds the desired state both tests compare against.
//
// The role carries no attributes, because an Oracle role has none to carry:
// the renderer refuses a declaration asking one for LOGIN or a password, and
// the reader reports neither. An empty schema means the grants are left out
// entirely, which is what the unprivileged half wants.
func oracleRoleDeclaration(role, schema string, grants []oracleDeclaredGrant) *schemamodel.Database {
	declared := &schemamodel.Database{
		Roles: []schemamodel.Role{{Name: role, Inherit: true}},
	}
	for _, grant := range grants {
		declared.Grants = append(declared.Grants, schemamodel.Grant{
			Role:       role,
			Privileges: grant.privileges,
			OnTable:    schema + "." + grant.table,
		})
	}
	return declared
}

// oracleRoleDiffSummary names every role or grant change a comparison
// reported, so a failure says which one rather than that a count was not zero.
func oracleRoleDiffSummary(diff *difftypes.SchemaDiff) []string {
	var changes []string
	for _, role := range diff.RolesAdded {
		changes = append(changes, "role added: "+role.Name)
	}
	for _, role := range diff.RolesRemoved {
		changes = append(changes, "role removed: "+role.Name)
	}
	for _, role := range diff.RolesModified {
		changes = append(changes, "role modified: "+role.RoleName)
	}
	for _, grant := range diff.GrantsAdded {
		changes = append(changes, "grant added: "+grant.Role+" "+grant.Privilege+" on "+grant.ObjectName)
	}
	for _, grant := range diff.GrantsRemoved {
		changes = append(changes, "grant removed: "+grant.Role+" "+grant.Privilege+" on "+grant.ObjectName)
	}
	return changes
}

// oracleGrantSummary renders one role's grants in a stable order.
func oracleGrantSummary(grants []catalog.Grant, role string) []string {
	var summary []string
	for _, grant := range grants {
		summary = appendOracleGrant(summary, grant, role)
	}
	slices.Sort(summary)
	return summary
}

func appendOracleGrant(summary []string, grant catalog.Grant, role string) []string {
	if grant.Role != role {
		return summary
	}
	return append(summary, grant.ObjectType+" "+grant.QualifiedTarget()+" "+grant.Privilege)
}

func oracleRoleNames(roles []catalog.Role) []string {
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		names = append(names, role.Name)
	}
	return names
}

func oracleRoleByName(c *qt.C, roles []catalog.Role, name string) catalog.Role {
	c.Helper()
	for _, role := range roles {
		if role.Name == name {
			return role
		}
	}
	c.Fatalf("role %q is absent from the read schema", name)
	return catalog.Role{}
}

// oracleURLAs re-points an Oracle URL at another account.
//
// The password is the account name, which createOracleUser also uses: these
// accounts exist for the length of one test on a container that exists for the
// length of one CI job, and a password nobody has to thread through the
// workflow is one fewer thing to keep in step.
func oracleURLAs(c *qt.C, address, account string) string {
	c.Helper()
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(account, account)
	return parsed.String()
}

// createOracleUser makes an account that may connect and own tables, and
// nothing else.
func createOracleUser(ctx context.Context, c *qt.C, admin *dbschema.DatabaseConnection, account string) {
	c.Helper()
	execOracle(ctx, c, admin, "CREATE USER "+account+" IDENTIFIED BY "+account)
	execOracle(ctx, c, admin, "GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW TO "+account)
	execOracle(ctx, c, admin, "ALTER USER "+account+" QUOTA UNLIMITED ON USERS")
}

// dropOracleUser removes the account and everything it owns, tolerating an
// account that is not there.
//
// Oracle has no IF EXISTS on either statement -- DROP USER answers ORA-01918
// and DROP ROLE answers ORA-01919 -- so the tolerance is here rather than in
// the SQL. It runs before the CREATE as well as after it, because a run killed
// between the two leaves the account behind, and the next run's CREATE would
// answer ORA-01920 against a defect that is not in the code.
func dropOracleUser(ctx context.Context, c *qt.C, admin *dbschema.DatabaseConnection, account string) {
	c.Helper()
	_ = admin.SchemaWriter().ExecuteSQL(ctx, "DROP USER "+account+" CASCADE")
}

func dropOracleRole(ctx context.Context, c *qt.C, admin *dbschema.DatabaseConnection, role string) {
	c.Helper()
	_ = admin.SchemaWriter().ExecuteSQL(ctx, "DROP ROLE "+role)
}

func execOracle(ctx context.Context, c *qt.C, conn *dbschema.DatabaseConnection, statement string) {
	c.Helper()
	c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, statement), qt.IsNil,
		qt.Commentf("statement: %s", statement))
}

// TestOracleRoleManagementPlansAndConvergesE2E is the assertion the
// role_management capability key could not be flipped without.
//
// The key promises four things at once -- Ptah plans a role and a grant,
// renders them, reads them back, and finds nothing left to do -- and the
// failure when one is missing is not a compile error. It is a plan that
// reports the same pending change forever, because the reader never sees what
// the renderer made, or a plan that emits nothing at all because a capability
// gate silently dropped it (stokaro/ptah#1920).
//
// The whole loop runs through the PLANNER rather than the renderer, which is
// the half a round trip over rendered statements cannot reach: planRoles and
// planGrants are gated on the key, so a preset that still read false would
// produce an empty plan here and the convergence assertion at the end would
// pass for the wrong reason. The count is asserted before anything is applied.
func TestOracleRoleManagementPlansAndConvergesE2E(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.OracleAdmin)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)

	const account = "PTAH_RM_PLAN"
	const role = "PTAH_RM_READER"
	dropOracleUser(ctx, c, admin, account)
	dropOracleRole(ctx, c, admin, role)
	createOracleUser(ctx, c, admin, account)
	defer dropOracleUser(context.WithoutCancel(ctx), c, admin, account)
	// The two privileges this loop needs, and they are two different
	// questions. SELECT_CATALOG_ROLE lets the account READ DBA_ROLES;
	// CREATE ROLE lets it make one. An account with the first and not the
	// second describes the server correctly and cannot apply the plan --
	// which is a privilege rather than a capability, and is why the key says
	// nothing about the account.
	execOracle(ctx, c, admin, "GRANT SELECT_CATALOG_ROLE TO "+account)
	execOracle(ctx, c, admin, "GRANT CREATE ROLE TO "+account)
	defer dropOracleRole(context.WithoutCancel(ctx), c, admin, role)

	conn, err := dbschema.ConnectToDatabase(ctx, oracleURLAs(c, adminURL, account))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	declared := oracleRoleManagementDeclaration(account, role)

	before, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	diff := schemadiff.CompareWithDialect(declared, before, platform.Oracle)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, platform.Oracle, planner.Options{Capabilities: conn.Info().Capabilities})
	c.Assert(err, qt.IsNil)

	// Non-vacuity: the plan really carries the role and the grants. Without
	// this, a capability gate that dropped them would leave an empty plan and
	// the convergence assertion below would pass having applied nothing.
	c.Assert(oracleStatementsNaming(statements, "CREATE ROLE"), qt.HasLen, 1)
	c.Assert(oracleStatementsNaming(statements, "GRANT"), qt.HasLen, 2)

	// And the ordering the plan has to get right: GRANT resolves its target
	// through the catalog, so a script that granted before creating the table
	// would answer ORA-00942 halfway through.
	c.Assert(oracleStatementIndex(c, statements, "CREATE TABLE") <
		oracleStatementIndex(c, statements, "GRANT"), qt.IsTrue)

	for _, statement := range statements {
		execOracle(ctx, c, conn, strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	}

	after, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleRoleNames(after.Roles), qt.Contains, role)

	settled := schemadiff.CompareWithDialect(declared, after, platform.Oracle)
	c.Assert(oracleRoleDiffSummary(settled), qt.DeepEquals, []string(nil))

	// And the plan the settled comparison produces is empty, which is the
	// statement-level form of the same claim.
	settledStatements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(settled, platform.Oracle, planner.Options{Capabilities: conn.Info().Capabilities})
	c.Assert(err, qt.IsNil)
	c.Assert(oracleStatementsNaming(settledStatements, "ROLE"), qt.HasLen, 0)
	c.Assert(oracleStatementsNaming(settledStatements, "GRANT"), qt.HasLen, 0)
}

// oracleRoleManagementDeclaration declares one table, one role and the two
// privileges the role holds on it.
func oracleRoleManagementDeclaration(schema, role string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "RM", Name: "rm_docs"}},
		Fields: []schemamodel.Field{
			{StructName: "RM", Name: "id", Type: "INT", Primary: true},
		},
		Roles: []schemamodel.Role{{StructName: "RM", Name: role, Inherit: true}},
		Grants: []schemamodel.Grant{{
			StructName: "RM", Role: role,
			Privileges: []string{"SELECT", "INSERT"},
			OnTable:    schema + ".rm_docs",
		}},
	}
}

// oracleStatementIndex is the position of the first planned statement
// carrying a keyword.
func oracleStatementIndex(c *qt.C, statements []string, keyword string) int {
	c.Helper()
	for i, statement := range statements {
		if strings.Contains(strings.ToUpper(statement), keyword) {
			return i
		}
	}
	c.Fatalf("no planned statement carries %q", keyword)
	return -1
}

// oracleStatementsNaming returns the planned statements carrying a keyword.
func oracleStatementsNaming(statements []string, keyword string) []string {
	var found []string
	for _, statement := range statements {
		if strings.Contains(strings.ToUpper(statement), keyword) {
			found = append(found, statement)
		}
	}
	return found
}
