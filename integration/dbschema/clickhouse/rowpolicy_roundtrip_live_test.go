//go:build integration

package clickhouse_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestClickHouseRowPolicyRoundTripsLive is the test the RowLevelSecurity
// capability could not have been flipped without.
//
// The key promises render, read back and plan together, and the failure when one
// is missing is not a compile error but an apply loop planning the same policy
// forever. Step 3 is what decides it.
//
// This target also has a trap no offline assertion reaches: `WITH CHECK` PARSES
// here and is then ignored, so a renderer that passed it through would report
// success while leaving writes open. The renderer refuses that declaration, and
// the refusal is only trustworthy if the engine really does swallow the clause —
// which the second test measures rather than asserts from a comment.
func TestClickHouseRowPolicyRoundTripsLive(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	database := conn.Info().Schema
	role := uniqueClickHouseRBACName("policy_reader")
	table := uniqueClickHouseRBACName("policy_orders")
	policy := uniqueClickHouseRBACName("tenant_isolation")
	dropClickHouseRoleAfterTest(c, conn, role)
	createClickHouseRBACTable(c, conn, database, table)
	dropClickHouseRowPolicyAfterTest(c, conn, database, table, policy)

	declared := clickHouseRowPolicyDeclaration(role, database, table, policy)

	// 1. The planner's statements are what the server is given, so a statement
	// this engine refuses fails here rather than being corrected by hand.
	creation, createStatements := planClickHouseRowPolicies(c, conn, declared)
	c.Assert(creation.RLSPoliciesAdded, qt.HasLen, 1)
	c.Assert(strings.Join(createStatements, "\n"), qt.Contains, "CREATE ROW POLICY")
	applyClickHouseRBACPlan(c, conn, createStatements)

	// 2. The catalog is asked what it holds. USING and TO have to come back in
	// the spelling the declaration used, not the catalog's.
	created := readClickHouseRowPolicies(c, conn)
	read := clickHouseRowPolicyNamed(created, policy)
	c.Assert(read.Table, qt.Equals, table)
	c.Assert(read.UsingExpression, qt.Equals, "tenant_id = 1")
	c.Assert(read.ToRoles, qt.Equals, role)

	// 3. The convergence assertion. The same declaration against a freshly read
	// database must leave nothing to do in any policy category.
	settled, settledStatements := planClickHouseRowPolicies(c, conn, declared)
	c.Assert(settled.RLSPoliciesAdded, qt.HasLen, 0)
	c.Assert(settled.RLSPoliciesModified, qt.HasLen, 0)
	c.Assert(settled.RLSPoliciesRemoved, qt.HasLen, 0)
	c.Assert(clickHouseRowPolicyStatements(settledStatements), qt.HasLen, 0)

	// 4. A changed filter plans ONE statement. ClickHouse alters a policy in
	// place and rejects CREATE OR REPLACE ROW POLICY, so a replacement planned
	// as a drop-then-create pair would be both wrong and destructive.
	changed := clickHouseRowPolicyDeclaration(role, database, table, policy)
	changed.RLSPolicies[0].UsingExpression = "tenant_id = 2"
	modification, modifyStatements := planClickHouseRowPolicies(c, conn, changed)
	c.Assert(modification.RLSPoliciesModified, qt.HasLen, 1)
	policyStatements := clickHouseRowPolicyStatements(modifyStatements)
	c.Assert(policyStatements, qt.HasLen, 1)
	applyClickHouseRBACPlan(c, conn, modifyStatements)

	after := readClickHouseRowPolicies(c, conn)
	c.Assert(clickHouseRowPolicyNamed(after, policy).UsingExpression, qt.Equals, "tenant_id = 2")
}

// TestClickHouseRowPolicyToAllRoundTripsLive covers the TO spelling the catalog
// splits across three columns.
//
// `TO ALL` sets apply_to_all rather than filling apply_to_list, so a read built
// from the list alone reports a policy that names nobody -- which is the
// spelling a policy with no TO clause at all uses. The two would then be
// indistinguishable, and a policy applying to everyone would compare equal to
// one applying to no one.
func TestClickHouseRowPolicyToAllRoundTripsLive(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	database := conn.Info().Schema
	role := uniqueClickHouseRBACName("toall_reader")
	table := uniqueClickHouseRBACName("toall_orders")
	policy := uniqueClickHouseRBACName("toall_policy")
	dropClickHouseRoleAfterTest(c, conn, role)
	createClickHouseRBACTable(c, conn, database, table)
	dropClickHouseRowPolicyAfterTest(c, conn, database, table, policy)

	declared := clickHouseRowPolicyDeclaration(role, database, table, policy)
	declared.RLSPolicies[0].ToRoles = "ALL"

	_, statements := planClickHouseRowPolicies(c, conn, declared)
	applyClickHouseRBACPlan(c, conn, statements)

	read := clickHouseRowPolicyNamed(readClickHouseRowPolicies(c, conn), policy)
	c.Assert(read.ToRoles, qt.Equals, "ALL")

	settled, _ := planClickHouseRowPolicies(c, conn, declared)
	c.Assert(settled.RLSPoliciesModified, qt.HasLen, 0)
	c.Assert(settled.RLSPoliciesAdded, qt.HasLen, 0)
}

// TestClickHouseSwallowsAWriteCheckLive measures the acceptance the renderer
// refuses a declaration over.
//
// Without this the refusal is only this renderer's opinion. `WITH CHECK` is not
// a syntax error here — the statement succeeds — and the clause simply does not
// survive into the catalog, which is why rendering the policy anyway would leave
// an author with reads filtered and writes open.
func TestClickHouseSwallowsAWriteCheckLive(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	database := conn.Info().Schema
	table := uniqueClickHouseRBACName("check_orders")
	policy := uniqueClickHouseRBACName("check_policy")
	createClickHouseRBACTable(c, conn, database, table)
	dropClickHouseRowPolicyAfterTest(c, conn, database, table, policy)

	c.Assert(conn.Writer().ExecuteSQL(c.Context(),
		"CREATE ROW POLICY "+policy+" ON "+database+"."+table+
			" USING tenant_id = 1 WITH CHECK tenant_id = 1"),
		qt.IsNil, qt.Commentf("the engine is expected to ACCEPT this, which is the whole point"))

	read := clickHouseRowPolicyNamed(readClickHouseRowPolicies(c, conn), policy)
	c.Assert(read.UsingExpression, qt.Equals, "tenant_id = 1")
	// Nothing anywhere records the check. The catalog has one filter column.
	c.Assert(read.WithCheckExpression, qt.Equals, "")
}

// clickHouseRowPolicyDeclaration is a table under one policy naming one role.
func clickHouseRowPolicyDeclaration(role, database, table, policy string) *goschema.Database {
	declared := clickHouseRBACDeclaration(role, database+"."+table, false)
	declared.RLSPolicies = []goschema.RLSPolicy{{
		StructName:      "Order",
		Name:            policy,
		Table:           table,
		PolicyFor:       "ALL",
		ToRoles:         role,
		UsingExpression: "tenant_id = 1",
	}}
	return declared
}

// clickHouseRowPolicyNamed returns the policy a read reports under a name.
func clickHouseRowPolicyNamed(schema *dbschematypes.DBSchema, name string) dbschematypes.DBRLSPolicy {
	for _, policy := range schema.RLSPolicies {
		if policy.Name == name {
			return policy
		}
	}
	return dbschematypes.DBRLSPolicy{}
}

// clickHouseRowPolicyStatements keeps the planned statements that name a row
// policy, so a count is about this feature rather than about everything else
// the same plan carries.
func clickHouseRowPolicyStatements(statements []string) []string {
	kept := make([]string, 0, len(statements))
	for _, statement := range statements {
		if strings.Contains(statement, "ROW POLICY") {
			kept = append(kept, statement)
		}
	}
	return kept
}

// dropClickHouseRowPolicyAfterTest removes the policy this suite created, so a
// shared database is not left carrying a filter nobody declared.
func dropClickHouseRowPolicyAfterTest(c *qt.C, conn *dbschema.DatabaseConnection, database, table, policy string) {
	c.Helper()
	c.Cleanup(func() {
		cleanupClickHouseRBACFixture(c, conn,
			"DROP ROW POLICY IF EXISTS "+sqlident.Quote(platform.ClickHouse, policy)+
				" ON "+sqlident.Qualified(platform.ClickHouse, database, table))
	})
}

// readClickHouseRowPolicies reads the whole description rather than the
// role-and-grant projection [readClickHouseRBAC] returns.
//
// That projection is deliberate where it lives -- those tests are about roles
// and grants, and narrowing what they see keeps an unrelated regression from
// reading as an RBAC failure. It is the wrong instrument here for the same
// reason: it drops RLSPolicies, so a comparison built on it would report every
// policy as an addition forever and step 3 would pass while proving nothing.
func readClickHouseRowPolicies(c *qt.C, conn *dbschema.DatabaseConnection) *dbschematypes.DBSchema {
	c.Helper()
	schema, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	return schema
}

// planClickHouseRowPolicies re-inspects the database and returns what the
// comparison found and what would be run for it.
//
// The read happens here rather than at the call site so that every plan is
// built from a description taken after the previous plan was applied.
func planClickHouseRowPolicies(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	declared *goschema.Database,
) (*difftypes.SchemaDiff, []string) {
	c.Helper()
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabaseInfo(declared, readClickHouseRowPolicies(c, conn), info, nil)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		diff, declared, info.Dialect, info.Capabilities,
	)
	c.Assert(err, qt.IsNil)
	return diff, statements
}
