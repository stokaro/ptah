package clickhouse_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/planner/dialects/clickhouse"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// rbacGrant is the reference this file grants and revokes with. Every field is
// spelled out because ObjectType is what tells a ClickHouse renderer whether the
// scope is `db`.`t` or `db`.*, and a row that left it empty would assert nothing
// about the half of the node that decides the statement's meaning.
func rbacGrant(privilege string) types.GrantRef {
	return types.GrantRef{
		Role:       "reporting",
		Privilege:  privilege,
		ObjectType: "TABLE",
		ObjectName: "analytics.events",
	}
}

// nodeTypes renders the plan as the sequence of node types it is, which is how
// the ordering assertions below say "roles precede grants" without naming an
// index that moves whenever a phase is added.
func nodeTypes(nodes []ast.Node) []string {
	kinds := make([]string, 0, len(nodes))
	for _, node := range nodes {
		kinds = append(kinds, fmt.Sprintf("%T", node))
	}
	return kinds
}

func planClickHouse(c *qt.C, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	nodes, err := clickhouse.New().GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	return nodes
}

// TestGenerateMigrationAST_ClickHouseGrantsArePlannedAsGrantNodes covers the
// two ways a GRANT reaches the plan.
//
// The GrantOptionsAdded row is the one worth having. That category describes a
// privilege the role already holds and differs only in grant_option, so a GRANT
// planned without WITH GRANT OPTION re-issues a grant the server already has:
// the statement succeeds, grant_option stays 0, and the next comparison asks for
// the same change forever. The planner did exactly that until this test existed,
// invisibly, because the renderer reduced every grant node to a comment.
func TestGenerateMigrationAST_ClickHouseGrantsArePlannedAsGrantNodes(t *testing.T) {
	tests := []struct {
		name           string
		diff           *types.SchemaDiff
		wantPrivileges []string
		wantWithOption bool
	}{
		{
			name:           "new grant without option",
			diff:           &types.SchemaDiff{GrantsAdded: []types.GrantRef{rbacGrant("SELECT")}},
			wantPrivileges: []string{"SELECT"},
			wantWithOption: false,
		},
		{
			name: "new grant carrying the option",
			diff: &types.SchemaDiff{GrantsAdded: []types.GrantRef{{
				Role:       "reporting",
				Privilege:  "SELECT",
				ObjectType: "TABLE",
				ObjectName: "analytics.events",
				WithOption: true,
			}}},
			wantPrivileges: []string{"SELECT"},
			wantWithOption: true,
		},
		{
			name:           "grant option added to a privilege already held",
			diff:           &types.SchemaDiff{GrantOptionsAdded: []types.GrantRef{rbacGrant("INSERT")}},
			wantPrivileges: []string{"INSERT"},
			wantWithOption: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes := planClickHouse(c, test.diff, &goschema.Database{})

			c.Assert(nodes, qt.HasLen, 1)
			node, ok := nodes[0].(*ast.GrantPrivilegeNode)
			c.Assert(ok, qt.IsTrue, qt.Commentf("want a GRANT node, got %T", nodes[0]))
			c.Assert(node.Role, qt.Equals, "reporting")
			c.Assert(node.Privileges, qt.DeepEquals, test.wantPrivileges)
			c.Assert(node.ObjectType, qt.Equals, "TABLE")
			c.Assert(node.ObjectName, qt.Equals, "analytics.events")
			c.Assert(node.WithOption, qt.Equals, test.wantWithOption)
			c.Assert(node.Comment, qt.Equals, "")
		})
	}
}

// TestGenerateMigrationAST_ClickHouseRevokesArePlannedAsRevokeNodes covers the
// two ways a REVOKE reaches the plan.
//
// GrantOptionFor is what separates them, and it is the whole statement:
// `REVOKE GRANT OPTION FOR SELECT ...` moves grant_option from 1 to 0 and leaves
// the privilege in place, while the same statement without the clause takes away
// a privilege the schema still declares. One statement does the downgrade, so a
// grant-option revocation plans exactly one node and no re-grant after it.
func TestGenerateMigrationAST_ClickHouseRevokesArePlannedAsRevokeNodes(t *testing.T) {
	tests := []struct {
		name               string
		diff               *types.SchemaDiff
		wantPrivileges     []string
		wantGrantOptionFor bool
	}{
		{
			name:               "grant no longer declared",
			diff:               &types.SchemaDiff{GrantsRemoved: []types.GrantRef{rbacGrant("SELECT")}},
			wantPrivileges:     []string{"SELECT"},
			wantGrantOptionFor: false,
		},
		{
			name:               "grant option withdrawn from a privilege that stays",
			diff:               &types.SchemaDiff{GrantOptionsRevoked: []types.GrantRef{rbacGrant("INSERT")}},
			wantPrivileges:     []string{"INSERT"},
			wantGrantOptionFor: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes := planClickHouse(c, test.diff, &goschema.Database{})

			c.Assert(nodes, qt.HasLen, 1)
			node, ok := nodes[0].(*ast.RevokePrivilegeNode)
			c.Assert(ok, qt.IsTrue, qt.Commentf("want a REVOKE node, got %T", nodes[0]))
			c.Assert(node.Role, qt.Equals, "reporting")
			c.Assert(node.Privileges, qt.DeepEquals, test.wantPrivileges)
			c.Assert(node.ObjectType, qt.Equals, "TABLE")
			c.Assert(node.ObjectName, qt.Equals, "analytics.events")
			c.Assert(node.GrantOptionFor, qt.Equals, test.wantGrantOptionFor)
			c.Assert(node.Comment, qt.Equals, "")
		})
	}
}

// TestGenerateMigrationAST_ClickHouseCreateRoleCarriesOnlyTheName pins the node
// against the measured shape of a ClickHouse role: system.roles is exactly
// (name, id, storage) on 26.7.3.19 and 24.10.4.191, and `CREATE ROLE r COMMENT
// 'x'` is a syntax error there.
//
// Every attribute [ast.CreateRoleNode] carries for PostgreSQL's sake is asserted
// to be absent, because a planner that filled one in would hand the renderer a
// credential or a privilege the target has nowhere to put -- and the safe
// outcome of that, silently dropping it, is what leaves an operator believing a
// password was set. Inherit is the one exception, and it is not an attribute:
// ClickHouse role membership always inherits.
func TestGenerateMigrationAST_ClickHouseCreateRoleCarriesOnlyTheName(t *testing.T) {
	tests := []struct {
		name  string
		roles []string
		want  []string
	}{
		{name: "one role", roles: []string{"reporting"}, want: []string{"reporting"}},
		{
			name:  "several roles keep the comparison's order",
			roles: []string{"admin_role", "app_role", "readonly_role"},
			want:  []string{"admin_role", "app_role", "readonly_role"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes := planClickHouse(c, &types.SchemaDiff{RolesAdded: test.roles}, &goschema.Database{})

			c.Assert(nodes, qt.HasLen, len(test.want))
			got := make([]string, 0, len(nodes))
			for _, node := range nodes {
				role, ok := node.(*ast.CreateRoleNode)
				c.Assert(ok, qt.IsTrue, qt.Commentf("want a CREATE ROLE node, got %T", node))
				got = append(got, role.Name)
				c.Assert(role.Login, qt.IsFalse)
				c.Assert(role.Password, qt.Equals, "")
				c.Assert(role.Superuser, qt.IsFalse)
				c.Assert(role.CreateDB, qt.IsFalse)
				c.Assert(role.CreateRole, qt.IsFalse)
				c.Assert(role.Replication, qt.IsFalse)
				c.Assert(role.Comment, qt.Equals, "")
				c.Assert(role.Inherit, qt.IsTrue)
			}
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestGenerateMigrationAST_ClickHouseRolesArePlannedBeforeTheGrantsThatNameThem
// is the ordering assertion the server forces.
//
// Granting to a role that does not exist yet answers `Code: 511. DB::Exception:
// ... (UNKNOWN_ROLE)` and grants nothing, so a plan that emitted the GRANT first
// would fail the migration on a schema that is perfectly expressible. The
// revokes ahead of the grants are the second ordering rule: ClickHouse absorbs a
// narrower grant into a broader one silently, so granting `db`.`t` before
// revoking `db`.* leaves one row that the revoke then removes entirely.
func TestGenerateMigrationAST_ClickHouseRolesArePlannedBeforeTheGrantsThatNameThem(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesAdded:         []string{"events"},
		RolesAdded:          []string{"reporting"},
		GrantsRemoved:       []types.GrantRef{rbacGrant("DROP")},
		GrantOptionsRevoked: []types.GrantRef{rbacGrant("ALTER")},
		GrantsAdded:         []types.GrantRef{rbacGrant("SELECT")},
		GrantOptionsAdded:   []types.GrantRef{rbacGrant("INSERT")},
	}

	nodes := planClickHouse(c, diff, mkDB())

	c.Assert(nodeTypes(nodes), qt.DeepEquals, []string{
		"*ast.CreateTableNode",
		"*ast.CreateRoleNode",
		"*ast.RevokePrivilegeNode",
		"*ast.RevokePrivilegeNode",
		"*ast.GrantPrivilegeNode",
		"*ast.GrantPrivilegeNode",
	})

	role, ok := nodes[1].(*ast.CreateRoleNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("want a CREATE ROLE node, got %T", nodes[1]))
	c.Assert(role.Name, qt.Equals, "reporting")

	firstRevoke, ok := nodes[2].(*ast.RevokePrivilegeNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("want a REVOKE node, got %T", nodes[2]))
	c.Assert(firstRevoke.Role, qt.Equals, "reporting")
	c.Assert(firstRevoke.Privileges, qt.DeepEquals, []string{"DROP"})
	c.Assert(firstRevoke.GrantOptionFor, qt.IsFalse)

	optionRevoke, ok := nodes[3].(*ast.RevokePrivilegeNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("want a REVOKE node, got %T", nodes[3]))
	c.Assert(optionRevoke.Privileges, qt.DeepEquals, []string{"ALTER"})
	c.Assert(optionRevoke.GrantOptionFor, qt.IsTrue)

	firstGrant, ok := nodes[4].(*ast.GrantPrivilegeNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("want a GRANT node, got %T", nodes[4]))
	c.Assert(firstGrant.Role, qt.Equals, "reporting")
	c.Assert(firstGrant.Privileges, qt.DeepEquals, []string{"SELECT"})
	c.Assert(firstGrant.WithOption, qt.IsFalse)

	optionGrant, ok := nodes[5].(*ast.GrantPrivilegeNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("want a GRANT node, got %T", nodes[5]))
	c.Assert(optionGrant.Privileges, qt.DeepEquals, []string{"INSERT"})
	c.Assert(optionGrant.WithOption, qt.IsTrue)
}

// TestGenerateMigrationAST_ClickHouseGrantsKeepTheSlotTheRenderPathUsesForThem
// pins where the two RBAC phases sit among the objects around them.
//
// The offline render path emits roles right after the tables and grants between
// the row-level security statements and the triggers. The plan has to agree on
// order and not merely on content, because `schema render` and
// `schema apply --dry-run` are compared line for line; the ordering also has to
// survive the phases that stayed diagnostics, which is why this case declares
// one of each around the two that did not.
func TestGenerateMigrationAST_ClickHouseGrantsKeepTheSlotTheRenderPathUsesForThem(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		RolesAdded:            []string{"reporting"},
		FunctionsAdded:        []string{"bump"},
		RLSEnabledTablesAdded: []string{"events"},
		RLSPoliciesAdded:      []types.RLSPolicyRef{{PolicyName: "p1", TableName: "events"}},
		GrantsAdded:           []types.GrantRef{rbacGrant("SELECT")},
		TriggersAdded:         []types.TriggerRef{{TriggerName: "trg1", TableName: "events"}},
	}

	// The row-policy phase plans from the declaration now that ClickHouse
	// carries capability.RowLevelSecurity, so the desired schema has to hold
	// what the diff names or the phase contributes nothing and the ordering
	// this test is about cannot be seen (stokaro/ptah#1736).
	nodes := planClickHouse(c, diff, &goschema.Database{
		RLSEnabledTables: []goschema.RLSEnabledTable{{Table: "events"}},
		RLSPolicies: []goschema.RLSPolicy{{
			Name: "p1", Table: "events", UsingExpression: "true",
		}},
	})

	c.Assert(nodeTypes(nodes), qt.DeepEquals, []string{
		"*ast.CreateRoleNode",
		"*ast.CreateFunctionNode",
		"*ast.AlterTableEnableRLSNode",
		"*ast.CreatePolicyNode",
		"*ast.GrantPrivilegeNode",
		"*ast.CreateTriggerNode",
	})
}

// TestGenerateMigrationAST_ClickHouseRoleChangesWithNoStatementAreNamed covers
// the two role categories that plan no SQL.
//
// A modification cannot be planned because a ClickHouse role has no attribute to
// alter, so whatever the comparison found is a disagreement about fields
// goschema.Role carries for PostgreSQL. A removal is not planned because a role
// is cluster-wide while Ptah manages one database, and dropping one would take
// away grants no declaration here describes -- stokaro/ptah#1025 lists that as a
// non-goal, and migration/schemadiff never produces the category anyway.
//
// Neither is dropped in silence, which is the rule the rest of this planner
// follows: a diff category nothing emits for produces a plan that exits 0 having
// said nothing about a difference the comparison did find.
func TestGenerateMigrationAST_ClickHouseRoleChangesWithNoStatementAreNamed(t *testing.T) {
	tests := []struct {
		name string
		diff *types.SchemaDiff
		want string
	}{
		{
			name: "modified role",
			diff: &types.SchemaDiff{RolesModified: []types.RoleDiff{{
				RoleName: "reporting",
				Changes:  map[string]string{"login": "false -> true"},
			}}},
			want: `CLICKHOUSE: role "reporting" differs only in attributes a ClickHouse role does not carry; nothing is altered`,
		},
		{
			name: "removed role",
			diff: &types.SchemaDiff{RolesRemoved: []string{"reporting"}},
			want: `CLICKHOUSE: role "reporting" exists on the server and not in the schema; Ptah does not drop ClickHouse roles`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes := planClickHouse(c, test.diff, &goschema.Database{})

			c.Assert(nodes, qt.HasLen, 1)
			comment, ok := nodes[0].(*ast.CommentNode)
			c.Assert(ok, qt.IsTrue, qt.Commentf("want a comment node, got %T", nodes[0]))
			c.Assert(comment.Text, qt.Equals, test.want)
		})
	}
}
