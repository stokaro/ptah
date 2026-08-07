package postgres

// White-box testing required: the whole of stokaro/ptah#1267 is the SQL that
// readRoles sends. PostgreSQL roles are cluster-wide, so the exported reader
// API cannot show the difference between "this role belongs to the inspected
// schemas" and "this role exists on the server" without a live server holding
// a role that belongs to some other database. readRoles is unexported and the
// query text has no other source.
//
// The fake server below answers a role only when the query actually reads the
// catalog column that is the reason that role is in scope, and only when the
// query binds the schema the reason lives in. A branch replaced by a literal,
// or a restriction dropped, is therefore visible as a missing or an extra role
// rather than being silently answered.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// clusterRole is one role on the simulated server together with the single
// reason it is reachable from an inspected schema.
type clusterRole struct {
	// name is the role name pg_roles reports.
	name string
	// schema is the schema the reason lives in. An empty schema means the
	// role has no reason at all: it exists on the server and nothing in any
	// database refers to it. That is the role stokaro/ptah#1267 is about.
	schema string
	// reads lists the catalog expressions that must all appear in one single
	// branch of the query before the server is willing to report this role.
	// Requiring them in the same branch is the point: two branches that read
	// the same catalog for different columns are separate reasons, and one of
	// them stopping must not be covered by the other.
	reads []string
}

// Catalog expressions, one per reason a role counts as used. These are the
// expressions the query is expected to contain, not prose: SQL comments are
// stripped before matching, so a branch documented by name but not actually
// read cannot satisfy the guard.
const (
	readsRelationACL      = "aclexplode(c.relacl)"
	readsSchemaACL        = "aclexplode(s.nspacl)"
	readsGrantee          = "acl.grantee"
	readsGrantor          = "acl.grantor"
	readsPolicyRoles      = "unnest(pol.polroles)"
	readsSystemExclusion  = "NOT LIKE 'pg_%'"
	readsPostgresExcluded = "!= 'postgres'"

	// readsScopedRoleSet is the outer statement consuming the set the branches
	// build. Binding the schema names is not on its own a restriction: the
	// scope CTE still references them, so a query that computes the used-role
	// set and then never applies it binds every argument and answers the whole
	// cluster anyway. Live PostgreSQL caught exactly that mutant while an
	// earlier version of this fake did not.
	readsScopedRoleSet = "FROM used"
)

// Ownership columns. These name reasons the query must NOT act on, so they
// exist to be asserted absent. See TestReadRolesDoesNotTreatOwnershipAsUse.
const (
	readsSchemaOwner   = "s.nspowner"
	readsRelationOwner = "c.relowner"
	readsTypeOwner     = "t.typowner"
	readsRoutineOwner  = "p.proowner"
)

// Reasons spelled as the branch that must carry them.
var (
	byRelationGrant   = []string{readsRelationACL, readsGrantee}
	byRelationGrantor = []string{readsRelationACL, readsGrantor}
	bySchemaGrant     = []string{readsSchemaACL, readsGrantee}
	bySchemaGrantor   = []string{readsSchemaACL, readsGrantor}
	byPolicy          = []string{readsPolicyRoles}

	bySchemaOwner   = []string{readsSchemaOwner}
	byRelationOwner = []string{readsRelationOwner}
	byTypeOwner     = []string{readsTypeOwner}
	byRoutineOwner  = []string{readsRoutineOwner}
)

// stripSQLComments removes `-- ...` comments so the reader's prose cannot
// stand in for the catalog expression it describes.
func stripSQLComments(query string) string {
	lines := strings.Split(query, "\n")
	for i, line := range lines {
		lines[i], _, _ = strings.Cut(line, "--")
	}
	return strings.Join(lines, "\n")
}

// unionBranches splits a comment-stripped query at its UNION keywords, so each
// reason can be checked against the branch that is supposed to carry it rather
// than against the whole statement.
func unionBranches(stripped string) []string {
	var branches []string
	var current []string
	for line := range strings.SplitSeq(stripped, "\n") {
		if strings.EqualFold(strings.TrimSpace(line), "UNION") {
			branches = append(branches, strings.Join(current, "\n"))
			current = nil
			continue
		}
		current = append(current, line)
	}
	return append(branches, strings.Join(current, "\n"))
}

// newRolesServer returns a Reader backed by a server holding cluster, scoped to
// schemas and reporting caps.
func newRolesServer(
	tb interface{ Cleanup(func()) },
	cluster []clusterRole,
	schemas []string,
	caps capability.Capabilities,
) *Reader {
	db := dbtest.Open(tb, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		return answerRoles(query, args, cluster)
	})
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, schemas[0], caps)
	reader.SetSchemas(schemas)
	return reader
}

// answerRoles plays PostgreSQL for the roles query.
//
// Scope comes from the bound arguments rather than from the spelling of a
// WHERE clause: a query that binds no schema has asked for the whole cluster,
// which is exactly the pre-fix behavior, and the server answers accordingly.
func answerRoles(query string, args []driver.NamedValue, cluster []clusterRole) (dbtest.QueryResult, error) {
	stripped := stripSQLComments(query)

	bound := make(map[string]bool, len(args))
	for _, arg := range args {
		placeholder := fmt.Sprintf("$%d", arg.Ordinal)
		if !strings.Contains(stripped, placeholder) {
			return dbtest.QueryResult{}, fmt.Errorf(
				"argument %s is bound but %s never appears in the query", arg.Value, placeholder,
			)
		}
		name, ok := arg.Value.(string)
		if !ok {
			return dbtest.QueryResult{}, fmt.Errorf("argument %s is not a schema name", placeholder)
		}
		bound[name] = true
	}

	result := dbtest.QueryResult{
		Columns: []string{
			"role_name", "login", "superuser", "create_db",
			"create_role", "inherit", "replication", "has_password", "comment",
		},
	}
	branches := unionBranches(stripped)
	for _, role := range cluster {
		if !roleIsAnswerable(role, stripped, branches, bound) {
			continue
		}
		result.Rows = append(result.Rows, []driver.Value{
			role.name, false, false, false, false, true, false, false, "",
		})
	}
	return result, nil
}

// roleIsAnswerable reports whether the server would hand this role back for
// the given query.
func roleIsAnswerable(role clusterRole, stripped string, branches []string, bound map[string]bool) bool {
	if strings.HasPrefix(role.name, "pg_") && strings.Contains(stripped, readsSystemExclusion) {
		return false
	}
	if role.name == "postgres" && strings.Contains(stripped, readsPostgresExcluded) {
		return false
	}
	if len(bound) == 0 || !strings.Contains(stripped, readsScopedRoleSet) {
		// Nothing restricts the answer to the inspected schemas, so every role
		// on the server is in it.
		return true
	}
	if role.schema == "" || !bound[role.schema] {
		return false
	}
	return someBranchReads(branches, role.reads)
}

// someBranchReads reports whether one branch of the query reads every catalog
// expression the reason requires.
func someBranchReads(branches []string, reads []string) bool {
	for _, branch := range branches {
		if branchReadsAll(branch, reads) {
			return true
		}
	}
	return false
}

func branchReadsAll(branch string, reads []string) bool {
	if len(reads) == 0 {
		return false
	}
	for _, expr := range reads {
		if !strings.Contains(branch, expr) {
			return false
		}
	}
	return true
}

// fullCluster is a server holding one role per reason a role can be used by
// the schema "public", one role used only by the schema "app", and three roles
// that are used by nothing in this database.
func fullCluster() []clusterRole {
	return []clusterRole{
		{name: "app_schema_grantee", schema: "app", reads: bySchemaGrant},
		{name: "pg_reserved", schema: "public", reads: byRelationGrant},
		{name: "policy_named", schema: "public", reads: byPolicy},
		{name: "postgres", schema: "public", reads: byRelationGrant},
		{name: "schema_grantee", schema: "public", reads: bySchemaGrant},
		{name: "schema_grantor", schema: "public", reads: bySchemaGrantor},
		{name: "someone_elses", schema: "", reads: nil},
		{name: "table_grantee", schema: "public", reads: byRelationGrant},
		{name: "table_grantor", schema: "public", reads: byRelationGrantor},
		{name: "third_party", schema: "", reads: nil},
		{name: "unrelated_tenant", schema: "", reads: nil},
	}
}

func roleNames(roles []types.DBRole) []string {
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		names = append(names, role.Name)
	}
	return names
}

func TestReadRolesReportsOneRolePerReason(t *testing.T) {
	c := qt.New(t)

	// Each row is a server holding exactly one role with a reason plus the
	// role from stokaro/ptah#1267 that has none, so a branch that stops
	// reading its catalog column loses its own row and nothing else.
	tests := []struct {
		name    string
		used    clusterRole
		schemas []string
	}{
		{
			name:    "holds a privilege on a relation in scope",
			used:    clusterRole{name: "table_grantee", schema: "public", reads: byRelationGrant},
			schemas: []string{"public"},
		},
		{
			name:    "granted a privilege on a relation in scope",
			used:    clusterRole{name: "table_grantor", schema: "public", reads: byRelationGrantor},
			schemas: []string{"public"},
		},
		{
			name:    "holds a privilege on the schema in scope",
			used:    clusterRole{name: "schema_grantee", schema: "public", reads: bySchemaGrant},
			schemas: []string{"public"},
		},
		{
			name:    "granted a privilege on the schema in scope",
			used:    clusterRole{name: "schema_grantor", schema: "public", reads: bySchemaGrantor},
			schemas: []string{"public"},
		},
		{
			name:    "named by a policy on a table in scope",
			used:    clusterRole{name: "policy_named", schema: "public", reads: byPolicy},
			schemas: []string{"public"},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			cluster := []clusterRole{
				test.used,
				{name: "someone_elses", schema: "", reads: nil},
			}
			reader := newRolesServer(c.TB, cluster, test.schemas, capability.Postgres16())

			roles, err := reader.readRoles()

			c.Assert(err, qt.IsNil)
			c.Assert(roleNames(roles), qt.DeepEquals, []string{test.used.name})
		})
	}
}

func TestReadRolesLeavesClusterRolesTheScopeDoesNotUseOut(t *testing.T) {
	c := qt.New(t)

	// The headline of stokaro/ptah#1267: a database with objects of its own
	// and no roles of its own must name no roles, however many the server has.
	tests := []struct {
		name    string
		cluster []clusterRole
		schemas []string
	}{
		{
			name: "every role belongs to another database",
			cluster: []clusterRole{
				{name: "advsup_reader", schema: "", reads: nil},
				{name: "app_reader", schema: "", reads: nil},
				{name: "appuser", schema: "", reads: nil},
				{name: "w1251c_reader", schema: "", reads: nil},
			},
			schemas: []string{"public"},
		},
		{
			name: "a role used by a schema that is out of scope",
			cluster: []clusterRole{
				{name: "app_schema_grantee", schema: "app", reads: bySchemaGrant},
				{name: "someone_elses", schema: "", reads: nil},
			},
			schemas: []string{"public"},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			reader := newRolesServer(c.TB, test.cluster, test.schemas, capability.Postgres16())

			roles, err := reader.readRoles()

			c.Assert(err, qt.IsNil)
			c.Assert(roles, qt.HasLen, 0)
		})
	}
}

func TestReadRolesFollowsTheSchemasBeingRead(t *testing.T) {
	c := qt.New(t)

	// Same server, three scopes. A filter that merely dropped some fixed set
	// of names would answer all three the same way.
	tests := []struct {
		name    string
		schemas []string
		want    []string
	}{
		{
			name:    "public only",
			schemas: []string{"public"},
			want: []string{
				"policy_named", "schema_grantee", "schema_grantor",
				"table_grantee", "table_grantor",
			},
		},
		{
			name:    "app only",
			schemas: []string{"app"},
			want:    []string{"app_schema_grantee"},
		},
		{
			name:    "both schemas",
			schemas: []string{"public", "app"},
			want: []string{
				"app_schema_grantee", "policy_named", "schema_grantee",
				"schema_grantor", "table_grantee", "table_grantor",
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			reader := newRolesServer(c.TB, fullCluster(), test.schemas, capability.Postgres16())

			roles, err := reader.readRoles()

			c.Assert(err, qt.IsNil)
			c.Assert(roleNames(roles), qt.DeepEquals, test.want)
		})
	}
}

func TestReadRolesKeepsSystemRolesOut(t *testing.T) {
	c := qt.New(t)

	// Pre-existing behavior this change must not lose: the reserved pg_ roles
	// and the bootstrap superuser are never described, even when they own an
	// object in the inspected schema.
	tests := []struct {
		name    string
		absent  string
		cluster []clusterRole
	}{
		{
			name:   "reserved role holding a privilege in scope",
			absent: "pg_reserved",
			cluster: []clusterRole{
				{name: "pg_reserved", schema: "public", reads: byRelationGrant},
				{name: "table_grantee", schema: "public", reads: byRelationGrant},
			},
		},
		{
			name:   "bootstrap superuser holding a privilege in scope",
			absent: "postgres",
			cluster: []clusterRole{
				{name: "postgres", schema: "public", reads: byRelationGrant},
				{name: "table_grantee", schema: "public", reads: byRelationGrant},
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			reader := newRolesServer(c.TB, test.cluster, []string{"public"}, capability.Postgres16())

			roles, err := reader.readRoles()

			c.Assert(err, qt.IsNil)
			c.Assert(roleNames(roles), qt.DeepEquals, []string{"table_grantee"})
			c.Assert(roleNames(roles), qt.Not(qt.Contains), test.absent)
		})
	}
}

func TestReadRolesAsksForPolicyRolesOnlyWherePoliciesExist(t *testing.T) {
	c := qt.New(t)

	// pg_policy is read under the same capability that gates readRLSPolicies,
	// so a PostgreSQL-family target that manages roles without row-level
	// security is not sent a query naming a catalog it does not have.
	tests := []struct {
		name string
		caps capability.Capabilities
		want []string
	}{
		{
			name: "row-level security supported",
			caps: capability.Postgres16(),
			want: []string{"policy_named", "table_grantee"},
		},
		{
			name: "row-level security not supported",
			caps: capability.Postgres16().With(capability.RowLevelSecurity, false),
			want: []string{"table_grantee"},
		},
	}

	cluster := []clusterRole{
		{name: "policy_named", schema: "public", reads: byPolicy},
		{name: "table_grantee", schema: "public", reads: byRelationGrant},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			reader := newRolesServer(c.TB, cluster, []string{"public"}, test.caps)

			roles, err := reader.readRoles()

			c.Assert(err, qt.IsNil)
			c.Assert(roleNames(roles), qt.DeepEquals, test.want)
		})
	}
}

func TestReadRolesDoesNotTreatOwnershipAsUse(t *testing.T) {
	c := qt.New(t)

	// Ptah describes no ownership: it emits no OWNER TO and no
	// CREATE SCHEMA ... AUTHORIZATION. An owner is therefore a role the
	// description would create and then never refer to, and since the
	// connecting superuser owns every object it creates, reading ownership
	// made every inspect describe the connecting role. Measured on live
	// PostgreSQL 18.4: a diff between a populated database and an empty dev
	// database in the same cluster planned
	// `CREATE ROLE "ptah_user" WITH LOGIN SUPERUSER CREATEDB CREATEROLE
	// INHERIT REPLICATION` and failed to apply it at SQLSTATE 42710.
	//
	// An owner a description does refer to is still described, through the
	// privilege clauses: granting anything on a relation makes its ACL
	// explicit, and an explicit ACL always carries the owner's own privileges.
	// That case is TestReadRolesReportsOneRolePerReason's relation-privilege
	// row, not this one.
	tests := []struct {
		name  string
		owner clusterRole
	}{
		{
			name:  "owns the schema in scope",
			owner: clusterRole{name: "schema_owner", schema: "public", reads: bySchemaOwner},
		},
		{
			name:  "owns a relation in scope",
			owner: clusterRole{name: "relation_owner", schema: "public", reads: byRelationOwner},
		},
		{
			name:  "owns a type in scope",
			owner: clusterRole{name: "type_owner", schema: "public", reads: byTypeOwner},
		},
		{
			name:  "owns a routine in scope",
			owner: clusterRole{name: "routine_owner", schema: "public", reads: byRoutineOwner},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			cluster := []clusterRole{
				test.owner,
				{name: "table_grantee", schema: "public", reads: byRelationGrant},
			}
			reader := newRolesServer(c.TB, cluster, []string{"public"}, capability.Postgres16())

			roles, err := reader.readRoles()

			c.Assert(err, qt.IsNil)
			c.Assert(roleNames(roles), qt.DeepEquals, []string{"table_grantee"})
		})
	}
}
