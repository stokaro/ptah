package postgres

// White-box testing required: the whole of stokaro/ptah#1267 is the SQL that
// readRoles and readRolesOutOfScope send. PostgreSQL roles are cluster-wide,
// so the exported reader API cannot show the difference between "this role
// belongs to the inspected schemas" and "this role exists on the server"
// without a live server holding a role that belongs to some other database.
// Both methods are unexported and the query text has no other source.
//
// The fake server below answers a role only when the query actually reads the
// catalog column that is the reason that role is in scope, and only when the
// query binds the schema the reason lives in. A branch replaced by a literal,
// or a restriction dropped, is therefore visible as a missing or an extra role
// rather than being silently answered.
//
// The fake evaluates the membership predicate rather than recognizing a token
// in it, and it holds its own literal copy of the two predicates it knows how
// to evaluate: a predicate it does not know is an error, never a guess. An
// earlier version decided "this is the complement" from the presence of
// `NOT EXISTS` anywhere in the statement and then computed the complement
// itself, which made the partition true by construction of the fake -- both
// `u.roleoid = u.roleoid` (the correlation dropped, so the complement is empty
// whenever anything is in scope, restoring the whole defect) and
// `TRUE OR NOT EXISTS (...)` (the complement returns everything, so the two
// lists overlap) walked past every test in this file.
// TestReadRolesComplementIsTheExactNegationOfTheScopedRead states the same
// property a second way, directly on the text the reader sends.
//
// The fake evaluates the reserved-prefix filter the same way: it reads the
// LIKE pattern rather than accepting any spelling of it, so leaving the
// underscore unescaped drops ordinary pg-prefixed roles here exactly as it
// does on a server (stokaro/ptah#1291).

import (
	"database/sql/driver"
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
	"go.5x5.cz/ptah/internal/rolescope"
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
	readsPostgresExcluded = "!= 'postgres'"

	// readsScopedRoleSet is the outer statement consuming the set the branches
	// build. Binding the schema names is not on its own a restriction: the
	// scope CTE still references them, so a query that computes the used-role
	// set and then never applies it binds every argument and answers the whole
	// cluster anyway. Live PostgreSQL caught exactly that mutant while an
	// earlier version of this fake did not.
	readsScopedRoleSet = "FROM used"
)

// The two spellings of the reserved-prefix filter, and what LIKE means by
// each. The escaped one matches the literal prefix `pg_`, which is what
// PostgreSQL reserves. The unescaped one reads the bare underscore as a
// single-character wildcard, so it also matches pgbouncer, pgadmin, pgpool and
// pguser -- ordinary user roles (stokaro/ptah#1291, escaped at four sites in
// stokaro/ptah#1292).
const (
	readsSystemExclusion          = `NOT LIKE 'pg\_%' ESCAPE '\'`
	readsUnescapedSystemExclusion = `NOT LIKE 'pg_%'`
)

// reservedPrefixClause is the filter's shape, whatever pattern it carries.
// Seeing this and recognizing no pattern is an error rather than a guess, for
// the same reason membershipPredicate refuses an unknown predicate: a third
// spelling means something this fake cannot evaluate, and answering it anyway
// would be inventing a server. Its absence is a different fact, and a real
// one -- no exclusion at all, which is what the exclusion-dropped mutant sends
// and what the reads did before the reserved names were excluded.
const reservedPrefixClause = "r.rolname NOT LIKE"

// reservedPrefixFilters pairs each spelling with the names it actually drops,
// so this fake evaluates the pattern instead of accepting either spelling as
// "the exclusion". Reverting the escape at either read therefore moves
// ordinary pg-prefixed roles out of that read rather than being waved through,
// and TestReadRolesKeepsOrdinaryRolesTheReservedPrefixWouldSwallow says so.
var reservedPrefixFilters = []struct {
	filter  string
	matches func(name string) bool
}{
	{
		filter:  readsSystemExclusion,
		matches: func(name string) bool { return strings.HasPrefix(name, "pg_") },
	},
	{
		// 'pg_%' is p, g, exactly one character of any kind, then anything.
		filter:  readsUnescapedSystemExclusion,
		matches: func(name string) bool { return len(name) >= 3 && strings.HasPrefix(name, "pg") },
	},
}

// reservedPrefixMatcher returns which names the query's reserved-prefix filter
// drops, resolved once per statement because it is a property of the statement
// rather than of a role.
func reservedPrefixMatcher(stripped string) (func(name string) bool, error) {
	for _, reserved := range reservedPrefixFilters {
		if strings.Contains(stripped, reserved.filter) {
			return reserved.matches, nil
		}
	}
	if strings.Contains(stripped, reservedPrefixClause) {
		return nil, fmt.Errorf(
			"this fake evaluates only the reserved-prefix patterns %q and %q, and the query carries "+
				"a third one. A reader that changes the pattern on purpose updates reservedPrefixFilters "+
				"and TestReadRolesKeepsOrdinaryRolesTheReservedPrefixWouldSwallow",
			readsSystemExclusion, readsUnescapedSystemExclusion,
		)
	}
	return func(string) bool { return false }, nil
}

// The two membership predicates this fake knows how to evaluate, spelled out
// here rather than read from the reader's constants: a guard that imports the
// value it is checking agrees with every mutation of it. Any other predicate
// is answered with an error, so a reader that sends a predicate meaning
// something else fails every test in this file instead of being played along
// with.
const (
	scopedMembership     = "EXISTS (SELECT 1 FROM used u WHERE u.roleoid = r.oid)"
	complementMembership = "NOT EXISTS (SELECT 1 FROM used u WHERE u.roleoid = r.oid)"
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
	reader, _ := newRecordingRolesServer(tb, cluster, schemas, caps)
	return reader
}

// newRecordingRolesServer is newRolesServer plus every statement the reader
// sent, in order, so a test can assert on the SQL itself rather than only on
// what a fake chose to answer.
func newRecordingRolesServer(
	tb interface{ Cleanup(func()) },
	cluster []clusterRole,
	schemas []string,
	caps capability.Capabilities,
) (*Reader, *[]string) {
	var sent []string
	db := dbtest.Open(tb, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		sent = append(sent, query)
		return answerRoles(query, args, cluster)
	})
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, schemas[0], caps)
	reader.SetSchemas(schemas)
	return reader, &sent
}

// answerRoles plays PostgreSQL for the roles query.
//
// Scope comes from the bound arguments rather than from the spelling of a
// WHERE clause: a query that binds no schema has asked for the whole cluster,
// which is exactly the pre-fix behavior, and the server answers accordingly.
// Which of the two reads this is comes from evaluating the membership
// predicate, not from spotting a token inside it.
func answerRoles(query string, args []driver.NamedValue, cluster []clusterRole) (dbtest.QueryResult, error) {
	stripped := stripSQLComments(query)

	membership, err := membershipPredicate(stripped)
	if err != nil {
		return dbtest.QueryResult{}, err
	}

	reserved, err := reservedPrefixMatcher(stripped)
	if err != nil {
		return dbtest.QueryResult{}, err
	}

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
		if !roleIsAnswerable(role, membership, stripped, branches, bound, reserved) {
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
//
// The complement predicate gets the exact negation of the scoped answer, over
// the same reserved-name exclusions, because that is what a server evaluating
// this predicate against the same set would return. A reader that scopes one
// of the two reads and not the other therefore shows up as a role reported
// twice or as a role reported nowhere.
func roleIsAnswerable(
	role clusterRole,
	membership, stripped string,
	branches []string,
	bound map[string]bool,
	reserved func(name string) bool,
) bool {
	if reserved(role.name) {
		return false
	}
	if role.name == "postgres" && strings.Contains(stripped, readsPostgresExcluded) {
		return false
	}
	if membership == complementMembership {
		return !roleIsUsedByScope(role, stripped, branches, bound)
	}
	return roleIsUsedByScope(role, stripped, branches, bound)
}

// membershipPredicate returns the predicate the pg_roles statement restricts
// itself with, and refuses anything this fake cannot evaluate.
//
// Refusing is the point. The two predicates are spelled out in this file, so a
// reader that sends a third thing -- a complement whose correlation was
// dropped, a predicate disjoined with TRUE, a scope that is computed and then
// not applied -- gets an error from the server rather than an answer this fake
// invented for it, and every test driving that read turns red.
func membershipPredicate(stripped string) (string, error) {
	predicate := roleStatementPredicate(stripped)
	if predicate == scopedMembership || predicate == complementMembership {
		return predicate, nil
	}
	return "", fmt.Errorf(
		"this fake evaluates only %q and its exact negation %q, and got %q. "+
			"A reader that changes the predicate on purpose updates this constant and "+
			"TestReadRolesComplementIsTheExactNegationOfTheScopedRead",
		scopedMembership, complementMembership, predicate,
	)
}

// roleStatementPredicate returns the text after WHERE in the statement that
// selects from pg_roles, or an empty string when there is none. It validates
// nothing: the caller decides what an unexpected predicate means, so a failure
// message can name the predicate that was actually sent.
func roleStatementPredicate(stripped string) string {
	inRoleStatement := false
	for line := range strings.SplitSeq(stripped, "\n") {
		text := strings.TrimSpace(line)
		if strings.HasPrefix(text, "FROM pg_roles") {
			inRoleStatement = true
			continue
		}
		if inRoleStatement && strings.HasPrefix(text, "WHERE ") {
			return strings.TrimSpace(strings.TrimPrefix(text, "WHERE "))
		}
	}
	return ""
}

// roleIsUsedByScope reports whether the query's scope restriction reaches this
// role at all.
func roleIsUsedByScope(role clusterRole, stripped string, branches []string, bound map[string]bool) bool {
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
// the schema "public", one role used only by the schema "app", and four roles
// that are used by nothing in this database.
//
// One of those four is named pgbouncer: an ordinary user role that the
// reserved-prefix filter drops if its underscore is left unescaped
// (stokaro/ptah#1291). It is in the shared fixture rather than only in its own
// test because losing it is the one shape the comparator cannot recover from
// -- dropped from BOTH reads, so neither list has it and it reads as absent.
func fullCluster() []clusterRole {
	return []clusterRole{
		{name: "app_schema_grantee", schema: "app", reads: bySchemaGrant},
		{name: "pg_reserved", schema: "public", reads: byRelationGrant},
		{name: "pgbouncer", schema: "", reads: nil},
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := newRolesServer(c.TB, test.cluster, test.schemas, capability.Postgres16())

			roles, err := reader.readRoles()

			c.Assert(err, qt.IsNil)
			c.Assert(roles, qt.HasLen, 0)
		})
	}
}

func TestReadRolesFollowsTheSchemasBeingRead(t *testing.T) {
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := newRolesServer(c.TB, fullCluster(), test.schemas, capability.Postgres16())

			roles, err := reader.readRoles()

			c.Assert(err, qt.IsNil)
			c.Assert(roleNames(roles), qt.DeepEquals, test.want)
		})
	}
}

func TestReadRolesKeepsSystemRolesOut(t *testing.T) {
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := newRolesServer(c.TB, test.cluster, []string{"public"}, capability.Postgres16())

			roles, err := reader.readRoles()

			c.Assert(err, qt.IsNil)
			c.Assert(roleNames(roles), qt.DeepEquals, []string{"table_grantee"})
			c.Assert(roleNames(roles), qt.Not(qt.Contains), test.absent)
		})
	}
}

func TestReadRolesAsksForPolicyRolesOnlyWherePoliciesExist(t *testing.T) {
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := newRolesServer(c.TB, cluster, []string{"public"}, test.caps)

			roles, err := reader.readRoles()

			c.Assert(err, qt.IsNil)
			c.Assert(roleNames(roles), qt.DeepEquals, test.want)
		})
	}
}

func TestReadRolesDoesNotTreatOwnershipAsUse(t *testing.T) {
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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

func TestReadRolesOutOfScopeReportsWhatTheDescriptionLeavesOut(t *testing.T) {
	// Same server, three scopes, and the answer readRoles does not give. This
	// is what tells a comparator that a role missing from the description
	// still exists on the server, so it plans no CREATE ROLE for it. See the
	// review thread on stokaro/ptah#1273 and requirement 2 of
	// stokaro/ptah#1276.
	tests := []struct {
		name    string
		schemas []string
		want    []string
	}{
		{
			name:    "public only",
			schemas: []string{"public"},
			want: []string{
				"app_schema_grantee", "pgbouncer", "someone_elses", "third_party",
				"unrelated_tenant",
			},
		},
		{
			name:    "app only",
			schemas: []string{"app"},
			want: []string{
				"pgbouncer", "policy_named", "schema_grantee", "schema_grantor",
				"someone_elses", "table_grantee", "table_grantor", "third_party",
				"unrelated_tenant",
			},
		},
		{
			name:    "both schemas",
			schemas: []string{"public", "app"},
			want: []string{
				"pgbouncer", "someone_elses", "third_party", "unrelated_tenant",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := newRolesServer(c.TB, fullCluster(), test.schemas, capability.Postgres16())

			roles, err := reader.readRolesOutOfScope()

			c.Assert(err, qt.IsNil)
			c.Assert(roleNames(roles), qt.DeepEquals, test.want)
		})
	}
}

func TestReadRolesOutOfScopeKeepsSystemRolesOut(t *testing.T) {
	// The complement is the complement of the described set, not of the whole
	// catalog: the reserved pg_ roles and the bootstrap superuser are excluded
	// from both reads. Reporting them here would hand the comparator roles it
	// must never manage.
	tests := []struct {
		name    string
		absent  string
		cluster []clusterRole
	}{
		{
			name:   "reserved role used by nothing in scope",
			absent: "pg_reserved",
			cluster: []clusterRole{
				{name: "pg_reserved", schema: "", reads: nil},
				{name: "someone_elses", schema: "", reads: nil},
			},
		},
		{
			name:   "bootstrap superuser used by nothing in scope",
			absent: "postgres",
			cluster: []clusterRole{
				{name: "postgres", schema: "", reads: nil},
				{name: "someone_elses", schema: "", reads: nil},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := newRolesServer(c.TB, test.cluster, []string{"public"}, capability.Postgres16())

			roles, err := reader.readRolesOutOfScope()

			c.Assert(err, qt.IsNil)
			c.Assert(roleNames(roles), qt.DeepEquals, []string{"someone_elses"})
			c.Assert(roleNames(roles), qt.Not(qt.Contains), test.absent)
		})
	}
}

func TestReadRolesKeepsOrdinaryRolesTheReservedPrefixWouldSwallow(t *testing.T) {
	// PostgreSQL reserves the prefix WITH the underscore, and LIKE reads a
	// bare underscore as a single-character wildcard, so `NOT LIKE 'pg_%'`
	// also drops pgbouncer, pgadmin, pgpool and pguser (stokaro/ptah#1291).
	// Both reads here carry that filter, so an unescaped one drops such a role
	// from the description AND from the complement -- the one shape this
	// change cannot recover from, because a role in neither list is a role the
	// comparator reads as absent and plans CREATE ROLE for, which is exactly
	// the SQLSTATE 42710 failure the complement exists to prevent.
	//
	// The reserved role sits in each fixture as the control: escaping the
	// underscore must not stop pg_reserved being excluded.
	tests := []struct {
		name    string
		cluster []clusterRole
		read    func(*Reader) ([]types.DBRole, error)
	}{
		{
			name: "the scoped read describes it when the scope uses it",
			cluster: []clusterRole{
				{name: "pg_reserved", schema: "public", reads: byRelationGrant},
				{name: "pgbouncer", schema: "public", reads: byRelationGrant},
			},
			read: (*Reader).readRoles,
		},
		{
			name: "the complement reports it when the scope does not",
			cluster: []clusterRole{
				{name: "pg_reserved", schema: "", reads: nil},
				{name: "pgbouncer", schema: "", reads: nil},
			},
			read: (*Reader).readRolesOutOfScope,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := newRolesServer(c.TB, test.cluster, []string{"public"}, capability.Postgres16())

			roles, err := test.read(reader)

			c.Assert(err, qt.IsNil)
			c.Assert(roleNames(roles), qt.DeepEquals, []string{"pgbouncer"})
		})
	}
}

func TestReadRolesPartitionsEveryManageableRole(t *testing.T) {
	// The property the comparator depends on, stated with the qualifier it
	// actually has: whatever the scoping rule decides, every role Ptah manages
	// lands in exactly one of the two reads. A managed role can therefore never
	// look absent merely because it was out of scope -- the failure this fix is
	// for, and one that no assertion about the described list alone can catch.
	//
	// It is NOT a partition of pg_roles, and the fixture proves that rather
	// than footnoting it: fullCluster holds pg_reserved and postgres, both
	// reads exclude them, and the union below is asserted to be the manageable
	// names exactly. A desired schema naming a reserved role would therefore be
	// compared against nothing, which is why such a schema is refused before it
	// reaches the comparator -- see
	// compare.TestRolesReservedNameIsRefusedBeforeThisComparisonRunsAtAll and
	// go.5x5.cz/ptah/internal/reservedrole.
	//
	// This test alone cannot catch a broken complement predicate: the fake
	// answers the complement read by negating its own scoped answer. What
	// stops that being circular is that the fake refuses any predicate other
	// than the two it spells out, plus
	// TestReadRolesComplementIsTheExactNegationOfTheScopedRead.
	tests := []struct {
		name    string
		schemas []string
	}{
		{name: "public only", schemas: []string{"public"}},
		{name: "app only", schemas: []string{"app"}},
		{name: "both schemas", schemas: []string{"public", "app"}},
		{name: "a schema nothing uses", schemas: []string{"empty"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := newRolesServer(c.TB, fullCluster(), test.schemas, capability.Postgres16())

			described, err := reader.readRoles()
			c.Assert(err, qt.IsNil)
			outOfScope, err := reader.readRolesOutOfScope()
			c.Assert(err, qt.IsNil)

			union := append(roleNames(described), roleNames(outOfScope)...)
			slices.Sort(union)

			c.Assert(union, qt.DeepEquals, manageableClusterRoleNames())
			c.Assert(slices.Compact(slices.Clone(union)), qt.DeepEquals, union,
				qt.Commentf("a role was reported by both reads"))
			c.Assert(union, qt.Not(qt.Contains), "postgres",
				qt.Commentf("the union is the manageable roles, not every role the server has"))
			c.Assert(union, qt.Not(qt.Contains), "pg_reserved",
				qt.Commentf("the union is the manageable roles, not every role the server has"))
		})
	}
}

func TestReadRolesComplementIsTheExactNegationOfTheScopedRead(t *testing.T) {
	c := qt.New(t)

	// The partition above is a property of two answers; this is the property
	// of the two statements, which is where the defect would live. Both
	// assertions were written against measured escapes: with
	// `u.roleoid = u.roleoid` the complement is empty whenever anything is in
	// scope -- the whole defect restored -- and with
	// `TRUE OR NOT EXISTS (...)` the two reads overlap. Both walked past every
	// other test in this file before the reads were compared as text.
	reader, sent := newRecordingRolesServer(c.TB, fullCluster(), []string{"public"}, capability.Postgres16())

	_, err := reader.readRoles()
	c.Assert(err, qt.IsNil)
	_, err = reader.readRolesOutOfScope()
	c.Assert(err, qt.IsNil)
	c.Assert(*sent, qt.HasLen, 2)

	scoped := stripSQLComments((*sent)[0])
	complement := stripSQLComments((*sent)[1])
	scopedPredicate := roleStatementPredicate(scoped)
	complementPredicate := roleStatementPredicate(complement)

	c.Assert(scopedPredicate, qt.Not(qt.Equals), "",
		qt.Commentf("the scoped read must restrict pg_roles with a predicate"))
	c.Assert(scopedPredicate, qt.Contains, "u.roleoid = r.oid",
		qt.Commentf("the used set has to be correlated with the role row, or it restricts nothing"))
	c.Assert(complementPredicate, qt.Equals, "NOT "+scopedPredicate,
		qt.Commentf("the complement must be the scoped predicate negated, and nothing else"))
	c.Assert(complement, qt.Equals,
		strings.Replace(scoped, "WHERE "+scopedPredicate, "WHERE "+complementPredicate, 1),
		qt.Commentf("the two reads must differ by that NOT alone, or they are not complements"))
}

func TestReadRolesIntoScopesTheDescriptionByDefault(t *testing.T) {
	c := qt.New(t)

	// The default: the description is the scoped read, and the roles it leaves
	// out are carried separately for the comparator alone. This is the shape
	// every surface produces unless an operator asks for the other one.
	envbooltest.Unset(rolescope.DescribeAllEnvVar)(c)
	reader := newRolesServer(c.TB, fullCluster(), []string{"public"}, capability.Postgres16())
	schema := &types.DBSchema{}

	c.Assert(reader.readRolesInto(schema), qt.IsNil)

	c.Assert(roleNames(schema.Roles), qt.DeepEquals, []string{
		"policy_named", "schema_grantee", "schema_grantor", "table_grantee", "table_grantor",
	})
	c.Assert(roleNames(schema.RolesOutOfScope), qt.DeepEquals, []string{
		"app_schema_grantee", "pgbouncer", "someone_elses", "third_party", "unrelated_tenant",
	})
}

// TestReadRolesIntoRefusesAMalformedOptIn pins the state split stokaro/ptah#1334
// introduced, and pins it on the run that used to hide it.
//
// The `--schemas empty` row is the discriminating one: the scoped read and the
// complement partition the same cluster either way, so on a schema nothing uses
// the opt-in changes which list the roles land in but never which roles exist.
// A read resolved beside the branch would pass there in silence. Resolving it
// before the two queries makes the typo answer on every read.
//
// The schema is asserted untouched in both rows: a refusal that had already
// written half a description would be a worse answer than the silence it
// replaces.
func TestReadRolesIntoRefusesAMalformedOptIn(t *testing.T) {
	tests := []struct {
		name        string
		env         func(testing.TB)
		schemas     []string
		wantMessage string
	}{
		{
			name:        "an unparsable value",
			env:         envbooltest.Set(rolescope.DescribeAllEnvVar, "all of them"),
			schemas:     []string{"public"},
			wantMessage: `invalid boolean value "all of them" for PTAH_POSTGRES_INSPECT_ALL_ROLES`,
		},
		{
			name:        "an exported empty value",
			env:         envbooltest.Set(rolescope.DescribeAllEnvVar, ""),
			schemas:     []string{"public"},
			wantMessage: `invalid boolean value "" for PTAH_POSTGRES_INSPECT_ALL_ROLES`,
		},
		{
			name:        "a scope where the opt-in would change nothing",
			env:         envbooltest.Set(rolescope.DescribeAllEnvVar, "maybe"),
			schemas:     []string{"empty"},
			wantMessage: `invalid boolean value "maybe" for PTAH_POSTGRES_INSPECT_ALL_ROLES`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.env(c)
			reader := newRolesServer(c.TB, fullCluster(), test.schemas, capability.Postgres16())
			schema := &types.DBSchema{}

			err := reader.readRolesInto(schema)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, test.wantMessage)
			c.Assert(schema.Roles, qt.HasLen, 0)
			c.Assert(schema.RolesOutOfScope, qt.HasLen, 0)
		})
	}
}

func TestReadRolesIntoDescribesEveryManagedRoleUnderTheOptIn(t *testing.T) {
	c := qt.New(t)

	// The capability scoping the description took away, put back on the same
	// surface (AGENTS.md, "Compatibility never removes a capability").
	// Measured on PostgreSQL 17.10 across two clusters: a database holding one
	// table and one ungranted cluster role is described with 4 CREATE ROLE
	// before stokaro/ptah#1267 and 0 after, and `ptah-compat schema apply
	// --dry-run` against an empty database in a second cluster plans those
	// roles before and not after. With this variable set, both numbers come
	// back.
	//
	// Reserved names do not come back, and that is the point of asserting
	// against manageableClusterRoleNames rather than against fullCluster: the
	// opt-in widens the description to every role Ptah MANAGES, and Ptah
	// manages neither the pg_ names nor the bootstrap superuser in either
	// direction. An opt-in that emitted `CREATE ROLE "postgres"` would be a
	// worse answer than the scoping it undoes.
	c.Setenv(rolescope.DescribeAllEnvVar, "1")
	reader := newRolesServer(c.TB, fullCluster(), []string{"public"}, capability.Postgres16())
	schema := &types.DBSchema{}

	c.Assert(reader.readRolesInto(schema), qt.IsNil)

	c.Assert(roleNames(schema.Roles), qt.DeepEquals, manageableClusterRoleNames())
	c.Assert(schema.RolesOutOfScope, qt.HasLen, 0,
		qt.Commentf("nothing was left out, so nothing may be reported as left out"))
}

func TestReadRolesIntoLeavesTheComparatorsAnswerAlone(t *testing.T) {
	// The safety property of the opt-in, stated on its own because it is the
	// reason the variable can exist at all: both reads run either way, so the
	// UNION -- which is what compare.Roles takes existence from -- is the same
	// set under both settings. Turning the variable on moves roles between the
	// two lists and can never make Ptah plan a CREATE ROLE for a role that is
	// already there, which is the failure stokaro/ptah#1276 is about.
	tests := []struct {
		name    string
		env     func(testing.TB)
		schemas []string
	}{
		{
			name:    "default, public",
			env:     envbooltest.Unset(rolescope.DescribeAllEnvVar),
			schemas: []string{"public"},
		},
		{
			name:    "opt-in, public",
			env:     envbooltest.Set(rolescope.DescribeAllEnvVar, "1"),
			schemas: []string{"public"},
		},
		{
			name:    "default, a schema nothing uses",
			env:     envbooltest.Unset(rolescope.DescribeAllEnvVar),
			schemas: []string{"empty"},
		},
		{
			name:    "opt-in, a schema nothing uses",
			env:     envbooltest.Set(rolescope.DescribeAllEnvVar, "1"),
			schemas: []string{"empty"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.env(c)
			reader := newRolesServer(c.TB, fullCluster(), test.schemas, capability.Postgres16())
			schema := &types.DBSchema{}

			c.Assert(reader.readRolesInto(schema), qt.IsNil)

			union := append(roleNames(schema.Roles), roleNames(schema.RolesOutOfScope)...)
			slices.Sort(union)
			c.Assert(union, qt.DeepEquals, manageableClusterRoleNames())
			c.Assert(slices.Compact(slices.Clone(union)), qt.DeepEquals, union,
				qt.Commentf("a role was reported by both lists"))
		})
	}
}

// manageableClusterRoleNames is every role fullCluster holds that either read
// is allowed to report. The reserved pg_ names and the bootstrap superuser are
// excluded from both reads, so they are in neither list and the union of the
// two reads is smaller than the cluster's role set.
func manageableClusterRoleNames() []string {
	names := make([]string, 0, len(fullCluster()))
	for _, role := range fullCluster() {
		if strings.HasPrefix(role.name, "pg_") || role.name == "postgres" {
			continue
		}
		names = append(names, role.name)
	}
	slices.Sort(names)
	return names
}
