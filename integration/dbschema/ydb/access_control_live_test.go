//go:build integration

package ydb_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// accessSchema is the directory the access-control tests write into. It sits
// at the database root, so on 25.1 a grant on it is named by its absolute
// path, and a table in it by a relative one.
const accessSchema = "ptah_ydb_access"

var accessSchemas = []string{accessSchema}

// accessNames are the principals one run creates. Users and groups belong to
// the database rather than to a directory, so each run takes names of its own
// and removes them, whatever the run ended with.
type accessNames struct {
	group, user, blocked string
}

// newAccessNames returns names unique to this run: YDB takes lower-case
// letters and digits only.
func newAccessNames(c *qt.C) accessNames {
	c.Helper()
	raw := make([]byte, 4)
	_, err := rand.Read(raw)
	c.Assert(err, qt.IsNil)
	suffix := hex.EncodeToString(raw)
	return accessNames{group: "ptahaccg" + suffix, user: "ptahaccu" + suffix, blocked: "ptahaccb" + suffix}
}

// all is every principal of the run.
func (n accessNames) all() []string { return []string{n.group, n.user, n.blocked} }

// removeAccess revokes everything the run's principals hold on the database
// and the directory, and drops them: DROP USER and DROP GROUP leave a
// principal's permission entries behind, and the next run must not inherit
// them. The table's entries go with the table.
func removeAccess(c *qt.C, conn *dbschema.DatabaseConnection, names accessNames) {
	c.Helper()
	// A cleanup runs after the test's context is done, so the read takes one
	// of its own.
	ctx := context.Background()
	read, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, accessSchemas)
	c.Assert(err, qt.IsNil)
	database := read.DatabasePath
	for _, principal := range names.all() {
		for _, object := range []string{database, database + "/" + accessSchema} {
			// The directory may not exist yet; the revoke has nothing to take then.
			_ = conn.Writer().ExecuteSQL(ctx, "REVOKE ALL ON "+sqlident.Quote("ydb", object)+" FROM "+principal)
		}
	}
	for _, statement := range []string{"DROP USER IF EXISTS " + names.user, "DROP USER IF EXISTS " + names.blocked,
		"DROP GROUP IF EXISTS " + names.group} {
		c.Assert(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("execute: %s", statement))
	}
}

// accessDeclaration is a table in the directory, a group, a user that logs in
// and is a member of userGroups, a blocked user in the group, and grants on the
// table, the directory and the database. userGroups and blockedLogin vary the
// parts a change tests.
func accessDeclaration(names accessNames, userGroups []string, blockedLogin bool, tablePrivileges ...string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders", Schema: accessSchema}},
		Fields: []schemamodel.Field{
			{StructName: "Order", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Order", Name: "note", Type: "TEXT", Nullable: true},
		},
		Roles: []schemamodel.Role{
			{Name: names.group, Group: true, Inherit: true},
			{Name: names.user, Login: true, Password: "Secret1!", Inherit: true, MemberOf: userGroups},
			{Name: names.blocked, Login: blockedLogin, Inherit: true, MemberOf: []string{names.group}},
		},
		Grants: []schemamodel.Grant{
			{Role: names.group, Privileges: tablePrivileges, OnTable: accessSchema + ".orders"},
			{Role: names.group, Privileges: []string{"LIST"}, OnSchema: accessSchema},
			{Role: names.user, Privileges: []string{"CONNECT"}, OnDatabase: true},
		},
	}
	schemamodel.Finalize(db)
	return db
}

// principalsOf keeps the roles, memberships and grants of a read that name the
// run's principals.
func principalsOf(live *catalog.Database, names accessNames) ([]catalog.Role, []catalog.RoleMembership, []catalog.Grant) {
	ours := func(name string) bool { return slices.Contains(names.all(), name) }
	var roles []catalog.Role
	for _, role := range live.Roles {
		if ours(role.Name) {
			roles = append(roles, role)
		}
	}
	var memberships []catalog.RoleMembership
	for _, membership := range live.RoleMemberships {
		if ours(membership.Member) && membership.Role != "USERS" {
			memberships = append(memberships, membership)
		}
	}
	var grants []catalog.Grant
	for _, grant := range live.Grants {
		if ours(grant.Role) {
			grants = append(grants, grant)
		}
	}
	slices.SortFunc(grants, func(a, b catalog.Grant) int {
		return strings.Compare(a.ObjectType+"|"+a.ObjectName+"|"+a.Role+"|"+a.Privilege,
			b.ObjectType+"|"+b.ObjectName+"|"+b.Role+"|"+b.Privilege)
	})
	return roles, memberships, grants
}

// TestYDBAccessControl_RoundTrip applies a declaration of users, a group,
// memberships and grants on a table, a directory and the database, reads it
// back as declared, and plans nothing after; the same declaration applied again
// plans nothing too. It then changes a membership, a user's login and a grant
// in place, and ends with nothing left to plan. A password is applied and never
// read back.
func TestYDBAccessControl_RoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			names := newAccessNames(c)
			dropTables(c, conn, accessSchemas)
			c.Cleanup(func() {
				dropTables(c, conn, accessSchemas)
				removeAccess(c, conn, names)
			})

			declared := accessDeclaration(names, []string{"DATA-READERS", names.group}, false, "SELECT ROW", "ydb.generic.list")
			apply(c, conn, planAgainst(c, conn, declared, accessSchemas))
			c.Assert(planAgainst(c, conn, declared, accessSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, accessSchemas))
			c.Assert(planAgainst(c, conn, declared, accessSchemas), qt.HasLen, 0)

			roles, memberships, grants := principalsOf(readScoped(c, conn, accessSchemas), names)
			c.Assert(roles, qt.DeepEquals, []catalog.Role{
				{Name: names.blocked, Inherit: true},
				{Name: names.group, Inherit: true, Group: true},
				{Name: names.user, Login: true, Inherit: true},
			})
			c.Assert(memberships, qt.DeepEquals, []catalog.RoleMembership{
				{Role: "DATA-READERS", Member: names.user},
				{Role: names.group, Member: names.blocked},
				{Role: names.group, Member: names.user},
			})
			c.Assert(grants, qt.DeepEquals, []catalog.Grant{
				{Role: names.user, Privilege: "ydb.database.connect", ObjectType: "DATABASE"},
				{Role: names.group, Privilege: "ydb.generic.list", ObjectType: "SCHEMA", ObjectName: accessSchema},
				{Role: names.group, Privilege: "ydb.generic.list", ObjectType: "TABLE", Schema: accessSchema, ObjectName: "orders"},
				{Role: names.group, Privilege: "ydb.granular.select_row", ObjectType: "TABLE", Schema: accessSchema, ObjectName: "orders"},
			})

			// In place: the user leaves the group, the blocked user may log in,
			// and the group's table grant loses one permission and gains another.
			changed := accessDeclaration(names, []string{"DATA-READERS"}, true, "SELECT ROW", "UPDATE ROW")
			changes := planAgainst(c, conn, changed, accessSchemas)
			c.Assert(slices.Sorted(slices.Values(changes)), qt.DeepEquals, []string{
				"ALTER GROUP `" + names.group + "` DROP USER `" + names.user + "`",
				"ALTER USER `" + names.blocked + "` LOGIN",
				"GRANT 'ydb.granular.update_row' ON `" + accessSchema + "/orders` TO `" + names.group + "`",
				"REVOKE 'ydb.generic.list' ON `" + accessSchema + "/orders` FROM `" + names.group + "`",
			})
			apply(c, conn, changes)
			c.Assert(planAgainst(c, conn, changed, accessSchemas), qt.HasLen, 0)
		})
	}
}

// TestYDBAccessControl_Rollback plans a declaration and its rollback through
// the generator, applies both, and finds every principal, membership and grant
// the forward plan made gone: the rollback revokes a principal's entries
// before it drops the principal, because DROP USER leaves them behind.
func TestYDBAccessControl_Rollback(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			names := newAccessNames(c)
			dropTables(c, conn, accessSchemas)
			c.Cleanup(func() {
				dropTables(c, conn, accessSchemas)
				removeAccess(c, conn, names)
			})
			info := conn.Info()
			current := readScoped(c, conn, accessSchemas)
			declared := accessDeclaration(names, []string{"DATA-READERS", names.group}, false, "SELECT ROW")
			diff, err := schemadiff.CompareWithDatabaseInfo(declared, current, info, nil)
			c.Assert(err, qt.IsNil)

			plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
				Diff: diff, DesiredSchema: declared, CurrentSchema: current,
				Dialect: info.Dialect, Capabilities: info.Capabilities,
			})
			c.Assert(err, qt.IsNil)
			forward, err := renderer.RenderSQLWithCapabilities(info.Dialect, info.Capabilities, plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			reverse, err := renderer.RenderSQLWithCapabilities(info.Dialect, info.Capabilities, plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)

			applyScript(c, conn, forward)
			c.Assert(planAgainst(c, conn, declared, accessSchemas), qt.HasLen, 0)
			applyScript(c, conn, reverse)

			roles, memberships, grants := principalsOf(readScoped(c, conn, accessSchemas), names)
			c.Assert(roles, qt.HasLen, 0)
			c.Assert(memberships, qt.HasLen, 0)
			c.Assert(grants, qt.HasLen, 0)
			// Every entry the principals held, on every object of the database:
			// a principal dropped with entries left would hand them to the
			// next principal of its name.
			var entries int64
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM `"+current.DatabasePath+
				"/.sys/auth_permissions` WHERE Sid IN ('"+strings.Join(names.all(), "'u, '")+"'u)").Scan(&entries), qt.IsNil)
			c.Assert(entries, qt.Equals, int64(0))
		})
	}
}

// applyScript runs a rendered script through the writer's YQL splitter, which
// keeps compound streaming-query bodies and their internal semicolons intact.
func applyScript(c *qt.C, conn *dbschema.DatabaseConnection, script string) {
	c.Helper()
	c.Assert(conn.SchemaWriter().ExecuteSQL(c.Context(), script), qt.IsNil, qt.Commentf("%s", script))
}

// TestYDBReader_LeavesTheDatabaseOwnerOutOfTheDescription reads a database
// owned by one of its users, local-ydb's root, and finds the owner known to
// exist and left out of the description. A description is what a replay
// creates, and the owner exists before any migration runs: a checkpoint that
// described root failed its replay at `CREATE USER root`.
func TestYDBReader_LeavesTheDatabaseOwnerOutOfTheDescription(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			names := func(roles []catalog.Role) []string {
				var out []string
				for _, role := range roles {
					out = append(out, role.Name)
				}
				return out
			}

			read := readScoped(c, openYDB(c, line), accessSchemas)

			c.Assert(read.ObjectOwners, qt.Contains, catalog.ObjectOwner{Kind: "database", Owner: "root", OwnerCanLogin: true})
			c.Assert(names(read.Roles), qt.Not(qt.Contains), "root")
			c.Assert(names(read.RolesOutOfScope), qt.Contains, "root")
		})
	}
}
