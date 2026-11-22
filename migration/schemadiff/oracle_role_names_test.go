package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

// heldRoleGrant is a read of the table, the roles named, and a SELECT on the
// table held by role, all in their stored spelling.
func heldRoleGrant(table, role string, roles ...string) *catalog.Database {
	read := &catalog.Database{StoredNames: true, Tables: []catalog.Table{heldTable(table)}}
	for _, name := range roles {
		read.Roles = append(read.Roles, catalog.Role{Name: name})
	}
	read.Grants = []catalog.Grant{{Role: role, Privilege: "SELECT", ObjectType: "TABLE", Schema: "APP", ObjectName: table}}
	return read
}

// heldOptionGrant is heldRoleGrant with the grant held WITH GRANT OPTION.
func heldOptionGrant(table, role string, roles ...string) *catalog.Database {
	read := heldRoleGrant(table, role, roles...)
	read.Grants[0].WithOption = true
	return read
}

// grantTargets names each grant as role, privilege and object.
func grantTargets(refs []difftypes.GrantRef) []string {
	var targets []string
	for _, ref := range refs {
		targets = append(targets, ref.Role+" "+ref.Privilege+" "+ref.ObjectName)
	}
	return targets
}

// roleNames lists the names of roles.
func roleNames(roles []schemamodel.Role) []string {
	var names []string
	for _, role := range roles {
		names = append(names, role.Name)
	}
	return names
}

// TestCompareWithDatabaseInfo_OracleRoleNames matches a role and a grant the
// way Oracle stores their names: a bare reader is the role READER, a quoted
// "reader" is the role reader, and a grant read on a table created quoted is
// a grant on that table. A revoke planned from the read names the role and the
// table it was read from, quoted where no bare name reaches them.
func TestCompareWithDatabaseInfo_OracleRoleNames(t *testing.T) {
	tests := []struct {
		name           string
		declared       string
		read           *catalog.Database
		rolesAdded     []string
		added          []string
		removed        []string
		optionsRevoked []string
	}{
		{
			name:     "a bare role is the role stored in upper case",
			declared: "CREATE TABLE keep_me (id NUMBER PRIMARY KEY); CREATE ROLE reader; GRANT SELECT ON keep_me TO reader;",
			read:     heldRoleGrant("KEEP_ME", "READER", "READER"),
		},
		{
			name:     "a bare role the schema does not create is the role stored in upper case",
			declared: "CREATE TABLE keep_me (id NUMBER PRIMARY KEY); GRANT SELECT ON keep_me TO reader;",
			read:     heldRoleGrant("KEEP_ME", "READER", "READER"),
		},
		{
			name:           "a grant option a bare role holds and the schema does not declare is taken back",
			declared:       "CREATE TABLE keep_me (id NUMBER PRIMARY KEY); CREATE ROLE reader; GRANT SELECT ON keep_me TO reader;",
			read:           heldOptionGrant("KEEP_ME", "READER", "READER"),
			optionsRevoked: []string{"READER SELECT APP.KEEP_ME"},
		},
		{
			name:     "a quoted role is the role stored as written",
			declared: `CREATE TABLE keep_me (id NUMBER PRIMARY KEY); CREATE ROLE "reader"; GRANT SELECT ON keep_me TO "reader";`,
			read:     heldRoleGrant("KEEP_ME", "reader", "reader"),
		},
		{
			name:       "a bare role is not the role created quoted",
			declared:   "CREATE TABLE keep_me (id NUMBER PRIMARY KEY); CREATE ROLE reader; GRANT SELECT ON keep_me TO reader;",
			read:       heldRoleGrant("KEEP_ME", "reader", "reader"),
			rolesAdded: []string{"reader"},
			added:      []string{"reader SELECT keep_me"},
		},
		{
			name:     "a grant on a quoted table is the grant read on it",
			declared: `CREATE TABLE "orders" (id NUMBER PRIMARY KEY); CREATE ROLE READER; GRANT SELECT ON "orders" TO READER;`,
			read:     heldRoleGrant("orders", "READER", "READER"),
		},
		{
			name:     "a revoke names the quoted role and table it was read from",
			declared: `CREATE TABLE "orders" (id NUMBER PRIMARY KEY); CREATE ROLE "reader";`,
			read:     heldRoleGrant("orders", "reader", "reader"),
			removed:  []string{`"reader" SELECT APP."""orders"""`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, _, err := sqlschema.ReadWith(c.Context(), []byte(test.declared),
				platform.Oracle, nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.IsNil)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), &desired, test.read,
				oracleRead(), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(roleNames(diff.RolesAdded), qt.DeepEquals, test.rolesAdded)
			c.Assert(grantTargets(diff.GrantsAdded), qt.DeepEquals, test.added)
			c.Assert(grantTargets(diff.GrantsRemoved), qt.DeepEquals, test.removed)
			c.Assert(grantTargets(diff.GrantOptionsRevoked), qt.DeepEquals, test.optionsRevoked)
			c.Assert(diff.TablesAdded.Names(), qt.HasLen, 0)
		})
	}
}
