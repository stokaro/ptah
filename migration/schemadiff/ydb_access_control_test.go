package schemadiff_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// ydbAccessDeclaration declares a group, a user in it and in a group the
// cluster made, and grants spelled as keywords and as permission names on a
// table, a directory and the database.
func ydbAccessDeclaration() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders", Schema: "shop"}},
		Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "BIGINT", Primary: true}},
		Roles: []schemamodel.Role{
			{Name: "readers", Group: true, Inherit: true},
			{Name: "app", Login: true, Password: "Secret1!", Inherit: true, MemberOf: []string{"readers", "DATA-READERS"}},
		},
		Grants: []schemamodel.Grant{
			{Role: "readers", Privileges: []string{"SELECT ROW", "ydb.generic.list"}, OnTable: "shop.orders"},
			{Role: "readers", Privileges: []string{"LIST"}, OnSchema: "shop"},
			{Role: "app", Privileges: []string{"CONNECT"}, OnDatabase: true},
		},
	}
}

// ydbAccessCatalog is the database as the YDB reader reports it once a plan
// applied the declaration: the password unknown, the groups the cluster made
// and its own users beside the declared principals, every new user in USERS,
// and each permission under its name.
func ydbAccessCatalog() *catalog.Database {
	return &catalog.Database{
		FeatureCoverage: completeYDBFixtureCoverage(),
		Tables: []catalog.Table{{Schema: "shop", Name: "orders", Type: "TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
		}}},
		Constraints: []catalog.Constraint{{Name: "orders_pkey", Schema: "shop", TableName: "orders", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
		Roles: []catalog.Role{
			{Name: "app", Login: true, Inherit: true, PasswordState: catalog.RolePasswordUnknown},
			{Name: "readers", Inherit: true, Group: true},
			{Name: "root", Login: true, Inherit: true, PasswordState: catalog.RolePasswordUnknown},
		},
		RolesOutOfScope: []catalog.Role{{Name: "USERS", Inherit: true, Group: true}, {Name: "DATA-READERS", Inherit: true, Group: true}},
		RoleMemberships: []catalog.RoleMembership{
			{Role: "readers", Member: "app"},
			{Role: "DATA-READERS", Member: "app"},
			{Role: "USERS", Member: "app"},
			{Role: "USERS", Member: "root"},
		},
		Grants: []catalog.Grant{
			{Role: "readers", Privilege: "ydb.granular.select_row", ObjectType: "TABLE", Schema: "shop", ObjectName: "orders"},
			{Role: "readers", Privilege: "ydb.generic.list", ObjectType: "TABLE", Schema: "shop", ObjectName: "orders"},
			{Role: "readers", Privilege: "ydb.generic.list", ObjectType: "SCHEMA", ObjectName: "shop"},
			{Role: "app", Privilege: "ydb.database.connect", ObjectType: "DATABASE"},
			{Role: "USERS", Privilege: "ydb.database.connect", ObjectType: "DATABASE"},
		},
		DatabasePath: "/local",
	}
}

// TestCompare_YDBAccessControl_PlansNothingOnceApplied reads the declaration
// against the database a plan built and against itself: neither plans
// anything. Keywords and permission names are one permission, the cluster's
// groups and their members are the server's, and an unknown password is not a
// change.
func TestCompare_YDBAccessControl_PlansNothingOnceApplied(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "against the database", diff: must.Must(schemadiff.CompareWithDialect(t.Context(), ydbAccessDeclaration(), ydbAccessCatalog(), platform.YDB, must.Must(builtin.New())))},
		{name: "against the same document", diff: must.Must(schemadiff.CompareSchemas(t.Context(), ydbAccessDeclaration(), ydbAccessDeclaration(), platform.YDB, must.Must(builtin.New())))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.diff.HasChanges(), qt.IsFalse)
		})
	}
}

// TestCompare_YDBAccessControl_PlansTheDifference plans what differs: a
// membership and a permission the database lacks, the membership and the
// permission a declared principal holds and the declaration does not, and a
// kind that changed. A membership in a group the schema does not declare is
// left alone, USERS included, which every new user joins.
func TestCompare_YDBAccessControl_PlansTheDifference(t *testing.T) {
	c := qt.New(t)
	declaration := ydbAccessDeclaration()
	declaration.Roles = append(declaration.Roles, schemamodel.Role{Name: "auditors", Group: true, Inherit: true})
	database := ydbAccessCatalog()
	database.Roles = append(database.Roles, catalog.Role{Name: "auditors", Inherit: true})
	database.RoleMemberships = append(slices.DeleteFunc(database.RoleMemberships, func(membership catalog.RoleMembership) bool {
		return membership == catalog.RoleMembership{Role: "readers", Member: "app"}
	}),
		catalog.RoleMembership{Role: "auditors", Member: "readers"},
		catalog.RoleMembership{Role: "ops", Member: "app"})
	database.Grants = append(database.Grants,
		catalog.Grant{Role: "app", Privilege: "ydb.granular.erase_row", ObjectType: "TABLE", Schema: "shop", ObjectName: "orders"},
		catalog.Grant{Role: "someone", Privilege: "ydb.granular.erase_row", ObjectType: "TABLE", Schema: "shop", ObjectName: "orders"})
	database.Grants = database.Grants[1:]

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), declaration, database, platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.RoleMembershipsAdded, qt.DeepEquals, []difftypes.RoleMembershipRef{{Role: "readers", Member: "app"}})
	c.Assert(diff.RoleMembershipsRemoved, qt.DeepEquals, []difftypes.RoleMembershipRef{{Role: "auditors", Member: "readers"}})
	c.Assert(diff.GrantsAdded, qt.DeepEquals, []difftypes.GrantRef{
		{Role: "readers", Privilege: "YDB.GRANULAR.SELECT_ROW", ObjectType: "TABLE", ObjectName: "shop.orders"},
	})
	c.Assert(diff.GrantsRemoved, qt.DeepEquals, []difftypes.GrantRef{
		{Role: "app", Privilege: "YDB.GRANULAR.ERASE_ROW", ObjectType: "TABLE", ObjectName: "shop.orders"},
	})
	c.Assert(diff.RolesModified, qt.HasLen, 1)
	c.Assert(diff.RolesModified[0].RoleName, qt.Equals, "auditors")
	c.Assert(diff.RolesModified[0].Changes, qt.DeepEquals, map[string]string{"group": "false -> true"})
	c.Assert(diff.CurrentDatabasePath, qt.Equals, "/local")
}

// TestCompare_RoleMembershipsNeedTheKey compares a declared membership on a
// target without role_membership, where no planner plans one: nothing is
// recorded, so nothing reaches a planner that would plan nothing for it. The
// declaration gate refuses such a schema before a comparison is asked for.
func TestCompare_RoleMembershipsNeedTheKey(t *testing.T) {
	c := qt.New(t)
	declaration := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", Inherit: true, MemberOf: []string{"readers"}}}}
	database := &catalog.Database{
		Roles:           []catalog.Role{{Name: "app", Inherit: true}},
		RoleMemberships: []catalog.RoleMembership{{Role: "other", Member: "app"}},
	}

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), declaration, database, platform.Postgres, must.Must(builtin.New())))

	c.Assert(diff.RoleMembershipsAdded, qt.IsNil)
	c.Assert(diff.RoleMembershipsRemoved, qt.IsNil)
}
