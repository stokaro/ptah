package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestGenerateMigrationAST_AccessOrder_HappyPath pins the order the access
// model's statements take around the tables: what is taken away first, while
// its object and its principals exist; the principals before anything names
// them; memberships once both ends exist; grants once their objects exist; and
// a dropped principal last, after its entries are revoked, since DROP USER
// leaves them behind (measured on 25.1.4.7 and 26.2.1.14).
func TestGenerateMigrationAST_AccessOrder_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{
			Name:   "orders",
			Table:  schemamodel.Table{StructName: "S", Name: "orders"},
			Fields: []schemamodel.Field{keyField("id")},
		}},
		RolesAdded: difftypes.RoleChanges{
			{Name: "readers", Group: true},
			{Name: "app", Login: true, Password: "Secret1!"},
		},
		RolesRemoved:           difftypes.RoleChanges{{Name: "olduser"}, {Name: "oldgroup", Group: true}},
		RolesModified:          []difftypes.RoleDiff{{RoleName: "etl", Changes: map[string]string{"login": "true -> false"}}},
		RoleMembershipsAdded:   []difftypes.RoleMembershipRef{{Role: "readers", Member: "app"}},
		RoleMembershipsRemoved: []difftypes.RoleMembershipRef{{Role: "oldgroup", Member: "etl"}},
		GrantsAdded: []difftypes.GrantRef{
			{Role: "readers", Privilege: "YDB.GRANULAR.SELECT_ROW", ObjectType: "TABLE", ObjectName: "orders"},
			{Role: "app", Privilege: "YDB.DATABASE.CONNECT", ObjectType: "DATABASE"},
		},
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "olduser", Privilege: "YDB.GENERIC.LIST", ObjectType: "SCHEMA", ObjectName: "shop"},
		},
		CurrentDatabasePath: "/local",
	}

	c.Assert(render(c, capability.YDB262(), diff), qt.Equals, "REVOKE 'ydb.generic.list' ON `shop` FROM `olduser`;\n"+
		"ALTER GROUP `oldgroup` DROP USER `etl`;\n"+
		"CREATE GROUP `readers`;\n"+
		"-- WARNING: the password is written in plain text; declare its hash to keep it out of the file\n"+
		"CREATE USER `app` PASSWORD 'Secret1!';\n"+
		"ALTER USER `etl` NOLOGIN;\n"+
		"CREATE TABLE `orders` (\n    `id` Int64 NOT NULL,\n    PRIMARY KEY (`id`)\n);\n"+
		"ALTER GROUP `readers` ADD USER `app`;\n"+
		"GRANT 'ydb.granular.select_row' ON `orders` TO `readers`;\n"+
		"GRANT 'ydb.database.connect' ON `/local` TO `app`;\n"+
		"DROP USER IF EXISTS `olduser`;\n"+
		"DROP GROUP IF EXISTS `oldgroup`;\n")
}

// TestGenerateMigrationAST_GrantPaths_HappyPath pins how a plan names a
// grant's object: relative to the database root where the line resolves the
// path, which a migration file needs to apply to another database, and by its
// absolute path under the database's own where it does not.
func TestGenerateMigrationAST_GrantPaths_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		ref  difftypes.GrantRef
		want string
	}{
		{name: "a table at the root on 26.2", caps: capability.YDB262(),
			ref:  difftypes.GrantRef{Role: "app", Privilege: "LIST", ObjectType: "TABLE", ObjectName: "events"},
			want: "GRANT 'ydb.generic.list' ON `events` TO `app`;\n"},
		{name: "a table at the root on 25.1", caps: capability.YDB251(),
			ref:  difftypes.GrantRef{Role: "app", Privilege: "LIST", ObjectType: "TABLE", ObjectName: "events"},
			want: "GRANT 'ydb.generic.list' ON `/local/events` TO `app`;\n"},
		{name: "a table in a directory on 25.1", caps: capability.YDB251(),
			ref:  difftypes.GrantRef{Role: "app", Privilege: "LIST", ObjectType: "TABLE", ObjectName: "shop.orders"},
			want: "GRANT 'ydb.generic.list' ON `shop/orders` TO `app`;\n"},
		{name: "a directory at the root on 25.1", caps: capability.YDB251(),
			ref:  difftypes.GrantRef{Role: "app", Privilege: "LIST", ObjectType: "SCHEMA", ObjectName: "shop"},
			want: "GRANT 'ydb.generic.list' ON `/local/shop` TO `app`;\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{GrantsAdded: []difftypes.GrantRef{test.ref}, CurrentDatabasePath: "/local"}
			c.Assert(render(c, test.caps, diff), qt.Equals, test.want)
		})
	}
}

// TestGenerateMigrationAST_RebuildRegrants_HappyPath pins that a table the plan
// rebuilds gets back every permission entry the old one held, for every
// subject: the entries live in the old table's access list, which the rebuild
// drops with it, and RENAME TO carries the new table's, which has none
// (measured on 25.1 to 26.2: a rename keeps a table's entries). An entry the
// plan revokes is not given back, and one it grants is granted once.
func TestGenerateMigrationAST_RebuildRegrants_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := modified(difftypes.TableDiff{
		TableName: "app.items", Desired: appItems(field("label", "TEXT", true), field("n", "BIGINT", true)),
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
	})
	diff.CurrentGrants = []difftypes.GrantRef{
		{Role: "app", Privilege: "YDB.GRANULAR.SELECT_ROW", ObjectType: "TABLE", ObjectName: "app.items"},
		{Role: "someone", Privilege: "YDB.GENERIC.LIST", ObjectType: "TABLE", ObjectName: "app.items"},
		{Role: "app", Privilege: "YDB.GRANULAR.ERASE_ROW", ObjectType: "TABLE", ObjectName: "app.items"},
		{Role: "app", Privilege: "YDB.GENERIC.LIST", ObjectType: "TABLE", ObjectName: "app.other"},
	}
	diff.GrantsRemoved = []difftypes.GrantRef{
		{Role: "app", Privilege: "YDB.GRANULAR.ERASE_ROW", ObjectType: "TABLE", ObjectName: "app.items"},
	}

	sql := renderRebuild(c, capability.YDB262(), diff)
	c.Assert(sql, qt.Matches, "(?s)REVOKE 'ydb.granular.erase_row' ON `app/items` FROM `app`;\n.*"+
		"RENAME TO `app/items`;\n.*"+
		"GRANT 'ydb.granular.select_row' ON `app/items` TO `app`;\n"+
		"GRANT 'ydb.generic.list' ON `app/items` TO `someone`;\n")
	c.Assert(sql, qt.Not(qt.Contains), "GRANT 'ydb.granular.erase_row'")
	c.Assert(sql, qt.Not(qt.Contains), "`app/other`")
}

// TestGenerateMigrationAST_AccessChanges_FailurePath pins what a plan refuses
// before it emits anything: a change YDB has no statement for, an object only
// an absolute path reaches in a plan with no database path, and a change on a
// target without the key it needs.
func TestGenerateMigrationAST_AccessChanges_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{name: "a user turned into a group", caps: capability.YDB262(),
			diff:    &difftypes.SchemaDiff{RolesModified: []difftypes.RoleDiff{{RoleName: "app", Changes: map[string]string{"group": "false -> true"}}}},
			wantErr: `role app: YDB has no statement that turns a user into a group or a group into a user; .*`},
		{name: "an attribute a user does not carry", caps: capability.YDB262(),
			diff:    &difftypes.SchemaDiff{RolesModified: []difftypes.RoleDiff{{RoleName: "app", Changes: map[string]string{"inherit": "true -> false"}}}},
			wantErr: `role app: its inherit changes \(true -> false\), which a YDB user or group does not carry`},
		{name: "a group's login", caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{RolesModified: []difftypes.RoleDiff{{RoleName: "readers",
				Changes: map[string]string{"login": "false -> true"}, Desired: schemamodel.Role{Name: "readers", Group: true}}}},
			wantErr: `role readers: a YDB group never logs in and has no password`},
		{name: "a grant option", caps: capability.YDB262(),
			diff:    &difftypes.SchemaDiff{GrantOptionsAdded: []difftypes.GrantRef{{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"}}},
			wantErr: `the plan changes a grant option: YDB records WITH GRANT OPTION as the permission ydb.access.grant .*`},
		{name: "the database with no path", caps: capability.YDB262(),
			diff:    &difftypes.SchemaDiff{GrantsAdded: []difftypes.GrantRef{{Role: "app", Privilege: "CONNECT", ObjectType: "DATABASE"}}},
			wantErr: `granting CONNECT on the database to app: YDB takes this object only by its absolute path, .*, and this plan was compared with no live database to take it from`},
		{name: "a root table on 25.1 with no path", caps: capability.YDB251(),
			diff:    &difftypes.SchemaDiff{GrantsRemoved: []difftypes.GrantRef{{Role: "app", Privilege: "LIST", ObjectType: "TABLE", ObjectName: "events"}}},
			wantErr: `revoking LIST on table events to app: YDB takes this object only by its absolute path, .*`},
		{name: "a membership without the key", caps: capability.YDB262().With(capability.RoleMembership, false),
			diff:    &difftypes.SchemaDiff{RoleMembershipsAdded: []difftypes.RoleMembershipRef{{Role: "readers", Member: "app"}}},
			wantErr: `the plan changes the members of a group, which requires target capability role_membership, .*`},
		{name: "a group without the key", caps: capability.YDB262().With(capability.GroupPrincipals, false),
			diff:    &difftypes.SchemaDiff{RolesAdded: difftypes.RoleChanges{{Name: "readers", Group: true}}},
			wantErr: `the plan creates or drops group readers, which requires target capability group_principals, .*`},
		{name: "a database grant without the key", caps: capability.YDB262().With(capability.DatabaseGrants, false),
			diff:    &difftypes.SchemaDiff{GrantsAdded: []difftypes.GrantRef{{Role: "app", Privilege: "CONNECT", ObjectType: "DATABASE"}}, CurrentDatabasePath: "/local"},
			wantErr: `the plan changes a permission on the database, which requires target capability database_grants, .*`},
		{name: "any access change without role management", caps: capability.YDB262().With(capability.RoleManagement, false),
			diff:    &difftypes.SchemaDiff{RolesAdded: difftypes.RoleChanges{{Name: "app"}}},
			wantErr: `the plan changes a user, a group or a permission, which requires target capability role_management, .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(test.diff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestGenerateMigrationAST_RevokesOnlyWhereTheTableStays plans no REVOKE on a
// table the plan creates or drops. A table the plan creates holds no entry yet,
// and a REVOKE planned before its CREATE TABLE would fail with `Path does not
// exist`; a table the plan drops takes its entries with it.
func TestGenerateMigrationAST_RevokesOnlyWhereTheTableStays(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{
			Name:   "orders",
			Table:  schemamodel.Table{StructName: "S", Name: "orders"},
			Fields: []schemamodel.Field{keyField("id")},
		}},
		TablesRemoved: []string{"legacy"},
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "app", Privilege: "YDB.GRANULAR.ERASE_ROW", ObjectType: "TABLE", ObjectName: "orders"},
			{Role: "app", Privilege: "YDB.GRANULAR.ERASE_ROW", ObjectType: "TABLE", ObjectName: "legacy"},
			{Role: "app", Privilege: "YDB.GRANULAR.ERASE_ROW", ObjectType: "TABLE", ObjectName: "kept"},
		},
	}

	c.Assert(render(c, capability.YDB262(), diff), qt.Equals,
		"REVOKE 'ydb.granular.erase_row' ON `kept` FROM `app`;\n"+
			"CREATE TABLE `orders` (\n    `id` Int64 NOT NULL,\n    PRIMARY KEY (`id`)\n);\n"+
			"DROP TABLE `legacy`;\n")
}
