package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/reservedrole"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestRolesComparison(t *testing.T) {
	t.Run("no roles in either schema", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{Roles: []goschema.Role{}}
		database := &types.DBSchema{Roles: []types.DBRole{}}
		diff := &difftypes.SchemaDiff{}

		compare.Roles(generated, database, diff)

		c.Assert(diff.RolesAdded, qt.HasLen, 0)
		c.Assert(diff.RolesRemoved, qt.HasLen, 0)
		c.Assert(diff.RolesModified, qt.HasLen, 0)
	})

	t.Run("roles added", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{
			Roles: []goschema.Role{
				{Name: "app_user", Login: true},
				{Name: "admin_user", Login: true, Superuser: true},
			},
		}
		database := &types.DBSchema{Roles: []types.DBRole{}}
		diff := &difftypes.SchemaDiff{}

		compare.Roles(generated, database, diff)

		c.Assert(diff.RolesAdded, qt.HasLen, 2)
		c.Assert(diff.RolesAdded, qt.Contains, "app_user")
		c.Assert(diff.RolesAdded, qt.Contains, "admin_user")
		c.Assert(diff.RolesRemoved, qt.HasLen, 0)
		c.Assert(diff.RolesModified, qt.HasLen, 0)
	})

	t.Run("roles not automatically removed", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{Roles: []goschema.Role{}}
		database := &types.DBSchema{
			Roles: []types.DBRole{
				{Name: "old_role", Login: true},
				{Name: "legacy_role", Login: false},
			},
		}
		diff := &difftypes.SchemaDiff{}

		compare.Roles(generated, database, diff)

		// Roles should not be automatically removed for safety
		c.Assert(diff.RolesAdded, qt.HasLen, 0)
		c.Assert(diff.RolesRemoved, qt.HasLen, 0)
		c.Assert(diff.RolesModified, qt.HasLen, 0)
	})

	t.Run("roles modified", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{
			Roles: []goschema.Role{
				{Name: "app_user", Login: true, CreateDB: true},
			},
		}
		database := &types.DBSchema{
			Roles: []types.DBRole{
				{Name: "app_user", Login: false, CreateDB: false},
			},
		}
		diff := &difftypes.SchemaDiff{}

		compare.Roles(generated, database, diff)

		c.Assert(diff.RolesAdded, qt.HasLen, 0)
		c.Assert(diff.RolesRemoved, qt.HasLen, 0)
		c.Assert(diff.RolesModified, qt.HasLen, 1)
		c.Assert(diff.RolesModified[0].RoleName, qt.Equals, "app_user")
		c.Assert(diff.RolesModified[0].Changes, qt.HasLen, 2)
		c.Assert(diff.RolesModified[0].Changes["login"], qt.Equals, "false -> true")
		c.Assert(diff.RolesModified[0].Changes["createdb"], qt.Equals, "false -> true")
	})

	t.Run("mixed changes", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{
			Roles: []goschema.Role{
				{Name: "app_user", Login: true},        // Modified
				{Name: "new_role", Login: true},        // Added
				{Name: "unchanged_role", Login: false}, // Unchanged
			},
		}
		database := &types.DBSchema{
			Roles: []types.DBRole{
				{Name: "app_user", Login: false},       // Modified
				{Name: "old_role", Login: true},        // Removed
				{Name: "unchanged_role", Login: false}, // Unchanged
			},
		}
		diff := &difftypes.SchemaDiff{}

		compare.Roles(generated, database, diff)

		c.Assert(diff.RolesAdded, qt.HasLen, 1)
		c.Assert(diff.RolesAdded[0], qt.Equals, "new_role")

		// Roles are not automatically removed for safety
		c.Assert(diff.RolesRemoved, qt.HasLen, 0)

		c.Assert(diff.RolesModified, qt.HasLen, 1)
		c.Assert(diff.RolesModified[0].RoleName, qt.Equals, "app_user")
		c.Assert(diff.RolesModified[0].Changes["login"], qt.Equals, "false -> true")
	})

	t.Run("results are sorted", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{
			Roles: []goschema.Role{
				{Name: "z_role", Login: true},
				{Name: "a_role", Login: true},
				{Name: "m_role", Login: true, CreateDB: true},
			},
		}
		database := &types.DBSchema{
			Roles: []types.DBRole{
				{Name: "z_old", Login: true},
				{Name: "a_old", Login: true},
				{Name: "m_role", Login: false, CreateDB: false},
			},
		}
		diff := &difftypes.SchemaDiff{}

		compare.Roles(generated, database, diff)

		// Check added roles are sorted
		c.Assert(diff.RolesAdded, qt.DeepEquals, []string{"a_role", "z_role"})

		// Roles are not automatically removed for safety
		c.Assert(diff.RolesRemoved, qt.HasLen, 0)

		// Check modified roles are sorted
		c.Assert(diff.RolesModified, qt.HasLen, 1)
		c.Assert(diff.RolesModified[0].RoleName, qt.Equals, "m_role")
	})
}

func TestRolesTreatsOutOfScopeRolesAsPresent(t *testing.T) {
	// PostgreSQL roles are cluster-wide, so a reader scoped to one schema
	// leaves out roles that exist on the server (stokaro/ptah#1267). Reading
	// that silence as absence plans CREATE ROLE for a role that is already
	// there, and the server refuses it at SQLSTATE 42710 -- the failure that
	// took stokaro/ptah#1273's integration job red three times. Existence has
	// to come from both lists.

	t.Run("plans no create for a role the description leaves out", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{
			Roles: []goschema.Role{{Name: "admin_user", Login: true, Superuser: true}},
		}
		database := &types.DBSchema{
			Roles: []types.DBRole{},
			RolesOutOfScope: []types.DBRole{
				{Name: "admin_user", Login: true, Superuser: true},
			},
		}
		diff := &difftypes.SchemaDiff{}

		compare.Roles(generated, database, diff)

		c.Assert(diff.RolesAdded, qt.HasLen, 0)
		c.Assert(diff.RolesRemoved, qt.HasLen, 0)
		c.Assert(diff.RolesModified, qt.HasLen, 0)
	})

	t.Run("still plans a create for a role that exists nowhere", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{
			Roles: []goschema.Role{
				{Name: "admin_user", Login: true},
				{Name: "brand_new_user", Login: true},
			},
		}
		database := &types.DBSchema{
			RolesOutOfScope: []types.DBRole{{Name: "admin_user", Login: true}},
		}
		diff := &difftypes.SchemaDiff{}

		compare.Roles(generated, database, diff)

		c.Assert(diff.RolesAdded, qt.DeepEquals, []string{"brand_new_user"})
	})

	t.Run("still alters a role the description leaves out", func(t *testing.T) {
		c := qt.New(t)
		// Scoping the description must not cost the capability of correcting
		// a role's attributes: the annotations name this role, so the user
		// asked about it.
		generated := &goschema.Database{
			Roles: []goschema.Role{{Name: "admin_user", Login: true, Superuser: true}},
		}
		database := &types.DBSchema{
			RolesOutOfScope: []types.DBRole{{Name: "admin_user", Login: false, Superuser: false}},
		}
		diff := &difftypes.SchemaDiff{}

		compare.Roles(generated, database, diff)

		c.Assert(diff.RolesAdded, qt.HasLen, 0)
		c.Assert(diff.RolesModified, qt.HasLen, 1)
		c.Assert(diff.RolesModified[0].RoleName, qt.Equals, "admin_user")
		c.Assert(diff.RolesModified[0].Changes["login"], qt.Equals, "false -> true")
		c.Assert(diff.RolesModified[0].Changes["superuser"], qt.Equals, "false -> true")
	})

	t.Run("a described role is compared, and an unrelated out-of-scope name changes nothing", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{
			Roles: []goschema.Role{{Name: "app_user", Login: true}},
		}
		database := &types.DBSchema{
			Roles:           []types.DBRole{{Name: "app_user", Login: true}},
			RolesOutOfScope: []types.DBRole{{Name: "other_tenant_user", Login: true}},
		}
		diff := &difftypes.SchemaDiff{}

		compare.Roles(generated, database, diff)

		c.Assert(diff.RolesAdded, qt.HasLen, 0)
		c.Assert(diff.RolesModified, qt.HasLen, 0)
		c.Assert(diff.RolesRemoved, qt.HasLen, 0)
	})

	t.Run("a described role wins over the same name out of scope", func(t *testing.T) {
		c := qt.New(t)
		// The same NAME in both lists, which is the only shape that can show
		// which one the comparison reads. A PostgreSQL reader's two lists are
		// disjoint, so this decides nothing there; it decides for every other
		// producer of a DBSchema, and the attributes have to come from the
		// description rather than from whichever loop happens to run last.
		// The out-of-scope copy is stale on every attribute, so reading it
		// would plan an ALTER ROLE that changes nothing back.
		generated := &goschema.Database{
			Roles: []goschema.Role{{Name: "app_user", Login: true, CreateDB: true}},
		}
		database := &types.DBSchema{
			Roles:           []types.DBRole{{Name: "app_user", Login: true, CreateDB: true}},
			RolesOutOfScope: []types.DBRole{{Name: "app_user", Login: false, CreateDB: false}},
		}
		diff := &difftypes.SchemaDiff{}

		compare.Roles(generated, database, diff)

		c.Assert(diff.RolesAdded, qt.HasLen, 0)
		c.Assert(diff.RolesRemoved, qt.HasLen, 0)
		c.Assert(diff.RolesModified, qt.HasLen, 0)
	})
}

func TestRolesAnswerIsTheSameWhicheverListTheRoleWasReadInto(t *testing.T) {
	c := qt.New(t)

	// The opt-in that puts the removed capability back
	// ([go.5x5.cz/ptah/internal/rolescope.DescribeAllEnvVar]) changes which
	// list a role arrives in: with it set, the PostgreSQL reader describes
	// every role it manages and RolesOutOfScope is empty. That must not change
	// a single planned statement, and this is where the property is decided.
	//
	// So the two shapes below carry the SAME roles and differ only in the list
	// they arrived in, and the comparison is asserted to be identical. Without
	// it, an operator who turned the variable on to copy a cluster's roles
	// could get a different migration plan for the same two databases, which
	// would make the escape hatch a second behavior rather than a fuller read.
	generated := &goschema.Database{
		Roles: []goschema.Role{
			{Name: "app_user", Login: true},
			{Name: "scoped_out", Login: true, CreateDB: true},
			{Name: "nowhere_at_all", Login: true},
		},
	}
	scoped := &types.DBSchema{
		Roles: []types.DBRole{{Name: "app_user", Login: true}},
		RolesOutOfScope: []types.DBRole{
			{Name: "scoped_out", Login: true},
			{Name: "other_tenant_user", Login: true},
		},
	}
	described := &types.DBSchema{
		Roles: []types.DBRole{
			{Name: "app_user", Login: true},
			{Name: "other_tenant_user", Login: true},
			{Name: "scoped_out", Login: true},
		},
	}

	scopedDiff := &difftypes.SchemaDiff{}
	compare.Roles(generated, scoped, scopedDiff)
	describedDiff := &difftypes.SchemaDiff{}
	compare.Roles(generated, described, describedDiff)

	c.Assert(scopedDiff.RolesAdded, qt.DeepEquals, []string{"nowhere_at_all"})
	c.Assert(scopedDiff.RolesModified, qt.HasLen, 1)
	c.Assert(scopedDiff.RolesModified[0].RoleName, qt.Equals, "scoped_out")
	c.Assert(describedDiff, qt.DeepEquals, scopedDiff,
		qt.Commentf("describing a role instead of carrying it out of scope changed the plan"))
}

func TestRolesReservedNameIsRefusedBeforeThisComparisonRunsAtAll(t *testing.T) {
	c := qt.New(t)

	// This is what TestRolesReservedNameIsNotComparedAgainstAnything pinned as
	// a known gap, rewritten now that stokaro/ptah#1312 closed it. The behavior
	// asserted below is UNCHANGED, and deliberately so: the gap was never in
	// this function, and moving the refusal into it would give the comparator
	// an error return every caller would have to plumb.
	//
	// Roles and RolesOutOfScope partition the roles Ptah MANAGES, not the
	// cluster's role set: a PostgreSQL reader excludes the reserved pg_ roles
	// and the bootstrap superuser from both reads, in either direction. So a
	// reserved name reaching this comparison is compared against nothing and
	// reads as absent, which is exactly why the declaration has to be refused
	// before it gets here rather than repaired once it has.
	//
	// The refusal lives at the two surfaces that accept a desired schema and
	// can return an error -- schemadiff.CompareWithDatabaseInfo on the compare
	// path, and the renderer validation phase migration/planner runs before it
	// emits any node on the generate path. See
	// schemadiff.TestCompareWithDatabaseInfoRefusesAReservedRole and
	// renderer.TestGetOrderedCreateStatementsRefusesAReservedRole.
	//
	// The second assertion is what keeps this from becoming decorative: every
	// name this comparison would read as absent is a name reservedrole.Is
	// recognizes, so there is no reserved spelling that slips past the refusal
	// and lands here.
	generated := &goschema.Database{
		Roles: []goschema.Role{
			{Name: "postgres", Login: true, Superuser: true},
			{Name: "pg_monitor"},
			{Name: "app_user", Login: true},
		},
	}
	database := &types.DBSchema{
		Roles:           []types.DBRole{{Name: "app_user", Login: true}},
		RolesOutOfScope: []types.DBRole{{Name: "other_tenant_user", Login: true}},
	}
	diff := &difftypes.SchemaDiff{}

	compare.Roles(generated, database, diff)

	c.Assert(diff.RolesAdded, qt.DeepEquals, []string{"pg_monitor", "postgres"},
		qt.Commentf("reserved names are in neither database list, so they read as absent"))
	c.Assert(diff.RolesModified, qt.HasLen, 0)
	for _, roleName := range diff.RolesAdded {
		c.Assert(reservedrole.Is(roleName), qt.IsTrue,
			qt.Commentf("%q read as absent but the refusal would not have caught it", roleName))
	}
	c.Assert(reservedrole.ValidateDeclared("postgres", generated.Roles), qt.IsNotNil,
		qt.Commentf("the desired schema this comparison received should never have reached it"))
}

func TestRoleDefinitionsComparison(t *testing.T) {
	t.Run("no differences", func(t *testing.T) {
		c := qt.New(t)
		generated := goschema.Role{
			Name:        "test_role",
			Login:       true,
			Superuser:   false,
			CreateDB:    true,
			CreateRole:  false,
			Inherit:     true,
			Replication: false,
		}
		database := types.DBRole{
			Name:        "test_role",
			Login:       true,
			Superuser:   false,
			CreateDB:    true,
			CreateRole:  false,
			Inherit:     true,
			Replication: false,
		}

		diff := compare.RoleDefinitions(generated, database)

		c.Assert(diff.RoleName, qt.Equals, "test_role")
		c.Assert(diff.Changes, qt.HasLen, 0)
	})

	t.Run("all attributes different", func(t *testing.T) {
		c := qt.New(t)
		generated := goschema.Role{
			Name:        "test_role",
			Login:       true,
			Password:    "encrypted_password",
			Superuser:   true,
			CreateDB:    true,
			CreateRole:  true,
			Inherit:     false,
			Replication: true,
		}
		database := types.DBRole{
			Name:        "test_role",
			Login:       false,
			Superuser:   false,
			CreateDB:    false,
			CreateRole:  false,
			Inherit:     true,
			Replication: false,
		}

		diff := compare.RoleDefinitions(generated, database)

		c.Assert(diff.RoleName, qt.Equals, "test_role")
		c.Assert(diff.Changes, qt.HasLen, 7)
		c.Assert(diff.Changes["login"], qt.Equals, "false -> true")
		c.Assert(diff.Changes["password"], qt.Equals, "password_update_required")
		c.Assert(diff.Changes["superuser"], qt.Equals, "false -> true")
		c.Assert(diff.Changes["createdb"], qt.Equals, "false -> true")
		c.Assert(diff.Changes["createrole"], qt.Equals, "false -> true")
		c.Assert(diff.Changes["inherit"], qt.Equals, "true -> false")
		c.Assert(diff.Changes["replication"], qt.Equals, "false -> true")
	})

	t.Run("only login changed", func(t *testing.T) {
		c := qt.New(t)
		generated := goschema.Role{
			Name:  "test_role",
			Login: true,
		}
		database := types.DBRole{
			Name:  "test_role",
			Login: false,
		}

		diff := compare.RoleDefinitions(generated, database)

		c.Assert(diff.RoleName, qt.Equals, "test_role")
		c.Assert(diff.Changes, qt.HasLen, 1)
		c.Assert(diff.Changes["login"], qt.Equals, "false -> true")
	})

	t.Run("password handling", func(t *testing.T) {
		c := qt.New(t)
		generated := goschema.Role{
			Name:     "test_role",
			Password: "new_password",
		}
		database := types.DBRole{
			Name: "test_role",
		}

		diff := compare.RoleDefinitions(generated, database)

		c.Assert(diff.RoleName, qt.Equals, "test_role")
		c.Assert(diff.Changes, qt.HasLen, 1)
		c.Assert(diff.Changes["password"], qt.Equals, "password_update_required")
	})

	t.Run("no password change when target has no password", func(t *testing.T) {
		c := qt.New(t)
		generated := goschema.Role{
			Name:     "test_role",
			Password: "", // No password in target
		}
		database := types.DBRole{
			Name:        "test_role",
			HasPassword: true, // Database role has a password
		}

		diff := compare.RoleDefinitions(generated, database)

		c.Assert(diff.RoleName, qt.Equals, "test_role")
		c.Assert(diff.Changes, qt.HasLen, 0) // No password change detected
	})
}

func TestGrantsComparison(t *testing.T) {
	t.Run("adds table and schema grants", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{
			Roles: []goschema.Role{{Name: "app_role"}},
			Grants: []goschema.Grant{
				{Role: "app_role", Privileges: []string{"SELECT", "INSERT"}, OnTable: "users"},
				{Role: "app_role", Privileges: []string{"USAGE"}, OnSchema: "public"},
			},
		}
		database := &types.DBSchema{}
		diff := &difftypes.SchemaDiff{}

		compare.Grants(generated, database, diff)

		c.Assert(diff.GrantsAdded, qt.DeepEquals, []difftypes.GrantRef{
			{Role: "app_role", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "public"},
			{Role: "app_role", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "users"},
			{Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users"},
		})
		c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
	})

	t.Run("matches PostgreSQL row per privilege introspection", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{
			Roles: []goschema.Role{{Name: "app_role"}},
			Grants: []goschema.Grant{
				{Role: "app_role", Privileges: []string{"select", "insert"}, OnTable: "public.users"},
			},
		}
		database := &types.DBSchema{
			Grants: []types.DBGrant{
				{Role: "app_role", Privilege: "INSERT", ObjectType: "TABLE", Schema: "public", ObjectName: "users"},
				{Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", Schema: "public", ObjectName: "users"},
			},
		}
		diff := &difftypes.SchemaDiff{}

		compare.Grants(generated, database, diff)

		c.Assert(diff.GrantsAdded, qt.HasLen, 0)
		c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
	})

	t.Run("removes grants only for managed roles", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{
			Roles:  []goschema.Role{{Name: "app_role"}},
			Grants: []goschema.Grant{{Role: "app_role", Privileges: []string{"SELECT"}, OnTable: "users"}},
		}
		database := &types.DBSchema{
			Grants: []types.DBGrant{
				{Role: "app_role", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "users"},
				{Role: "external_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users"},
			},
		}
		diff := &difftypes.SchemaDiff{}

		compare.Grants(generated, database, diff)

		c.Assert(diff.GrantsRemoved, qt.DeepEquals, []difftypes.GrantRef{
			{Role: "app_role", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "users"},
		})
		c.Assert(diff.GrantOptionsRevoked, qt.HasLen, 0)
	})

	t.Run("matches explicit grants to external roles without revoking their other privileges", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{
			Grants: []goschema.Grant{{Role: "external_role", Privileges: []string{"SELECT"}, OnTable: "users"}},
		}
		database := &types.DBSchema{
			Grants: []types.DBGrant{
				{Role: "external_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users"},
				{Role: "external_role", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "users"},
			},
		}
		diff := &difftypes.SchemaDiff{}

		compare.Grants(generated, database, diff)

		c.Assert(diff.GrantsAdded, qt.HasLen, 0)
		c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
	})

	t.Run("downgrades grant option without revoking the privilege", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{
			Roles:  []goschema.Role{{Name: "app_role"}},
			Grants: []goschema.Grant{{Role: "app_role", Privileges: []string{"SELECT"}, OnTable: "users"}},
		}
		database := &types.DBSchema{
			Grants: []types.DBGrant{
				{Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
			},
		}
		diff := &difftypes.SchemaDiff{}

		compare.Grants(generated, database, diff)

		c.Assert(diff.GrantsAdded, qt.HasLen, 0)
		c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
		c.Assert(diff.GrantOptionsRevoked, qt.DeepEquals, []difftypes.GrantRef{
			{Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
		})
	})

	t.Run("upgrades grant option without re-adding the privilege", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{
			Roles: []goschema.Role{{Name: "app_role"}},
			Grants: []goschema.Grant{
				{Role: "app_role", Privileges: []string{"SELECT"}, OnTable: "users", WithOption: true},
			},
		}
		database := &types.DBSchema{
			Grants: []types.DBGrant{
				{Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users"},
			},
		}
		diff := &difftypes.SchemaDiff{}

		compare.Grants(generated, database, diff)

		c.Assert(diff.GrantsAdded, qt.HasLen, 0)
		c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
		c.Assert(diff.GrantOptionsAdded, qt.DeepEquals, []difftypes.GrantRef{
			{Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
		})
		c.Assert(diff.GrantOptionsRevoked, qt.HasLen, 0)
	})

	t.Run("removing a grant with grant option still revokes the full privilege", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{Roles: []goschema.Role{{Name: "app_role"}}}
		database := &types.DBSchema{
			Grants: []types.DBGrant{
				{Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
			},
		}
		diff := &difftypes.SchemaDiff{}

		compare.Grants(generated, database, diff)

		c.Assert(diff.GrantsAdded, qt.HasLen, 0)
		c.Assert(diff.GrantsRemoved, qt.DeepEquals, []difftypes.GrantRef{
			{Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
		})
		c.Assert(diff.GrantOptionsRevoked, qt.HasLen, 0)
	})
}
