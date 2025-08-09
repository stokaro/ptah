package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/compare"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestRolesComparison(t *testing.T) {
	t.Run("no roles in either schema", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{Roles: []goschema.Role{}}
		database := &types.DBSchema{Roles: []types.DBRole{}}
		diff := &difftypes.SchemaDiff{}

		compare.Roles(generated, database, diff)

		c.Assert(len(diff.RolesAdded), qt.Equals, 0)
		c.Assert(len(diff.RolesRemoved), qt.Equals, 0)
		c.Assert(len(diff.RolesModified), qt.Equals, 0)
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

		c.Assert(len(diff.RolesAdded), qt.Equals, 2)
		c.Assert(diff.RolesAdded, qt.Contains, "app_user")
		c.Assert(diff.RolesAdded, qt.Contains, "admin_user")
		c.Assert(len(diff.RolesRemoved), qt.Equals, 0)
		c.Assert(len(diff.RolesModified), qt.Equals, 0)
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
		c.Assert(len(diff.RolesAdded), qt.Equals, 0)
		c.Assert(len(diff.RolesRemoved), qt.Equals, 0)
		c.Assert(len(diff.RolesModified), qt.Equals, 0)
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

		c.Assert(len(diff.RolesAdded), qt.Equals, 0)
		c.Assert(len(diff.RolesRemoved), qt.Equals, 0)
		c.Assert(len(diff.RolesModified), qt.Equals, 1)
		c.Assert(diff.RolesModified[0].RoleName, qt.Equals, "app_user")
		c.Assert(len(diff.RolesModified[0].Changes), qt.Equals, 2)
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

		c.Assert(len(diff.RolesAdded), qt.Equals, 1)
		c.Assert(diff.RolesAdded[0], qt.Equals, "new_role")

		// Roles are not automatically removed for safety
		c.Assert(len(diff.RolesRemoved), qt.Equals, 0)

		c.Assert(len(diff.RolesModified), qt.Equals, 1)
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
		c.Assert(len(diff.RolesRemoved), qt.Equals, 0)

		// Check modified roles are sorted
		c.Assert(len(diff.RolesModified), qt.Equals, 1)
		c.Assert(diff.RolesModified[0].RoleName, qt.Equals, "m_role")
	})
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
		c.Assert(len(diff.Changes), qt.Equals, 0)
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
		c.Assert(len(diff.Changes), qt.Equals, 7)
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
		c.Assert(len(diff.Changes), qt.Equals, 1)
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
		c.Assert(len(diff.Changes), qt.Equals, 1)
		c.Assert(diff.Changes["password"], qt.Equals, "password_update_required")
	})

	t.Run("no password change when target has no password", func(t *testing.T) {
		c := qt.New(t)
		generated := goschema.Role{
			Name:     "test_role",
			Password: "", // No password in target
		}
		database := types.DBRole{
			Name:    "test_role",
			Comment: "has_password", // Simulating existing password
		}

		diff := compare.RoleDefinitions(generated, database)

		c.Assert(diff.RoleName, qt.Equals, "test_role")
		c.Assert(len(diff.Changes), qt.Equals, 0) // No password change detected
	})
}
