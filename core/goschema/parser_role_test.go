package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
)

func TestRoleAnnotationParsing(t *testing.T) {
	t.Run("basic role annotation", func(t *testing.T) {
		c := qt.New(t)
		goCode := `
package test

//migrator:schema:role name="app_user" login="true" comment="Application user role"
type UserRoles struct {
}
`
		database := parseStringAsGoFile(c, goCode)

		c.Assert(len(database.Roles), qt.Equals, 1)
		role := database.Roles[0]
		c.Assert(role.StructName, qt.Equals, "UserRoles")
		c.Assert(role.Name, qt.Equals, "app_user")
		c.Assert(role.Login, qt.Equals, true)
		c.Assert(role.Password, qt.Equals, "")
		c.Assert(role.Superuser, qt.Equals, false)
		c.Assert(role.CreateDB, qt.Equals, false)
		c.Assert(role.CreateRole, qt.Equals, false)
		c.Assert(role.Inherit, qt.Equals, true) // Default to true
		c.Assert(role.Replication, qt.Equals, false)
		c.Assert(role.Comment, qt.Equals, "Application user role")
	})

	t.Run("role with all attributes", func(t *testing.T) {
		c := qt.New(t)
		goCode := `
package test

//migrator:schema:role name="admin_user" login="true" password="encrypted_password" superuser="true" createdb="true" createrole="true" inherit="false" replication="true" comment="Administrator role"
type AdminRoles struct {
}
`
		database := parseStringAsGoFile(c, goCode)

		c.Assert(len(database.Roles), qt.Equals, 1)
		role := database.Roles[0]
		c.Assert(role.StructName, qt.Equals, "AdminRoles")
		c.Assert(role.Name, qt.Equals, "admin_user")
		c.Assert(role.Login, qt.Equals, true)
		c.Assert(role.Password, qt.Equals, "encrypted_password")
		c.Assert(role.Superuser, qt.Equals, true)
		c.Assert(role.CreateDB, qt.Equals, true)
		c.Assert(role.CreateRole, qt.Equals, true)
		c.Assert(role.Inherit, qt.Equals, false)
		c.Assert(role.Replication, qt.Equals, true)
		c.Assert(role.Comment, qt.Equals, "Administrator role")
	})

	t.Run("role with alternative attribute names", func(t *testing.T) {
		c := qt.New(t)
		goCode := `
package test

//migrator:schema:role name="service_user" create_db="true" create_role="true"
type ServiceRoles struct {
}
`
		database := parseStringAsGoFile(c, goCode)

		c.Assert(len(database.Roles), qt.Equals, 1)
		role := database.Roles[0]
		c.Assert(role.Name, qt.Equals, "service_user")
		c.Assert(role.CreateDB, qt.Equals, true)
		c.Assert(role.CreateRole, qt.Equals, true)
	})

	t.Run("multiple roles in single struct", func(t *testing.T) {
		c := qt.New(t)
		goCode := `
package test

//migrator:schema:role name="app_user" login="true" comment="Application user role"
//migrator:schema:role name="admin_user" login="true" superuser="true" comment="Administrator role"
//migrator:schema:role name="readonly_user" login="true" comment="Read-only user role"
type UserRoles struct {
}
`
		database := parseStringAsGoFile(c, goCode)

		c.Assert(len(database.Roles), qt.Equals, 3)

		// Check app_user
		appUser := findRoleByName(database.Roles, "app_user")
		c.Assert(appUser, qt.IsNotNil)
		c.Assert(appUser.Login, qt.Equals, true)
		c.Assert(appUser.Superuser, qt.Equals, false)
		c.Assert(appUser.Comment, qt.Equals, "Application user role")

		// Check admin_user
		adminUser := findRoleByName(database.Roles, "admin_user")
		c.Assert(adminUser, qt.IsNotNil)
		c.Assert(adminUser.Login, qt.Equals, true)
		c.Assert(adminUser.Superuser, qt.Equals, true)
		c.Assert(adminUser.Comment, qt.Equals, "Administrator role")

		// Check readonly_user
		readonlyUser := findRoleByName(database.Roles, "readonly_user")
		c.Assert(readonlyUser, qt.IsNotNil)
		c.Assert(readonlyUser.Login, qt.Equals, true)
		c.Assert(readonlyUser.Superuser, qt.Equals, false)
		c.Assert(readonlyUser.Comment, qt.Equals, "Read-only user role")
	})

	t.Run("roles across multiple structs", func(t *testing.T) {
		c := qt.New(t)
		goCode := `
package test

//migrator:schema:role name="app_user" login="true"
type AppRoles struct {
}

//migrator:schema:role name="admin_user" login="true" superuser="true"
type AdminRoles struct {
}
`
		database := parseStringAsGoFile(c, goCode)

		c.Assert(len(database.Roles), qt.Equals, 2)

		appUser := findRoleByName(database.Roles, "app_user")
		c.Assert(appUser, qt.IsNotNil)
		c.Assert(appUser.StructName, qt.Equals, "AppRoles")

		adminUser := findRoleByName(database.Roles, "admin_user")
		c.Assert(adminUser, qt.IsNotNil)
		c.Assert(adminUser.StructName, qt.Equals, "AdminRoles")
	})

	t.Run("role with minimal attributes", func(t *testing.T) {
		c := qt.New(t)
		goCode := `
package test

//migrator:schema:role name="minimal_role"
type MinimalRoles struct {
}
`
		database := parseStringAsGoFile(c, goCode)

		c.Assert(len(database.Roles), qt.Equals, 1)
		role := database.Roles[0]
		c.Assert(role.Name, qt.Equals, "minimal_role")
		c.Assert(role.Login, qt.Equals, false)
		c.Assert(role.Password, qt.Equals, "")
		c.Assert(role.Superuser, qt.Equals, false)
		c.Assert(role.CreateDB, qt.Equals, false)
		c.Assert(role.CreateRole, qt.Equals, false)
		c.Assert(role.Inherit, qt.Equals, true) // Default to true
		c.Assert(role.Replication, qt.Equals, false)
		c.Assert(role.Comment, qt.Equals, "")
	})

	t.Run("inherit defaults to true unless explicitly set to false", func(t *testing.T) {
		c := qt.New(t)
		goCode := `
package test

//migrator:schema:role name="inherit_default"
//migrator:schema:role name="inherit_explicit_true" inherit="true"
//migrator:schema:role name="inherit_explicit_false" inherit="false"
type InheritRoles struct {
}
`
		database := parseStringAsGoFile(c, goCode)

		c.Assert(len(database.Roles), qt.Equals, 3)

		defaultRole := findRoleByName(database.Roles, "inherit_default")
		c.Assert(defaultRole.Inherit, qt.Equals, true)

		explicitTrueRole := findRoleByName(database.Roles, "inherit_explicit_true")
		c.Assert(explicitTrueRole.Inherit, qt.Equals, true)

		explicitFalseRole := findRoleByName(database.Roles, "inherit_explicit_false")
		c.Assert(explicitFalseRole.Inherit, qt.Equals, false)
	})
}

// Helper function to find a role by name in a slice of roles
func findRoleByName(roles []goschema.Role, name string) *goschema.Role {
	for _, role := range roles {
		if role.Name == name {
			return &role
		}
	}
	return nil
}
