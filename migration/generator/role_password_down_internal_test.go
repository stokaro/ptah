package generator

// White-box testing required: the down direction is built by reversing a diff
// through unexported helpers, and what this pins is which role the reversal
// hands the planner -- a fact the public API only exposes as rendered SQL after
// the whole pipeline has run.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestGenerateDownMigration_RolledBackPasswordChangeSetsNoPassword pins the one
// entry in a role change map that does not reverse.
//
// `password_update_required` records that a password has to be set, never what
// it is being set from: a comparison cannot read the old value out of the
// database. So the reversed change map still asks for a password, and the role
// the reversal hands the planner is what decides that nothing is written --
// the pre-change database reports no password at all. A rollback carrying the
// declaration's role through would set the NEW password on the way back.
func TestGenerateDownMigration_RolledBackPasswordChangeSetsNoPassword(t *testing.T) {
	c := qt.New(t)

	const declaredPassword = "the-new-secret"

	desired := &schemamodel.Database{
		Roles: []schemamodel.Role{{
			StructName: "AppUser", Name: "app_user",
			Login: true, CreateDB: true, Password: declaredPassword,
		}},
	}
	database := &catalog.Database{
		Roles: []catalog.Role{{Name: "app_user", Login: true}},
	}

	caps := capability.Postgres17().With(capability.RoleManagement, true)
	upDiff := schemadiff.CompareWithDialect(desired, database, platform.Postgres)
	c.Assert(upDiff.RolesModified, qt.HasLen, 1)

	up, err := generateUpMigrationSQL(upDiff, desired, platform.Postgres, caps)
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, "ALTER ROLE \"app_user\" CREATEDB;")
	c.Assert(up, qt.Contains, declaredPassword,
		qt.Commentf("the forward direction sets the password the declaration carries"))

	down, err := generateDownMigrationSQL(upDiff, desired, database, platform.Postgres, caps)
	c.Assert(err, qt.IsNil)
	c.Assert(down, qt.Contains, "ALTER ROLE \"app_user\" NOCREATEDB;",
		qt.Commentf("the attribute change does reverse"))
	c.Assert(down, qt.Not(qt.Contains), declaredPassword,
		qt.Commentf("the rollback must not set the password it is undoing"))
	c.Assert(down, qt.Not(qt.Contains), "PASSWORD")
}
