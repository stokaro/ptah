package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
)

func TestCompareWithDatabaseInfoRefusesUnknownRolePasswordStateFailurePath(t *testing.T) {
	const declaredValue = "do-not-print-this-password"
	tests := []struct {
		name     string
		current  *catalog.Database
		roleName string
	}{
		{
			name: "described role",
			current: &catalog.Database{Roles: []catalog.Role{{
				Name:          "app_user",
				PasswordState: catalog.RolePasswordUnknown,
			}}},
			roleName: "app_user",
		},
		{
			name: "role outside the described scope",
			current: &catalog.Database{RolesOutOfScope: []catalog.Role{{
				Name:          "background_user",
				PasswordState: catalog.RolePasswordUnknown,
			}}},
			roleName: "background_user",
		},
		{
			name: "invalid catalog state is refused conservatively",
			current: &catalog.Database{Roles: []catalog.Role{{
				Name:          "corrupt_user",
				PasswordState: catalog.RolePasswordState("invalid"),
			}}},
			roleName: "corrupt_user",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff, err := schemadiff.CompareWithDatabaseInfo(
				&schemamodel.Database{Roles: []schemamodel.Role{{
					Name:     test.roleName,
					Password: declaredValue,
				}}},
				test.current,
				postgresInfo(),
				nil,
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err.Error(), qt.Contains,
				`cannot compare password for role "`+test.roleName+`": current password state is unknown`)
			c.Assert(err.Error(), qt.Not(qt.Contains), declaredValue)
			c.Assert(diff, qt.IsNil)
		})
	}
}

func TestCompareWithDatabaseInfoKnownAbsentRolePasswordPlansUpdateHappyPath(t *testing.T) {
	c := qt.New(t)
	diff, err := schemadiff.CompareWithDatabaseInfo(
		&schemamodel.Database{Roles: []schemamodel.Role{{Name: "app_user", Password: "new-password"}}},
		&catalog.Database{Roles: []catalog.Role{{
			Name:          "app_user",
			PasswordState: catalog.RolePasswordAbsent,
		}}},
		postgresInfo(),
		nil,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.RolesModified, qt.HasLen, 1)
	c.Assert(diff.RolesModified[0].Changes, qt.DeepEquals, map[string]string{
		"password": "password_update_required",
	})
}

func TestCompareWithDatabaseInfoKnownPresentRolePasswordPlansNothingHappyPath(t *testing.T) {
	c := qt.New(t)
	diff, err := schemadiff.CompareWithDatabaseInfo(
		&schemamodel.Database{Roles: []schemamodel.Role{{Name: "app_user", Password: "new-password"}}},
		&catalog.Database{Roles: []catalog.Role{{
			Name:          "app_user",
			PasswordState: catalog.RolePasswordPresent,
		}}},
		postgresInfo(),
		nil,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.RolesModified, qt.HasLen, 0)
}

func TestCompareWithDatabaseInfoNewRoleMayDeclarePasswordHappyPath(t *testing.T) {
	c := qt.New(t)
	diff, err := schemadiff.CompareWithDatabaseInfo(
		&schemamodel.Database{Roles: []schemamodel.Role{{Name: "new_user", Password: "new-password"}}},
		&catalog.Database{},
		postgresInfo(),
		nil,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.RolesAdded.Names(), qt.DeepEquals, []string{"new_user"})
}
