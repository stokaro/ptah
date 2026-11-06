package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/devclean"
)

// rootGrantSides is a read and a declaration that each hold a permission on
// the database root and one on a table.
func rootGrantSides() (*catalog.Database, *schemamodel.Database) {
	current := &catalog.Database{Grants: []catalog.Grant{
		{Role: "USERS", Privilege: "ydb.database.connect", ObjectType: "DATABASE", ObjectName: "/local"},
		{Role: "readers", Privilege: "ydb.granular.select_row", ObjectType: "TABLE", Schema: "app", ObjectName: "items"},
	}}
	desired := &schemamodel.Database{
		Grants: []schemamodel.Grant{
			{Role: "USERS", Privileges: []string{"ydb.database.connect"}, OnDatabase: true},
			{Role: "readers", Privileges: []string{"ydb.granular.select_row"}, OnTable: "items"},
		},
		RevokedGrants: []schemamodel.Grant{
			{Role: "DDL-ADMINS", Privileges: []string{"ydb.granular.create_table"}, OnDatabase: true},
			{Role: "writers", Privileges: []string{"ydb.tables.modify"}, OnTable: "items"},
		},
	}
	return current, desired
}

// Both sides lose their permissions on the database root and keep the rest.
func TestWithoutDatabaseGrants_HappyPath(t *testing.T) {
	c := qt.New(t)
	current, desired := rootGrantSides()

	keptCurrent, keptDesired := devclean.WithoutDatabaseGrants(current, desired)

	c.Assert(keptCurrent.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "readers", Privilege: "ydb.granular.select_row", ObjectType: "TABLE", Schema: "app", ObjectName: "items"},
	})
	c.Assert(keptDesired.Grants, qt.DeepEquals, []schemamodel.Grant{
		{Role: "readers", Privileges: []string{"ydb.granular.select_row"}, OnTable: "items"},
	})
	c.Assert(keptDesired.RevokedGrants, qt.DeepEquals, []schemamodel.Grant{
		{Role: "writers", Privileges: []string{"ydb.tables.modify"}, OnTable: "items"},
	})
}

// The caller's values keep every grant they held.
func TestWithoutDatabaseGrantsLeavesItsInputs_HappyPath(t *testing.T) {
	c := qt.New(t)
	current, desired := rootGrantSides()
	wantCurrent, wantDesired := rootGrantSides()

	devclean.WithoutDatabaseGrants(current, desired)

	c.Assert(current, qt.DeepEquals, wantCurrent)
	c.Assert(desired, qt.DeepEquals, wantDesired)
}

// A missing side stays missing.
func TestWithoutDatabaseGrantsOfNothing_HappyPath(t *testing.T) {
	c := qt.New(t)

	keptCurrent, keptDesired := devclean.WithoutDatabaseGrants(nil, nil)

	c.Assert(keptCurrent, qt.IsNil)
	c.Assert(keptDesired, qt.IsNil)
}
