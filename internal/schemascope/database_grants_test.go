package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/internal/schemascope"
)

// A directory selection must retain database permissions on both sides, or
// it silently ignores the CONNECT permission declared beside a YDB user.
func TestDatabaseGrantsHaveNoOwningSchema(t *testing.T) {
	c := qt.New(t)
	grants := []schemamodel.Grant{{Role: "app", OnDatabase: true, Privileges: []string{"CONNECT"}}}
	readGrants := []catalog.Grant{{Role: "app", ObjectType: "DATABASE", Privilege: "CONNECT"}}
	desired := &schemamodel.Database{Grants: grants, RevokedGrants: grants}
	live := &catalog.Database{Grants: readGrants}
	got, err := schemascope.FilterGeneratedWithDefaultSchema(t.Context(), selectext.None(), desired, []string{"shop"}, "other")
	c.Assert(err, qt.IsNil)
	read, err := schemascope.FilterDatabaseWithDefaultSchema(t.Context(), selectext.None(), live, []string{"shop"}, "other")
	c.Assert(err, qt.IsNil)
	c.Assert(got.Grants, qt.DeepEquals, grants)
	c.Assert(got.RevokedGrants, qt.DeepEquals, grants)
	c.Assert(read.Grants, qt.DeepEquals, readGrants)
}

// A directory selection keeps a grant on a scheme object, such as a YDB
// topic, by the directory the object is in, on both sides, as it keeps the
// object.
func TestObjectGrantsRideTheirDirectory(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Grants: []schemamodel.Grant{
		{Role: "app", OnObject: "shop.events", ObjectKind: "TOPIC", Privileges: []string{"SELECT ROW"}},
		{Role: "app", OnObject: "other.events", ObjectKind: "SECRET", Privileges: []string{"SELECT ROW"}},
	}}
	live := &catalog.Database{Grants: []catalog.Grant{
		{Role: "app", ObjectType: "TOPIC", Schema: "shop", ObjectName: "events", Privilege: "ydb.granular.select_row"},
		{Role: "app", ObjectType: "SECRET", Schema: "other", ObjectName: "events", Privilege: "ydb.granular.select_row"},
	}}

	got, err := schemascope.FilterGeneratedWithDefaultSchema(t.Context(), selectext.None(), desired, []string{"shop"}, "")
	c.Assert(err, qt.IsNil)
	read, err := schemascope.FilterDatabaseWithDefaultSchema(t.Context(), selectext.None(), live, []string{"shop"}, "")
	c.Assert(err, qt.IsNil)

	c.Assert(got.Grants, qt.DeepEquals, desired.Grants[:1])
	c.Assert(read.Grants, qt.DeepEquals, live.Grants[:1])
}
