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
