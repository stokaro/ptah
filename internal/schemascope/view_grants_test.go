package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/internal/schemascope"
)

// TestFilterScopesViewGrantsOnBothSidesTheSameWay pins that a privilege on a
// view or a materialized view rides its relation on both sides of a
// comparison, as a privilege on a table does. PostgreSQL reports a grant on a
// view as a TABLE grant. Scoped by the kept tables alone, the grant leaves the
// current side, and `migrate diff` plans it again on every run
// (stokaro/ptah#5047, stokaro/ptah#5016). The grant on a view in a schema the
// selection leaves out leaves with it. The unqualified read stands in for the
// connected schema, which is how the reader names an object there.
func TestFilterScopesViewGrantsOnBothSidesTheSameWay(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Views:             []schemamodel.View{{Name: "v"}, {Name: "other.w"}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "public.mv"}},
		Grants: []schemamodel.Grant{
			{Role: "PUBLIC", Privileges: []string{"SELECT"}, OnTable: "v"},
			{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "public.mv"},
			{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "other.w"},
		},
		RevokedGrants: []schemamodel.Grant{
			{Role: "PUBLIC", Privileges: []string{"INSERT"}, OnTable: "v"},
			{Role: "PUBLIC", Privileges: []string{"INSERT"}, OnTable: "other.w"},
		},
	}
	database := &catalog.Database{
		Views:    []catalog.View{{Name: "v"}, {Schema: "other", Name: "w"}},
		MatViews: []catalog.MaterializedView{{Name: "mv"}},
		Grants: []catalog.Grant{
			{Role: "PUBLIC", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "v"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "mv"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "other", ObjectName: "w"},
		},
	}

	generated, err := schemascope.FilterGeneratedWithDefaultSchema(t.Context(), selectext.None(), desired, []string{"public"}, "public")
	c.Assert(err, qt.IsNil)
	read, err := schemascope.FilterDatabaseWithDefaultSchema(t.Context(), selectext.None(), database, []string{"public"}, "public")
	c.Assert(err, qt.IsNil)

	c.Assert(generated.Grants, qt.DeepEquals, desired.Grants[:2])
	c.Assert(generated.RevokedGrants, qt.DeepEquals, desired.RevokedGrants[:1])
	c.Assert(read.Grants, qt.DeepEquals, database.Grants[:2])
}

// A grant on a relation the source does not declare stays when its schema is
// selected, so the plan answers for it as on an unscoped run: it refuses a
// grant on a view it drops, or the server refuses the grant. Without this the
// plan drops the view and says nothing about the grant, and the next run reports
// the schema synced while the source still names a grant that does not exist
// (stokaro/ptah#5371). A grant in a schema the selection leaves out goes.
func TestFilterKeepsAGrantOnAnUndeclaredRelationInASelectedSchema(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Grants: []schemamodel.Grant{
			{Role: "PUBLIC", Privileges: []string{"SELECT"}, OnTable: "gone"},
			{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "public.gone"},
			{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "other.gone"},
		},
		RevokedGrants: []schemamodel.Grant{
			{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "public.gone"},
			{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "other.gone"},
		},
	}

	generated, err := schemascope.FilterGeneratedWithDefaultSchema(t.Context(), selectext.None(), desired, []string{"public"}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(generated.Grants, qt.DeepEquals, desired.Grants[:2])
	c.Assert(generated.RevokedGrants, qt.DeepEquals, desired.RevokedGrants[:1])
}
