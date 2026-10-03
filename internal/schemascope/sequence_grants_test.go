package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemascope"
)

// TestFilterScopesSequenceGrantsOnBothSidesTheSameWay pins that a privilege on
// a sequence rides the sequence's schema on both sides of a comparison, as a
// routine's does. A sequence is not a table, and a filter that kept only grants
// on kept tables dropped every one from the current side, so `migrate diff`
// planned the grant again on every run (stokaro/ptah#4085). The unqualified
// read stands in for the connected schema, which is how the reader names an
// object there.
func TestFilterScopesSequenceGrantsOnBothSidesTheSameWay(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Grants: []schemamodel.Grant{
		{Role: "app", Privileges: []string{"USAGE"}, OnSequence: "public.todos_id_seq"},
		{Role: "app", Privileges: []string{"USAGE"}, OnSequence: "other.counter"},
	}}
	database := &catalog.Database{Grants: []catalog.Grant{
		{Role: "app", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "todos_id_seq"},
		{Role: "app", Privilege: "USAGE", ObjectType: "SEQUENCE", Schema: "other", ObjectName: "counter"},
	}}

	generated := schemascope.FilterGeneratedWithDefaultSchema(desired, []string{"public"}, "public")
	read := schemascope.FilterDatabaseWithDefaultSchema(database, []string{"public"}, "public")

	c.Assert(generated.Grants, qt.DeepEquals, desired.Grants[:1])
	c.Assert(read.Grants, qt.DeepEquals, database.Grants[:1])
}
