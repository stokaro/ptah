package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
)

// TestToDBSchema_DefaultPrivilegesSurviveTheRoundTrip holds the two directions
// of the default-privilege conversion to one cardinality.
//
// ToDBSchema fans one declaration out to one row per privilege, and
// [dbschematogo.ConvertDBSchemaToGoSchema] folds the rows of one identity back
// into one declaration. Each package tests its own direction, and neither test
// can see the pair disagree: a fan-out that emitted one row per declaration, or
// a fold that kept one declaration per row, passes on its own side while a read
// compared against the description it produced plans the same change forever.
//
// What crosses is the identity and the privilege list. StructName, Comment and
// Dialects describe where a declaration came from and what it is scoped to,
// which the catalog has no column for, so the declaration that comes back
// carries none of them.
func TestToDBSchema_DefaultPrivilegesSurviveTheRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		declared []schemamodel.DefaultPrivilege
		wantRows []catalog.DefaultPrivilege
		wantBack []schemamodel.DefaultPrivilege
	}{
		{
			name: "one privilege is one row",
			declared: []schemamodel.DefaultPrivilege{{
				StructName: "AccessControl",
				Grantor:    "app_owner", Schema: "app", ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			}},
			wantRows: []catalog.DefaultPrivilege{{
				Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
				Grantee: "app_reader", Privilege: "SELECT",
			}},
			wantBack: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			}},
		},
		{
			// Three privileges of one declaration are three rows, and they come
			// back as one declaration again. A fold keyed on anything narrower
			// than the identity would answer three declarations here, which the
			// comparison would read as three objects the server does not hold.
			name: "three privileges are three rows and one declaration again",
			declared: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
				Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{
					{Privilege: "SELECT"},
					{Privilege: "INSERT", WithOption: true},
					{Privilege: "UPDATE"},
				},
			}},
			wantRows: []catalog.DefaultPrivilege{
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "SELECT",
				},
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "INSERT", WithOption: true,
				},
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "UPDATE",
				},
			},
			wantBack: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
				Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{
					{Privilege: "SELECT"},
					{Privilege: "INSERT", WithOption: true},
					{Privilege: "UPDATE"},
				},
			}},
		},
		{
			// Two objects that differ only in the object type stay two, and the
			// rows of one do not join the other on the way back.
			name: "two identities stay two declarations",
			declared: []schemamodel.DefaultPrivilege{
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee:    "app_reader",
					Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
				},
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "SEQUENCES",
					Grantee:    "app_reader",
					Privileges: []schemamodel.PrivilegeGrant{{Privilege: "USAGE"}},
				},
			},
			wantRows: []catalog.DefaultPrivilege{
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "SELECT",
				},
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "SEQUENCES",
					Grantee: "app_reader", Privilege: "USAGE",
				},
			},
			wantBack: []schemamodel.DefaultPrivilege{
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee:    "app_reader",
					Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
				},
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "SEQUENCES",
					Grantee:    "app_reader",
					Privileges: []schemamodel.PrivilegeGrant{{Privilege: "USAGE"}},
				},
			},
		},
		{
			// A declaration written loosely reaches the rows in its canonical
			// form, which is the form the catalog answers in, and comes back in
			// that same form. Without the canonicalization on the way out the
			// comparison would hold a lower-case privilege against an upper-case
			// row for good.
			name: "a loosely written declaration crosses in canonical form",
			declared: []schemamodel.DefaultPrivilege{{
				Grantor: " app_owner ", Schema: "app", ObjectType: "tables",
				Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{
					{Privilege: "select"},
					{Privilege: "SELECT", WithOption: true},
				},
			}},
			wantRows: []catalog.DefaultPrivilege{{
				Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
				Grantee: "app_reader", Privilege: "SELECT", WithOption: true,
			}},
			wantBack: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT", WithOption: true}},
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			read := goschematodb.ToDBSchema(
				&schemamodel.Database{DefaultPrivileges: test.declared},
				platform.Postgres,
			)

			c.Assert(read.DefaultPrivileges, qt.DeepEquals, test.wantRows)

			back := dbschematogo.ConvertDBSchemaToGoSchema(read, platform.Postgres)

			c.Assert(back.DefaultPrivileges, qt.DeepEquals, test.wantBack)
		})
	}
}
