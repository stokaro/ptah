package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvert_FoldsDefaultPrivilegeRowsByIdentity measures the fold the
// family-coverage guard cannot: that guard passes as soon as the family is named
// in convertedFamilies, with no conversion written, because it records a
// decision rather than a conversion.
//
// The read's grain is one privilege per row and a declaration carries the whole
// privilege list of one identity, so the row count and the declaration count are
// different numbers. Describing each row as its own declaration would claim
// several objects where the server holds one, and the comparison against the
// read they came from would plan a change on every run.
func TestConvert_FoldsDefaultPrivilegeRowsByIdentity(t *testing.T) {
	tests := []struct {
		name string
		rows []catalog.DefaultPrivilege
		want []schemamodel.DefaultPrivilege
	}{
		{
			name: "one row is one declaration",
			rows: []catalog.DefaultPrivilege{{
				Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
				Grantee: "app_reader", Privilege: "SELECT",
			}},
			want: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			}},
		},
		{
			// The grant option lives on the privilege, not on the object: the
			// catalog answers is_grantable per row, and folding the rows into one
			// bool would flip whichever privilege lost.
			name: "grantability stays with the privilege it belongs to",
			rows: []catalog.DefaultPrivilege{
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "SELECT",
				},
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "INSERT", WithOption: true,
				},
			},
			want: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
				Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{
					{Privilege: "SELECT"},
					{Privilege: "INSERT", WithOption: true},
				},
			}},
		},
		{
			// Two identities interleaved, which is what a read ordered by
			// something other than the identity gives. Grouping by the previous
			// row rather than by identity would answer four declarations here.
			name: "identities group across the rows between them",
			rows: []catalog.DefaultPrivilege{
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "SELECT",
				},
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "SEQUENCES",
					Grantee: "app_reader", Privilege: "USAGE",
				},
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "INSERT",
				},
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "SEQUENCES",
					Grantee: "app_reader", Privilege: "SELECT",
				},
			},
			want: []schemamodel.DefaultPrivilege{
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader",
					Privileges: []schemamodel.PrivilegeGrant{
						{Privilege: "SELECT"},
						{Privilege: "INSERT"},
					},
				},
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "SEQUENCES",
					Grantee: "app_reader",
					Privileges: []schemamodel.PrivilegeGrant{
						{Privilege: "USAGE"},
						{Privilege: "SELECT"},
					},
				},
			},
		},
		{
			// The grantor is part of the identity. PostgreSQL keys the catalog on
			// it and refuses the statement from a non-member of that role, so two
			// grantors are two objects and merging them would describe one
			// operator's default as another's.
			name: "the grantor separates two objects",
			rows: []catalog.DefaultPrivilege{
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "SELECT",
				},
				{
					Grantor: "reporting_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "SELECT",
				},
			},
			want: []schemamodel.DefaultPrivilege{
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee:    "app_reader",
					Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
				},
				{
					Grantor: "reporting_owner", Schema: "app", ObjectType: "TABLES",
					Grantee:    "app_reader",
					Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
				},
			},
		},
		{
			// One identity spelled two ways reaches one declaration, and the
			// privilege it holds twice keeps the grantable spelling, which is how
			// PostgreSQL merges two such statements.
			name: "the identity is compared in its canonical form",
			rows: []catalog.DefaultPrivilege{
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "tables",
					Grantee: "app_reader", Privilege: "select",
				},
				{
					Grantor: "app_owner ", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "SELECT", WithOption: true,
				},
			},
			want: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT", WithOption: true}},
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted := dbschematogo.ConvertDBSchemaToGoSchema(&catalog.Database{
				DefaultPrivileges: test.rows,
			}, platform.Postgres)

			c.Assert(converted.DefaultPrivileges, qt.DeepEquals, test.want)
		})
	}
}

// TestConvert_DescribesNoDefaultPrivilegeWhenTheReadFoundNone pins the empty
// answer, which is the state of a stock PostgreSQL database: pg_default_acl
// holds no row until somebody writes ALTER DEFAULT PRIVILEGES.
func TestConvert_DescribesNoDefaultPrivilegeWhenTheReadFoundNone(t *testing.T) {
	c := qt.New(t)

	converted := dbschematogo.ConvertDBSchemaToGoSchema(&catalog.Database{}, platform.Postgres)

	c.Assert(converted.DefaultPrivileges, qt.DeepEquals, make([]schemamodel.DefaultPrivilege, 0))
}
