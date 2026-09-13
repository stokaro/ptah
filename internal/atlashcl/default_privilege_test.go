package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
)

// TestParseDefaultPrivilege_HappyPath reads the block Ptah writes for the
// family, and pins the part a `permission` block could not carry: the grantor.
//
// Two declarations differing only in `for_role` are two objects, so a parser
// that dropped the attribute would fold them into one and the comparison would
// plan a change forever.
func TestParseDefaultPrivilege_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "app" {}

role "app_owner" {}

role "app_reader" {}

default_privilege {
  for_role    = role.app_owner
  schema      = schema.app
  object_type = "TABLES"
  to          = role.app_reader
  privileges  = ["SELECT", "INSERT"]
  grantable   = ["INSERT"]
  comment     = "reader sees new tables"
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.DefaultPrivileges, qt.HasLen, 1)
	c.Assert(db.DefaultPrivileges[0].Grantor, qt.Equals, "app_owner")
	c.Assert(db.DefaultPrivileges[0].Schema, qt.Equals, "app")
	c.Assert(db.DefaultPrivileges[0].ObjectType, qt.Equals, "TABLES")
	c.Assert(db.DefaultPrivileges[0].Grantee, qt.Equals, "app_reader")
	c.Assert(db.DefaultPrivileges[0].Comment, qt.Equals, "reader sees new tables")
	c.Assert(db.DefaultPrivileges[0].Privileges, qt.DeepEquals, []schemamodel.PrivilegeGrant{
		{Privilege: "SELECT"},
		{Privilege: "INSERT", WithOption: true},
	})
}

// TestParseDefaultPrivilege_GrantabilityIsPerPrivilege is the row the pair shape
// exists for.
//
// pg_default_acl explodes to one row per privilege, each with its own
// is_grantable, so a declaration mixing the two reads back as two rows. A single
// flag beside a privilege list would answer one of them and lose the other, and
// the comparison would then flip that privilege on every run.
func TestParseDefaultPrivilege_GrantabilityIsPerPrivilege(t *testing.T) {
	tests := []struct {
		name      string
		grantable string
		want      []schemamodel.PrivilegeGrant
	}{
		{
			name:      "none grantable",
			grantable: "",
			want: []schemamodel.PrivilegeGrant{
				{Privilege: "SELECT"},
				{Privilege: "INSERT"},
			},
		},
		{
			name:      "one of two grantable",
			grantable: `grantable = ["INSERT"]`,
			want: []schemamodel.PrivilegeGrant{
				{Privilege: "SELECT"},
				{Privilege: "INSERT", WithOption: true},
			},
		},
		{
			name:      "all grantable",
			grantable: `grantable = ["SELECT", "INSERT"]`,
			want: []schemamodel.PrivilegeGrant{
				{Privilege: "SELECT", WithOption: true},
				{Privilege: "INSERT", WithOption: true},
			},
		},
		{
			// The fold is the one Canonicalize applies, so a document written in
			// lower case still pairs its grantable names with its privileges.
			name:      "a grantable name spelled in lower case",
			grantable: `grantable = ["insert"]`,
			want: []schemamodel.PrivilegeGrant{
				{Privilege: "SELECT"},
				{Privilege: "INSERT", WithOption: true},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(`
default_privilege {
  for_role    = "app_owner"
  schema      = "app"
  object_type = "TABLES"
  to          = "app_reader"
  privileges  = ["SELECT", "INSERT"]
  `+test.grantable+`
}
`), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.DefaultPrivileges, qt.HasLen, 1)
			c.Assert(db.DefaultPrivileges[0].Privileges, qt.DeepEquals, test.want)
		})
	}
}

// TestParseDefaultPrivilege_FailurePath refuses a block that addresses nothing
// or contradicts itself.
//
// Every part of the identity is required, because the identity is what the
// object IS. A block missing one would be accepted, rendered as nothing by the
// PostgreSQL renderer -- which refuses a statement without each of them -- and
// the author would learn about it from a schema that never changes.
func TestParseDefaultPrivilege_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name: "no grantor",
			body: `
  schema      = "app"
  object_type = "TABLES"
  to          = "app_reader"
  privileges  = ["SELECT"]`,
			wantErr: `.*default_privilege requires for_role.*`,
		},
		{
			name: "no schema",
			body: `
  for_role    = "app_owner"
  object_type = "TABLES"
  to          = "app_reader"
  privileges  = ["SELECT"]`,
			wantErr: `.*default_privilege requires schema.*`,
		},
		{
			name: "no object type",
			body: `
  for_role   = "app_owner"
  schema     = "app"
  to         = "app_reader"
  privileges = ["SELECT"]`,
			wantErr: `.*default_privilege requires object_type.*`,
		},
		{
			name: "no grantee",
			body: `
  for_role    = "app_owner"
  schema      = "app"
  object_type = "TABLES"
  privileges  = ["SELECT"]`,
			wantErr: `.*default_privilege requires to.*`,
		},
		{
			name: "no privileges",
			body: `
  for_role    = "app_owner"
  schema      = "app"
  object_type = "TABLES"
  to          = "app_reader"`,
			wantErr: `.*default_privilege requires privileges.*`,
		},
		{
			name: "a grantable privilege that is not granted",
			body: `
  for_role    = "app_owner"
  schema      = "app"
  object_type = "TABLES"
  to          = "app_reader"
  privileges  = ["SELECT"]
  grantable   = ["INSERT"]`,
			wantErr: `.*default_privilege grantable "INSERT" is not one of its privileges.*`,
		},
		{
			name: "an attribute the block does not model",
			body: `
  for_role    = "app_owner"
  schema      = "app"
  object_type = "TABLES"
  to          = "app_reader"
  privileges  = ["SELECT"]
  wibble      = true`,
			wantErr: `.*unsupported default_privilege attribute "wibble".*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte("default_privilege {"+test.body+"\n}\n"), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParseDefaultPrivilege_RefusesALabel keeps the block addressed by its
// attributes alone. Its identity has four parts, so a label would name one of
// them and leave the reader guessing which.
func TestParseDefaultPrivilege_RefusesALabel(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
default_privilege "app" {
  for_role    = "app_owner"
  schema      = "app"
  object_type = "TABLES"
  to          = "app_reader"
  privileges  = ["SELECT"]
}
`), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `.*default_privilege block does not accept labels.*`)
	c.Assert(db, qt.IsNil)
}

// TestParseDefaultPrivilege_ReadsARepeatedIdentity is the verdict the
// redeclaration ledger records for this block, measured rather than asserted in
// prose alone.
//
// A repeat costs nothing here: the fold MERGES the privilege lists of two
// declarations sharing an identity, which is what PostgreSQL does with two ALTER
// DEFAULT PRIVILEGES statements naming one. The privileges differ between the
// two blocks on purpose -- with one list in both, a drop and a merge produce the
// same answer and the row would say nothing.
func TestParseDefaultPrivilege_ReadsARepeatedIdentity(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
default_privilege {
  for_role    = "app_owner"
  schema      = "app"
  object_type = "TABLES"
  to          = "app_reader"
  privileges  = ["SELECT"]
}

default_privilege {
  for_role    = "app_owner"
  schema      = "app"
  object_type = "TABLES"
  to          = "app_reader"
  privileges  = ["INSERT"]
  grantable   = ["INSERT"]
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.DefaultPrivileges, qt.HasLen, 1)
	c.Assert(db.DefaultPrivileges[0].Privileges, qt.DeepEquals, []schemamodel.PrivilegeGrant{
		{Privilege: "SELECT"},
		{Privilege: "INSERT", WithOption: true},
	})
}

// TestParseDefaultPrivilege_KeepsTwoGrantorsApart is what makes the grantor part
// of the identity rather than a note on the side.
//
// Read as one object, the second declaration would merge into the first and the
// privileges granted by one role would be planned against the other.
func TestParseDefaultPrivilege_KeepsTwoGrantorsApart(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
default_privilege {
  for_role    = "owner_a"
  schema      = "app"
  object_type = "TABLES"
  to          = "app_reader"
  privileges  = ["SELECT"]
}

default_privilege {
  for_role    = "owner_b"
  schema      = "app"
  object_type = "TABLES"
  to          = "app_reader"
  privileges  = ["SELECT"]
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.DefaultPrivileges, qt.HasLen, 2)
	c.Assert(db.DefaultPrivileges[0].Grantor, qt.Equals, "owner_a")
	c.Assert(db.DefaultPrivileges[1].Grantor, qt.Equals, "owner_b")
}
