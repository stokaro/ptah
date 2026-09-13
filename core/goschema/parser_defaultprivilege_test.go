package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

func TestDefaultPrivilegeAnnotationParsing(t *testing.T) {
	t.Run("grantable names the subset that carries the grant option", func(t *testing.T) {
		c := qt.New(t)
		goCode := `
package test

//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="TABLES" grantee="app_reader" privileges="SELECT,INSERT" grantable="INSERT" comment="Future tables"
type AccessControl struct {
}
`
		database := parseStringAsGoFile(c, goCode)

		c.Assert(database.DefaultPrivileges, qt.HasLen, 1)
		privilege := database.DefaultPrivileges[0]
		c.Assert(privilege.StructName, qt.Equals, "AccessControl")
		c.Assert(privilege.Grantor, qt.Equals, "app_owner")
		c.Assert(privilege.Schema, qt.Equals, "app")
		c.Assert(privilege.ObjectType, qt.Equals, "TABLES")
		c.Assert(privilege.Grantee, qt.Equals, "app_reader")
		c.Assert(privilege.Privileges, qt.DeepEquals, []schemamodel.PrivilegeGrant{
			{Privilege: "SELECT", WithOption: false},
			{Privilege: "INSERT", WithOption: true},
		})
		c.Assert(privilege.Comment, qt.Equals, "Future tables")
		c.Assert(privilege.Dialects, qt.IsNil)
	})

	t.Run("an omitted grantable list leaves every privilege plain", func(t *testing.T) {
		c := qt.New(t)
		goCode := `
package test

//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="SEQUENCES" grantee="PUBLIC" privileges="USAGE,SELECT"
type AccessControl struct {
}
`
		database := parseStringAsGoFile(c, goCode)

		c.Assert(database.DefaultPrivileges, qt.HasLen, 1)
		c.Assert(database.DefaultPrivileges[0].Grantee, qt.Equals, "PUBLIC")
		c.Assert(database.DefaultPrivileges[0].Privileges, qt.DeepEquals, []schemamodel.PrivilegeGrant{
			{Privilege: "USAGE", WithOption: false},
			{Privilege: "SELECT", WithOption: false},
		})
	})

	// The keywords reach the renderer verbatim, so a lower-case spelling that
	// survived would render `ALTER DEFAULT PRIVILEGES ... ON functions`, and the
	// comparator would read the declaration and the catalog's `FUNCTIONS` as two
	// different objects forever.
	t.Run("the object type and the privileges are upper-cased", func(t *testing.T) {
		c := qt.New(t)
		goCode := `
package test

//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="functions" grantee="app_reader" privileges="execute" grantable="Execute"
type AccessControl struct {
}
`
		database := parseStringAsGoFile(c, goCode)

		c.Assert(database.DefaultPrivileges, qt.HasLen, 1)
		c.Assert(database.DefaultPrivileges[0].ObjectType, qt.Equals, "FUNCTIONS")
		c.Assert(database.DefaultPrivileges[0].Privileges, qt.DeepEquals, []schemamodel.PrivilegeGrant{
			{Privilege: "EXECUTE", WithOption: true},
		})
	})

	// A scope kept as `postgresql` while the target calls itself `postgres`
	// would omit the object from the one dialect that has the statement, and
	// nothing would say so.
	t.Run("the dialect scope is canonicalized", func(t *testing.T) {
		c := qt.New(t)
		goCode := `
package test

//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="TYPES" grantee="app_reader" privileges="USAGE" dialects="postgresql"
type AccessControl struct {
}
`
		database := parseStringAsGoFile(c, goCode)

		c.Assert(database.DefaultPrivileges, qt.HasLen, 1)
		c.Assert(database.DefaultPrivileges[0].Dialects, qt.DeepEquals, []string{"postgres"})
	})

	// Identity is (grantor, schema, object type, grantee). Two declarations that
	// differ only in object type are two objects, and folding them would drop
	// one of the two statements.
	t.Run("two object types on one struct stay two declarations", func(t *testing.T) {
		c := qt.New(t)
		goCode := `
package test

//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="TABLES" grantee="app_reader" privileges="SELECT"
//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="SEQUENCES" grantee="app_reader" privileges="USAGE"
type AccessControl struct {
}
`
		database := parseStringAsGoFile(c, goCode)

		c.Assert(database.DefaultPrivileges, qt.HasLen, 2)
		c.Assert(database.DefaultPrivileges[0].ObjectType, qt.Equals, "TABLES")
		c.Assert(database.DefaultPrivileges[1].ObjectType, qt.Equals, "SEQUENCES")
	})
}

func TestDefaultPrivilegeAnnotationParsing_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
		wantIs     error
		wantText   string
	}{
		{
			name:       "an object type outside the closed set",
			annotation: `//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="ROUTINES" grantee="app_reader" privileges="EXECUTE"`,
			wantIs:     ptaherr.ErrInvalidAttributeValue,
			wantText:   `invalid "object_type" value "ROUTINES" on //ptah:schema:defaultprivilege at AccessControl: must be one of TABLES, SEQUENCES, FUNCTIONS, TYPES`,
		},
		{
			name:       "the cluster-wide form, which SCHEMAS would spell",
			annotation: `//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="SCHEMAS" grantee="app_reader" privileges="USAGE"`,
			wantIs:     ptaherr.ErrInvalidAttributeValue,
			wantText:   `invalid "object_type" value "SCHEMAS" on //ptah:schema:defaultprivilege at AccessControl: must be one of TABLES, SEQUENCES, FUNCTIONS, TYPES`,
		},
		{
			name:       "a grantable privilege that is not granted",
			annotation: `//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="TABLES" grantee="app_reader" privileges="SELECT" grantable="INSERT"`,
			wantIs:     ptaherr.ErrInvalidAttributeValue,
			wantText:   `invalid "grantable" value "INSERT" on //ptah:schema:defaultprivilege at AccessControl: "INSERT" is not in "SELECT"`,
		},
		{
			name:       "a missing grantee",
			annotation: `//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="TABLES" privileges="SELECT"`,
			wantIs:     ptaherr.ErrMissingRequiredAttribute,
			wantText:   `missing required annotation attribute "grantee" on //ptah:schema:defaultprivilege at AccessControl`,
		},
		{
			name:       "a missing privilege list",
			annotation: `//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="TABLES" grantee="app_reader"`,
			wantIs:     ptaherr.ErrMissingRequiredAttribute,
			wantText:   `missing required annotation attribute "privileges" on //ptah:schema:defaultprivilege at AccessControl`,
		},
		{
			name:       "a missing schema, which the model has no spelling for",
			annotation: `//ptah:schema:defaultprivilege for_role="app_owner" object_type="TABLES" grantee="app_reader" privileges="SELECT"`,
			wantIs:     ptaherr.ErrMissingRequiredAttribute,
			wantText:   `missing required annotation attribute "schema" on //ptah:schema:defaultprivilege at AccessControl`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := goschema.ParseSource("schema.go", "package test\n\n"+test.annotation+"\ntype AccessControl struct{}\n")

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err.Error(), qt.Contains, test.wantText)
			c.Assert(database.DefaultPrivileges, qt.HasLen, 0)
		})
	}
}
