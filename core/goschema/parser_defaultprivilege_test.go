package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/pgdefaultprivilegesource"
)

var defaultPrivilegeOwners = must.Must(annotation.NewSet(pgdefaultprivilegesource.Annotations()))

type defaultPrivilegeDeclaration struct {
	Identity pgdefaultprivilege.Identity
	Value    pgdefaultprivilege.Desired
	Targets  []string
}

func defaultPrivilegeDeclarations(c *qt.C, database schemamodel.Database) []defaultPrivilegeDeclaration {
	c.Helper()
	objects, err := database.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	var declarations []defaultPrivilegeDeclaration
	for _, object := range objects {
		identity, err := pgdefaultprivilege.IdentityOf(object.Ref)
		c.Assert(err, qt.IsNil)
		value, ok := object.Value.(*pgdefaultprivilege.Desired)
		c.Assert(ok, qt.IsTrue)
		declarations = append(declarations, defaultPrivilegeDeclaration{Identity: identity, Value: *value, Targets: object.Targets})
	}
	return declarations
}

func TestDefaultPrivilegeAnnotationParsing(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		want       defaultPrivilegeDeclaration
	}{
		{
			name:       "grantable names a subset",
			attributes: `schema="app" object_type="TABLES" grantee="app_reader" privileges="SELECT,INSERT" grantable="INSERT" comment="Future tables"`,
			want: defaultPrivilegeDeclaration{
				Identity: pgdefaultprivilege.Identity{Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader"},
				Value:    pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "INSERT", WithOption: true}, {Privilege: "SELECT"}}, Comment: "Future tables"},
			},
		},
		{
			name:       "omitted grantable keeps plain privileges",
			attributes: `schema="app" object_type="SEQUENCES" grantee="PUBLIC" privileges="USAGE,SELECT"`,
			want: defaultPrivilegeDeclaration{
				Identity: pgdefaultprivilege.Identity{Grantor: "app_owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "PUBLIC"},
				Value:    pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}, {Privilege: "USAGE"}}},
			},
		},
		{
			name:       "keywords are canonicalized",
			attributes: `schema="app" object_type="functions" grantee="app_reader" privileges="execute" grantable="Execute"`,
			want: defaultPrivilegeDeclaration{
				Identity: pgdefaultprivilege.Identity{Grantor: "app_owner", Schema: "app", ObjectType: "FUNCTIONS", Grantee: "app_reader"},
				Value:    pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "EXECUTE", WithOption: true}}},
			},
		},
		{
			name:       "dialect alias selects postgres",
			attributes: `schema="app" object_type="TYPES" grantee="app_reader" privileges="USAGE" dialects="postgresql"`,
			want: defaultPrivilegeDeclaration{
				Identity: pgdefaultprivilege.Identity{Grantor: "app_owner", Schema: "app", ObjectType: "TYPES", Grantee: "app_reader"},
				Value:    pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "USAGE"}}}, Targets: []string{"postgres"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := "package test\n\n//ptah:schema:defaultprivilege for_role=\"app_owner\" " + test.attributes + "\ntype AccessControl struct{}\n"
			database, err := goschema.ParseSource(defaultPrivilegeOwners, "schema.go", source)
			c.Assert(err, qt.IsNil)
			c.Assert(defaultPrivilegeDeclarations(c, database), qt.DeepEquals, []defaultPrivilegeDeclaration{test.want})
			c.Assert(database.FeatureCoverage.Lookup(pgdefaultprivilege.DefaultPrivilegeKind, database.FeatureObjects.Refs()[0]).State, qt.Equals, schemaext.Complete)
		})
	}
}

func TestDefaultPrivilegeAnnotationParsing_GlobalScopes(t *testing.T) {
	c := qt.New(t)
	source := `package test

//ptah:schema:defaultprivilege for_role="app_owner" object_type="FUNCTIONS" grantee="PUBLIC" revoked="EXECUTE"
//ptah:schema:defaultprivilege for_role="app_owner" object_type="SCHEMAS" grantee="app_reader" privileges="USAGE"
type AccessControl struct{}
`
	database, err := goschema.ParseSource(defaultPrivilegeOwners, "schema.go", source)
	c.Assert(err, qt.IsNil)
	c.Assert(defaultPrivilegeDeclarations(c, database), qt.ContentEquals, []defaultPrivilegeDeclaration{
		{Identity: pgdefaultprivilege.Identity{Grantor: "app_owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC"}, Value: pgdefaultprivilege.Desired{Revoked: []string{"EXECUTE"}}},
		{Identity: pgdefaultprivilege.Identity{Grantor: "app_owner", ObjectType: "SCHEMAS", Grantee: "app_reader"}, Value: pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "USAGE"}}}},
	})
}

func TestDefaultPrivilegeAnnotationParsing_DistinctClasses(t *testing.T) {
	c := qt.New(t)
	source := `package test

//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="TABLES" grantee="app_reader" privileges="SELECT"
//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="SEQUENCES" grantee="app_reader" privileges="USAGE"
type AccessControl struct{}
`
	database, err := goschema.ParseSource(defaultPrivilegeOwners, "schema.go", source)
	c.Assert(err, qt.IsNil)
	c.Assert(defaultPrivilegeDeclarations(c, database), qt.ContentEquals, []defaultPrivilegeDeclaration{
		{Identity: pgdefaultprivilege.Identity{Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader"}, Value: pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
		{Identity: pgdefaultprivilege.Identity{Grantor: "app_owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "app_reader"}, Value: pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "USAGE"}}}},
	})
}

func TestDefaultPrivilegeAnnotationParsing_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		wantText   string
	}{
		{name: "unsupported class alias", attributes: `schema="app" object_type="ROUTINES" grantee="app_reader" privileges="EXECUTE"`, wantText: `unsupported default privilege object type "ROUTINES"`},
		{name: "global class with schema", attributes: `schema="app" object_type="SCHEMAS" grantee="app_reader" privileges="USAGE"`, wantText: `default privileges on SCHEMAS cannot name a schema`},
		{name: "unsupported class", attributes: `object_type="VIEWS" grantee="app_reader" privileges="SELECT"`, wantText: `unsupported default privilege object type "VIEWS"`},
		{name: "ungranted option", attributes: `schema="app" object_type="TABLES" grantee="app_reader" privileges="SELECT" grantable="INSERT"`, wantText: `grantable privilege "INSERT" is not in privileges`},
		{name: "empty intent", attributes: `schema="app" object_type="TABLES" grantee="app_reader"`, wantText: `a default privilege requires privileges or revoked`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := "package test\n\n//ptah:schema:defaultprivilege for_role=\"app_owner\" " + test.attributes + "\ntype AccessControl struct{}\n"
			database, err := goschema.ParseSource(defaultPrivilegeOwners, "schema.go", source)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err.Error(), qt.Contains, test.wantText)
			c.Assert(database.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

func TestDefaultPrivilegeAnnotationParsing_MetadataFailures(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		want       error
		message    string
	}{
		{name: "missing grantee", attributes: `privileges="SELECT"`, want: ptaherr.ErrMissingRequiredAttribute, message: `missing required annotation attribute "grantee"`},
		{name: "unknown attribute", attributes: `grantee="reader" privileges="SELECT" bogus="x"`, want: ptaherr.ErrUnknownAttribute, message: `unknown annotation attribute "bogus"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := "package test\n\n//ptah:schema:defaultprivilege for_role=\"owner\" object_type=\"TABLES\" " + test.attributes + "\ntype AccessControl struct{}\n"
			database, err := goschema.ParseSource(defaultPrivilegeOwners, "schema.go", source)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(err.Error(), qt.Contains, test.message)
			c.Assert(database.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}
