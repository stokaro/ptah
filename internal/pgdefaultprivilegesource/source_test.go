package pgdefaultprivilegesource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/pgdefaultprivilegesource"
)

var goOwners = must.Must(annotation.NewSet(pgdefaultprivilegesource.Annotations()))
var yamlOwners = must.Must(yamlext.NewSet(pgdefaultprivilegesource.YAML()))

func goSource(body string) string {
	return "package models\n\n" + body + "\ntype Privileges struct{}\n"
}

const annotationPrefix = `//ptah:schema:defaultprivilege for_role="owner" object_type="TABLES" grantee="reader"`
const yamlPrefix = "default_privileges:\n  reader:\n    for_role: owner\n    object_type: TABLES\n    grantee: reader\n"

func TestSources_PreservePrivilegesScopesAndComments(t *testing.T) {
	c := qt.New(t)
	fromGo, err := goschema.ParseSource(goOwners, "privileges.go", goSource(annotationPrefix+` schema="app" privileges="select,insert" grantable="select" revoked="delete" comment="Read and write" dialects="postgresql,cockroachdb"`))
	c.Assert(err, qt.IsNil)
	fromYAML, err := yamlschema.Parse(yamlOwners, []byte(yamlPrefix+"    schema: app\n    privileges: [select, insert]\n    grantable: select\n    revoked: delete\n    comment: Read and write\n    dialects: [postgresql, cockroachdb]\n"))
	c.Assert(err, qt.IsNil)
	c.Assert(fromGo.FeatureObjects.Equal(fromYAML.FeatureObjects), qt.IsTrue)
	objects := must.Must(fromGo.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Targets, qt.DeepEquals, []string{"cockroachdb", "postgres"})
	c.Assert(must.Must(pgdefaultprivilege.IdentityOf(objects[0].Ref)), qt.DeepEquals, pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"})
	c.Assert(objects[0].Value, qt.DeepEquals, &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}, {Privilege: "SELECT", WithOption: true}}, Revoked: []string{"DELETE"}, Comment: "Read and write"})
	c.Assert(fromGo.FeatureCoverage.Lookup(pgdefaultprivilege.DefaultPrivilegeKind, objects[0].Ref).State, qt.Equals, schemaext.Complete)
	c.Assert(fromYAML.FeatureCoverage.Lookup(pgdefaultprivilege.DefaultPrivilegeKind, objects[0].Ref).State, qt.Equals, schemaext.Complete)
}

func TestSources_CombineIndependentDeclarations(t *testing.T) {
	c := qt.New(t)
	fromGo := must.Must(goschema.ParseSource(goOwners, "privileges.go", goSource(annotationPrefix+` privileges="SELECT"`+"\n"+annotationPrefix+` privileges="INSERT"`)))
	fromYAML := must.Must(yamlschema.Parse(yamlOwners, []byte(yamlPrefix+"    privileges: SELECT\n    grantable: SELECT\n  second:\n    for_role: owner\n    object_type: TABLES\n    grantee: reader\n    privileges: UPDATE\n")))
	merged, err := schemamodel.Merge(&fromGo, fromYAML)
	c.Assert(err, qt.IsNil)
	objects := must.Must(merged.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Value, qt.DeepEquals, &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}, {Privilege: "SELECT", WithOption: true}, {Privilege: "UPDATE"}}})
	c.Assert(must.Must(pgdefaultprivilege.IdentityOf(objects[0].Ref)).Schema, qt.Equals, "")
}

func TestSources_RefuseInvalidDeclarations(t *testing.T) {
	tests := []struct{ name, goAttributes, yamlAttributes, wantErr string }{
		{name: "empty intent", wantErr: "requires privileges or revoked"},
		{name: "ungranted option", wantErr: "grantable privilege.*is not in privileges", goAttributes: ` privileges="SELECT" grantable="INSERT"`, yamlAttributes: "    privileges: SELECT\n    grantable: INSERT\n"},
		{name: "contradictory all", wantErr: "both granted and revoked", goAttributes: ` privileges="ALL" revoked="SELECT"`, yamlAttributes: "    privileges: ALL\n    revoked: SELECT\n"},
		{name: "unknown right", wantErr: "EXECUTE.*not valid on TABLES", goAttributes: ` privileges="EXECUTE"`, yamlAttributes: "    privileges: EXECUTE\n"},
		{name: "empty scope", wantErr: "names no dialect", goAttributes: ` privileges="SELECT" dialects=""`, yamlAttributes: "    privileges: SELECT\n    dialects: []\n"},
		{name: "unknown scope", wantErr: "names no supported dialect", goAttributes: ` privileges="SELECT" dialects="postgress"`, yamlAttributes: "    privileges: SELECT\n    dialects: postgress\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fromGo, err := goschema.ParseSource(goOwners, "privileges.go", goSource(annotationPrefix+test.goAttributes))
			c.Assert(err, qt.ErrorMatches, "(?s).*"+test.wantErr+".*")
			c.Assert(fromGo.FeatureObjects.IsZero(), qt.IsTrue)
			fromYAML, err := yamlschema.Parse(yamlOwners, []byte(yamlPrefix+test.yamlAttributes))
			c.Assert(err, qt.ErrorMatches, "(?s).*"+test.wantErr+".*")
			c.Assert(fromYAML, qt.IsNil)
		})
	}
}
