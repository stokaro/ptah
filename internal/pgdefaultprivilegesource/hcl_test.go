package pgdefaultprivilegesource_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/builtintest"
)

const hclIdentity = "  for_role = role.owner\n  object_type = \"TABLES\"\n  to = role.reader\n"

func parseHCL(text string) (*schemamodel.Database, error) {
	return atlashcl.ParseWithOptions([]byte(text), "privileges.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
}

func hclBlock(body string) string {
	return "default_privilege {\n" + body + "\n}\n"
}

func TestHCL_GrantOptionsBelongToEachPrivilege(t *testing.T) {
	tests := []struct {
		name, grantable string
		want            []pgdefaultprivilege.Grant
	}{
		{name: "plain", want: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}, {Privilege: "SELECT"}}},
		{name: "one option", grantable: `grantable = ["select"]`, want: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}, {Privilege: "SELECT", WithOption: true}}},
		{name: "all options", grantable: `grantable = [SELECT, INSERT]`, want: []pgdefaultprivilege.Grant{{Privilege: "INSERT", WithOption: true}, {Privilege: "SELECT", WithOption: true}}},
		{name: "duplicate option", grantable: `grantable = [INSERT, INSERT]`, want: []pgdefaultprivilege.Grant{{Privilege: "INSERT", WithOption: true}, {Privilege: "SELECT"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := parseHCL(hclBlock(hclIdentity + "schema = schema.app\nprivileges = [SELECT, \"insert\", SELECT]\ncomment = \"new tables\"\n" + test.grantable))
			c.Assert(err, qt.IsNil)
			objects := must.Must(db.FeatureObjects.All())
			c.Assert(objects, qt.HasLen, 1)
			c.Assert(must.Must(pgdefaultprivilege.IdentityOf(objects[0].Ref)), qt.DeepEquals, pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"})
			c.Assert(objects[0].Value, qt.DeepEquals, &pgdefaultprivilege.Desired{Privileges: test.want, Comment: "new tables"})
			c.Assert(db.FeatureCoverage.Lookup(pgdefaultprivilege.DefaultPrivilegeKind, objects[0].Ref).State, qt.Equals, schemaext.Complete)
		})
	}
}

func TestHCL_EvaluatesInputsAndCombinesDeclarations(t *testing.T) {
	c := qt.New(t)
	input := `variable "rights" {
  type = list(string)
  default = ["select", "INSERT"]
}
locals {
  owner = "owner.with.dot"
  reader = "reader.with.dot"
}
`
	identity := "for_role = local.owner\nobject_type = \"TABLES\"\nto = local.reader\n"
	db, err := parseHCL(input + hclBlock(identity+"privileges = var.rights") + hclBlock(identity+"privileges = [upper(\"select\")]\ngrantable = \"SELECT\"\nrevoked = [DELETE]"))
	c.Assert(err, qt.IsNil)
	objects := must.Must(db.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(must.Must(pgdefaultprivilege.IdentityOf(objects[0].Ref)), qt.DeepEquals, pgdefaultprivilege.Identity{Grantor: "owner.with.dot", ObjectType: "TABLES", Grantee: "reader.with.dot"})
	c.Assert(objects[0].Value, qt.DeepEquals, &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}, {Privilege: "SELECT", WithOption: true}}, Revoked: []string{"DELETE"}})
}

func TestHCL_IdentitySeparatesGrantorsAndGlobalScope(t *testing.T) {
	c := qt.New(t)
	block := hclBlock(hclIdentity + "privileges = SELECT")
	db := must.Must(parseHCL(block + strings.ReplaceAll(block, "role.owner", "role.another") + hclBlock(hclIdentity+"schema = schema.public\nprivileges = SELECT")))
	objects := must.Must(db.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 3)
	identities := make([]pgdefaultprivilege.Identity, 0, len(objects))
	for _, object := range objects {
		identities = append(identities, must.Must(pgdefaultprivilege.IdentityOf(object.Ref)))
	}
	c.Assert(identities, qt.ContentEquals, []pgdefaultprivilege.Identity{
		{Grantor: "owner", ObjectType: "TABLES", Grantee: "reader"},
		{Grantor: "another", ObjectType: "TABLES", Grantee: "reader"},
		{Grantor: "owner", Schema: "public", ObjectType: "TABLES", Grantee: "reader"},
	})
}

func TestHCL_RequiresSelectedOwner(t *testing.T) {
	c := qt.New(t)
	db, err := atlashcl.ParseWithOptions([]byte(hclBlock(hclIdentity+"privileges = [SELECT]")), "privileges.hcl", atlashcl.Options{HCL: hclext.None()})
	c.Assert(err, qt.ErrorMatches, `(?s).*unsupported.*default_privilege.*`)
	c.Assert(db, qt.IsNil)
}
