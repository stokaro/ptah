package pgdefaultprivilegesource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/pgdefaultprivilegesource"
)

func TestGoExport_RoundTripPreservesGlobalScopeAndOptions(t *testing.T) {
	c := qt.New(t)
	original := must.Must(yamlschema.Parse(yamlOwners, []byte(yamlPrefix+"    privileges: [SELECT, INSERT]\n    grantable: SELECT\n    revoked: DELETE\n    dialects: [postgres, cockroachdb]\n    comment: 'A quoted \"grant\"'\n")))
	files, err := goschematogo.Render(t.Context(), original, goschematogo.Options{Annotations: goOwners, SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(string(files[0].Data), qt.Contains, `grantable="SELECT"`)
	read, err := goschema.ParseSource(goOwners, files[0].Name, files[0].Data)
	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(read.FeatureObjects.All()), qt.DeepEquals, must.Must(original.FeatureObjects.All()))
	c.Assert(read.FeatureCoverage.KindRecords(), qt.DeepEquals, original.FeatureCoverage.KindRecords())
	again, err := goschematogo.Render(t.Context(), &read, goschematogo.Options{Annotations: goOwners, SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(again, qt.DeepEquals, files)
}

func TestRetention_CombinesDeclarationsAndPreservesScope(t *testing.T) {
	c := qt.New(t)
	object := must.Must(pgdefaultprivilegesource.Declare(pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "reader"}, []string{"INSERT", "SELECT"}, []string{"SELECT"}, nil, "", []string{"postgres"}))
	request := annotation.RetentionRequest{Objects: must.Must(schemaext.NewObjects(object)), Declarations: []annotation.Declaration{
		{Directive: pgdefaultprivilegesource.Directive, Attributes: map[string]string{"for_role": "owner", "object_type": "TABLES", "grantee": "reader", "privileges": "INSERT", "dialects": "postgresql"}},
		{Directive: pgdefaultprivilegesource.Directive, Attributes: map[string]string{"for_role": "owner", "object_type": "TABLES", "grantee": "reader", "privileges": "SELECT", "grantable": "SELECT", "dialects": "postgres"}},
	}}
	got, err := goOwners.Retained(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []bool{true, true})
	object.Targets = nil
	request.Objects = must.Must(schemaext.NewObjects(object))
	got, err = goOwners.Retained(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []bool{false, false})
}

func TestRetention_RefusesDroppedIntent(t *testing.T) {
	tests := []struct {
		name                     string
		grants, options, revoked []string
		comment                  string
	}{
		{name: "grant option", grants: []string{"SELECT"}, revoked: []string{"DELETE"}, comment: "Read"},
		{name: "revoked privilege", grants: []string{"SELECT"}, options: []string{"SELECT"}, comment: "Read"},
		{name: "comment", grants: []string{"SELECT"}, options: []string{"SELECT"}, revoked: []string{"DELETE"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			object := must.Must(pgdefaultprivilegesource.Declare(pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "reader"}, test.grants, test.options, test.revoked, test.comment, nil))
			got, err := goOwners.Retained(t.Context(), annotation.RetentionRequest{Objects: must.Must(schemaext.NewObjects(object)), Declarations: []annotation.Declaration{{Directive: pgdefaultprivilegesource.Directive, Attributes: map[string]string{
				"for_role": "owner", "object_type": "TABLES", "grantee": "reader", "privileges": "SELECT", "grantable": "SELECT", "revoked": "DELETE", "comment": "Read",
			}}}})
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, []bool{false})
		})
	}
}
