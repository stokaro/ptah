package pgdefaultprivilegesource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/pgdefaultprivilegesource"
)

func TestSourceLimits_RoundTripPreservesUnknownScopes(t *testing.T) {
	semantics := identifier.ForDialect("postgres")
	object := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: ` owner," `, ObjectType: "TABLES", Grantee: ` owner," `}, semantics))
	class := must.Must(pgdefaultprivilege.ClassRef(pgdefaultprivilege.Class{Schema: " app ", ObjectType: "TABLES"}, semantics))
	tests := []struct {
		name      string
		namespace schemaext.Knowledge
		subjects  []objectidentity.ID
	}{
		{name: "refused catalog", namespace: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "catalog read refused"}},
		{name: "partial owner and FOR ALL ROLES", namespace: schemaext.Knowledge{State: schemaext.Complete}, subjects: []objectidentity.ID{class, object}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var subjects []schemaext.SubjectCoverage
			for _, subject := range test.subjects {
				subjects = append(subjects, schemaext.SubjectCoverage{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "catalog ACL is incomplete"}})
			}
			original := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "owner"}}, FeatureCoverage: must.Must(pgdefaultprivilege.Coverage(schemaext.Desired, test.namespace, subjects))}
			files, err := goschematogo.Render(t.Context(), original, goschematogo.Options{Annotations: goOwners, SingleFile: true})
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 1)
			goRead, err := goschema.ParseSource(goOwners, files[0].Name, files[0].Data)
			c.Assert(err, qt.IsNil)
			hcl, err := atlashclrender.RenderInspected(original, "public", atlashclrender.Options{HCL: builtintest.Runtime().HCL()})
			c.Assert(err, qt.IsNil)
			hclRead, err := parseHCL(t.Context(), string(hcl.Data))
			c.Assert(err, qt.IsNil)
			for _, read := range []*schemamodel.Database{&goRead, hclRead} {
				known := read.FeatureCoverage.SelectKinds([]schemaext.Kind{pgdefaultprivilege.DefaultPrivilegeKind})
				c.Assert(known.KindRecords(), qt.HasLen, 1)
				c.Assert(known.KindRecords()[0].Knowledge.State == schemaext.Complete, qt.Equals, test.namespace.State == schemaext.Complete)
				c.Assert(known.SubjectRecords(), qt.HasLen, len(test.subjects))
				for _, subject := range test.subjects {
					c.Assert(known.Lookup(pgdefaultprivilege.DefaultPrivilegeKind, subject).State, qt.Equals, schemaext.Uninspected)
				}
			}
			c.Assert(hclRead.FeatureCoverage.SelectKinds([]schemaext.Kind{pgdefaultprivilege.DefaultPrivilegeKind}).SubjectRecords(), qt.DeepEquals, goRead.FeatureCoverage.SubjectRecords())
			again := must.Must(goschematogo.Render(t.Context(), &goRead, goschematogo.Options{Annotations: goOwners, SingleFile: true}))
			c.Assert(again, qt.DeepEquals, files)
		})
	}
}

func TestSQLLimits_ReachTheOwner(t *testing.T) {
	c := qt.New(t)
	directive := (coverage.Object{Kind: pgdefaultprivilegesource.LimitKind, Name: `["owner","app","TABLES","reader"]`}).Directive()
	read := readSQL(c, "-- "+directive+"\n"+alterDefault+"GRANT SELECT ON TABLES TO reader;")
	objects := must.Must(read.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(read.FeatureCoverage.Lookup(pgdefaultprivilege.DefaultPrivilegeKind, objects[0].Ref).State, qt.Equals, schemaext.Uninspected)
	c.Assert(read.NotDescribed.IsZero(), qt.IsTrue)
}

func TestSourceLimits_RefuseMalformedIdentity(t *testing.T) {
	for _, name := range []string{`["owner","app"]`, `[null,"app","TABLES"]`, `["owner","app","UNKNOWN","reader"]`, `owner`} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			known, err := goOwners.Coverage(coverage.Object{Kind: pgdefaultprivilegesource.LimitKind, Name: name})
			c.Assert(err, qt.IsNotNil)
			c.Assert(known.IsZero(), qt.IsTrue)
		})
	}
}

func TestSourceLimits_RefuseLossOfCompleteException(t *testing.T) {
	c := qt.New(t)
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "reader"}, identifier.ForDialect("postgres")))
	known := must.Must(pgdefaultprivilege.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "partial source"}, []schemaext.SubjectCoverage{
		{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
	}))
	encoded, err := goOwners.Encode(t.Context(), annotation.EncodeRequest{Coverage: known})
	c.Assert(err, qt.ErrorMatches, `.*cannot preserve a complete subject inside an unknown namespace`)
	c.Assert(encoded, qt.DeepEquals, annotation.EncodeResult{})
}
