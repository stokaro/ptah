package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/dbschematogo"
)

// The database conversion preserves every owner identity and per-right option.
// Different grantors, object classes, and decoded role names stay separate.
func TestConvert_DefaultPrivilegeObjectsKeepTheirIdentity(t *testing.T) {
	c := qt.New(t)
	var current, want []schemaext.Object
	for _, identity := range []pgdefaultprivilege.Identity{
		{Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"},
		{Grantor: "other_owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"},
		{Grantor: "app_owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "reader"},
		{Grantor: "app_owner ", Schema: "app", ObjectType: "TABLES", Grantee: "reader"},
	} {
		ref := must.Must(pgdefaultprivilege.Ref(identity, identifier.ForDialect("postgres")))
		current = append(current, must.Must(pgdefaultprivilege.ObservedObject(ref, pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}})))
		want = append(want, must.Must(pgdefaultprivilege.DesiredObject(ref, pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}})))
	}
	original := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(current...)), FeatureCoverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Observed))}
	converted, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), original, "postgres", builtintest.Runtime())
	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(converted.FeatureObjects.All()), qt.ContentEquals, want)
	objects := must.Must(converted.FeatureObjects.All())
	objects[0].Value.(*pgdefaultprivilege.Desired).Privileges[0].WithOption = false
	c.Assert(must.Must(original.FeatureObjects.All()), qt.ContentEquals, current)
}

func TestConvert_DescribesNoDefaultPrivilegeWhenTheReadFoundNone(t *testing.T) {
	c := qt.New(t)
	converted, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), &catalog.Database{
		FeatureCoverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Observed)),
	}, "postgres", builtintest.Runtime())
	c.Assert(err, qt.IsNil)
	c.Assert(converted.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(converted.FeatureCoverage.Lookup(pgdefaultprivilege.DefaultPrivilegeKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
}
