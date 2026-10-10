package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/internal/atlasfilter"
)

func secretNames(objects schemaext.Objects) []string {
	var names []string
	for _, ref := range objects.Select(func(ref objectidentity.ID) bool { return ref.Kind == objectidentity.Kind(ydbsecret.Kind) }).Refs() {
		names = append(names, ydbsecret.Display(ref.Schema.Source, ref.Name.Source))
	}
	return names
}

// TestScope_Secrets selects a YDB secret on its own name, in the directory
// that holds it, on both sides of a comparison: an include, an exclusion by
// name and an excluded directory each keep the same secrets in the
// declaration and in the read.
func TestScope_Secrets(t *testing.T) {
	tests := []struct {
		name  string
		scope atlasfilter.Scope
		want  []string
	}{
		{name: "include", scope: atlasfilter.Scope{Include: []string{"pg[type=secret]"}}, want: []string{"pg"}},
		{name: "include in a directory", scope: atlasfilter.Scope{Include: []string{"ext.s3[type=secret]"}}, want: []string{"ext/s3"}},
		{name: "exclude by name", scope: atlasfilter.Scope{Exclude: []string{"pg[type=secret]"}}, want: []string{"ext/s3"}},
		{name: "schema", scope: atlasfilter.Scope{Schemas: []string{"ext"}}, want: []string{"ext/s3"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
				ydbsecret.DesiredObject("", "pg", "", "PTAH_SECRET_PG"), ydbsecret.DesiredObject("ext", "s3", "", "PTAH_SECRET_S3")))}
			held := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(
				ydbsecret.ObservedObject("", "pg"), ydbsecret.ObservedObject("ext", "s3")))}

			generated, err := atlasfilter.ScopeGenerated(declared, test.scope)
			c.Assert(err, qt.IsNil)
			live, err := atlasfilter.ScopeDatabase(held, test.scope)
			c.Assert(err, qt.IsNil)

			c.Assert(secretNames(generated.FeatureObjects), qt.DeepEquals, test.want)
			c.Assert(secretNames(live.FeatureObjects), qt.DeepEquals, test.want)
			c.Assert(declared.FeatureObjects.Len(), qt.Equals, 2)
			c.Assert(held.FeatureObjects.Len(), qt.Equals, 2)
		})
	}
}

// TestExclude_Secrets drops an excluded secret from the declaration as well as
// from the read, so a secret the database holds is neither dropped nor created
// again.
func TestExclude_Secrets(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		ydbsecret.DesiredObject("", "pg", "", "PTAH_SECRET_PG"), ydbsecret.DesiredObject("ext", "s3", "", "PTAH_SECRET_S3")))}
	held := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		ydbsecret.ObservedObject("", "pg"), ydbsecret.ObservedObject("ext", "s3")))}

	generated, err := atlasfilter.ExcludeGenerated(declared, []string{"ext.*[type=secret]"})
	c.Assert(err, qt.IsNil)
	live, err := atlasfilter.ExcludeDatabase(held, []string{"ext.*[type=secret]"})
	c.Assert(err, qt.IsNil)

	c.Assert(secretNames(generated.FeatureObjects), qt.DeepEquals, []string{"pg"})
	c.Assert(secretNames(live.FeatureObjects), qt.DeepEquals, []string{"pg"})
}
