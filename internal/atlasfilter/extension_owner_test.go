package atlasfilter_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/atlasfilter"
)

// ownedTrio is three installed extensions: one in the default schema, one in
// a schema of its own, and one in another schema nothing else uses.
var ownedTrio = []catalog.Extension{
	{Name: "pg_trgm", Schema: "public", Version: "1.6"},
	{Name: "citext", Schema: "extensions", Version: "1.6"},
	{Name: "isn", Schema: "audit", Version: "1.2"},
}

// readBoth returns one read twice: with extensions in the common list, and
// with the same extensions as the extension owner's observations.
func readBoth(extensions []catalog.Extension) (common, owned *catalog.Database) {
	base := func() *catalog.Database {
		return &catalog.Database{
			Schemas: []catalog.Schema{{Name: "app"}, {Name: "audit"}, {Name: "extensions"}, {Name: "public"}},
			Tables:  []catalog.Table{{Schema: "app", Name: "users", Columns: []catalog.Column{{Name: "id", DataType: "integer"}}}},
		}
	}
	common = base()
	common.Extensions = slices.Clone(extensions)
	owned = base()
	var objects []schemaext.Object
	for _, extension := range extensions {
		objects = append(objects, must.Must(pgextension.ObservedObject(pgextension.Ref(extension.Name),
			pgextension.ObservedExtension{Schema: extension.Schema, Version: extension.Version})))
	}
	owned.FeatureObjects = must.Must(schemaext.NewObjects(objects...))
	owned.FeatureCoverage = must.Must(pgextension.CompleteCoverage(schemaext.Observed))
	return common, owned
}

// declareBoth is [readBoth] for a declared schema.
func declareBoth(extensions []catalog.Extension) (common, owned *schemamodel.Database) {
	base := func() *schemamodel.Database {
		return &schemamodel.Database{
			Schemas: []schemamodel.Schema{{Name: "app"}, {Name: "audit"}, {Name: "extensions"}, {Name: "public"}},
			Tables:  []schemamodel.Table{{StructName: "User", Schema: "app", Name: "users"}},
			Fields:  []schemamodel.Field{{StructName: "User", Name: "id", Type: "INTEGER", Primary: true}},
		}
	}
	common = base()
	owned = base()
	var objects []schemaext.Object
	for _, extension := range extensions {
		common.Extensions = append(common.Extensions, schemamodel.Extension{Name: extension.Name, Schema: extension.Schema, Version: extension.Version})
		objects = append(objects, must.Must(pgextension.DesiredObject(pgextension.Ref(extension.Name),
			pgextension.DesiredExtension{Schema: extension.Schema, Version: extension.Version})))
	}
	owned.FeatureObjects = must.Must(schemaext.NewObjects(objects...))
	owned.FeatureCoverage = must.Must(pgextension.CompleteCoverage(schemaext.Desired))
	return common, owned
}

// readExtensions lists a read's extensions, from either model, as
// schema.name:version in name order.
func readExtensions(db *catalog.Database) []string {
	listed := make([]string, 0, len(db.Extensions))
	for _, extension := range db.Extensions {
		listed = append(listed, extension.Schema+"."+extension.Name+":"+extension.Version)
	}
	for _, object := range must.Must(db.FeatureObjects.All()) {
		value := object.Value.(*pgextension.ObservedExtension)
		listed = append(listed, value.Schema+"."+pgextension.Name(object.Ref)+":"+value.Version)
	}
	slices.Sort(listed)
	return listed
}

// declaredExtensions is [readExtensions] for a declared schema.
func declaredExtensions(db *schemamodel.Database) []string {
	listed := make([]string, 0, len(db.Extensions))
	for _, extension := range db.Extensions {
		listed = append(listed, extension.Schema+"."+extension.Name+":"+extension.Version)
	}
	for _, object := range must.Must(db.FeatureObjects.All()) {
		value := object.Value.(*pgextension.DesiredExtension)
		listed = append(listed, value.Schema+"."+pgextension.Name(object.Ref)+":"+value.Version)
	}
	slices.Sort(listed)
	return listed
}

// TestExclude_TreatsOwnedExtensionsAsCommonOnes holds the extension owner's
// objects to the exclusions the common list gets, on both sides of a
// comparison: by name, by a qualified pattern, by the excluded schema they are
// installed into, and a version field the server is left to choose.
func TestExclude_TreatsOwnedExtensionsAsCommonOnes(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		want     []string
	}{
		{name: "a name", patterns: []string{"pg_trgm[type=extension]"}, want: []string{"audit.isn:1.2", "extensions.citext:1.6"}},
		{name: "a qualified pattern", patterns: []string{"extensions.*[type=extension]"}, want: []string{"audit.isn:1.2", "public.pg_trgm:1.6"}},
		{name: "an excluded schema", patterns: []string{"audit"}, want: []string{"extensions.citext:1.6", "public.pg_trgm:1.6"}},
		{name: "a version", patterns: []string{"pg_trgm[type=extension].version"},
			want: []string{"audit.isn:1.2", "extensions.citext:1.6", "public.pg_trgm:"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			commonRead, ownedRead := readBoth(ownedTrio)
			commonDeclared, ownedDeclared := declareBoth(ownedTrio)

			wantRead, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(commonRead, test.patterns, "public")
			c.Assert(err, qt.IsNil)
			gotRead, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(ownedRead, test.patterns, "public")
			c.Assert(err, qt.IsNil)
			wantDeclared, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(commonDeclared, test.patterns, "public")
			c.Assert(err, qt.IsNil)
			gotDeclared, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(ownedDeclared, test.patterns, "public")
			c.Assert(err, qt.IsNil)

			c.Assert(readExtensions(gotRead), qt.DeepEquals, test.want)
			c.Assert(readExtensions(wantRead), qt.DeepEquals, test.want)
			c.Assert(declaredExtensions(gotDeclared), qt.DeepEquals, test.want)
			c.Assert(declaredExtensions(wantDeclared), qt.DeepEquals, test.want)
			c.Assert(gotRead.FeatureCoverage.SubjectRecords(), qt.HasLen, 0)
			c.Assert(readExtensions(ownedRead), qt.HasLen, 3)
		})
	}
}

// TestScope_TreatsOwnedExtensionsAsCommonOnes holds the extension owner's
// objects to the scope the common list gets: a schema universe and a
// non-extension include keep every extension, an extension-only include keeps
// the ones it names, and the schema an extension is installed into stays with
// it.
func TestScope_TreatsOwnedExtensionsAsCommonOnes(t *testing.T) {
	tests := []struct {
		name        string
		scope       atlasfilter.Scope
		want        []string
		wantSchemas []string
	}{
		{name: "a schema universe", scope: atlasfilter.Scope{Schemas: []string{"app"}},
			want:        []string{"audit.isn:1.2", "extensions.citext:1.6", "public.pg_trgm:1.6"},
			wantSchemas: []string{"app"}},
		{name: "a non-extension include", scope: atlasfilter.Scope{Include: []string{"app.users"}},
			want:        []string{"audit.isn:1.2", "extensions.citext:1.6", "public.pg_trgm:1.6"},
			wantSchemas: []string{"app", "audit", "extensions", "public"}},
		{name: "an extension-only include", scope: atlasfilter.Scope{Include: []string{"extensions.citext"}},
			want:        []string{"extensions.citext:1.6"},
			wantSchemas: []string{"extensions"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := test.scope
			scope.DefaultSchema = "public"
			commonRead, ownedRead := readBoth(ownedTrio)
			commonDeclared, ownedDeclared := declareBoth(ownedTrio)

			wantRead, err := atlasfilter.ScopeDatabase(commonRead, scope)
			c.Assert(err, qt.IsNil)
			gotRead, err := atlasfilter.ScopeDatabase(ownedRead, scope)
			c.Assert(err, qt.IsNil)
			wantDeclared, err := atlasfilter.ScopeGenerated(commonDeclared, scope)
			c.Assert(err, qt.IsNil)
			gotDeclared, err := atlasfilter.ScopeGenerated(ownedDeclared, scope)
			c.Assert(err, qt.IsNil)

			c.Assert(readExtensions(gotRead), qt.DeepEquals, test.want)
			c.Assert(readExtensions(wantRead), qt.DeepEquals, test.want)
			c.Assert(declaredExtensions(gotDeclared), qt.DeepEquals, test.want)
			c.Assert(declaredExtensions(wantDeclared), qt.DeepEquals, test.want)
			c.Assert(gotRead.Schemas, qt.DeepEquals, wantRead.Schemas)
			c.Assert(gotDeclared.Schemas, qt.DeepEquals, wantDeclared.Schemas)
			c.Assert(schemaNames(gotRead.Schemas), qt.DeepEquals, test.wantSchemas)
		})
	}
}

func schemaNames(schemas []catalog.Schema) []string {
	names := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		names = append(names, schema.Name)
	}
	return names
}
