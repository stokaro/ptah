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

// installed is one extension a fixture's read holds or its schema declares:
// where it is installed, its name and its version.
type installed struct {
	schema, name, version string
}

// withReadExtensions gives db the extensions as the extension owner's
// observations, with the owner's coverage of a complete read.
func withReadExtensions(db *catalog.Database, extensions ...installed) *catalog.Database {
	objects := make([]schemaext.Object, 0, len(extensions))
	for _, extension := range extensions {
		objects = append(objects, must.Must(pgextension.ObservedObject(pgextension.Ref(extension.name),
			pgextension.ObservedExtension{Schema: extension.schema, Version: extension.version})))
	}
	db.FeatureObjects = must.Must(schemaext.NewObjects(objects...))
	db.FeatureCoverage = must.Must(pgextension.CompleteCoverage(schemaext.Observed))
	return db
}

// withDeclaredExtensions is [withReadExtensions] for a declared schema.
func withDeclaredExtensions(db *schemamodel.Database, extensions ...installed) *schemamodel.Database {
	objects := make([]schemaext.Object, 0, len(extensions))
	for _, extension := range extensions {
		objects = append(objects, must.Must(pgextension.DesiredObject(pgextension.Ref(extension.name),
			pgextension.DesiredExtension{Schema: extension.schema, Version: extension.version})))
	}
	db.FeatureObjects = must.Must(schemaext.NewObjects(objects...))
	db.FeatureCoverage = must.Must(pgextension.CompleteCoverage(schemaext.Desired))
	return db
}

// readExtensions lists a read's extensions as schema.name:version in name
// order.
func readExtensions(db *catalog.Database) []string {
	listed := make([]string, 0, db.FeatureObjects.Len())
	for _, object := range must.Must(db.FeatureObjects.All()) {
		value := object.Value.(*pgextension.ObservedExtension)
		listed = append(listed, value.Schema+"."+pgextension.Name(object.Ref)+":"+value.Version)
	}
	slices.Sort(listed)
	return listed
}

// declaredExtensions is [readExtensions] for a declared schema.
func declaredExtensions(db *schemamodel.Database) []string {
	listed := make([]string, 0, db.FeatureObjects.Len())
	for _, object := range must.Must(db.FeatureObjects.All()) {
		value := object.Value.(*pgextension.DesiredExtension)
		listed = append(listed, value.Schema+"."+pgextension.Name(object.Ref)+":"+value.Version)
	}
	slices.Sort(listed)
	return listed
}

// ownedTrio is three installed extensions: one in the default schema, one in
// a schema of its own, and one in another schema nothing else uses.
var ownedTrio = []installed{
	{schema: "public", name: "pg_trgm", version: "1.6"},
	{schema: "extensions", name: "citext", version: "1.6"},
	{schema: "audit", name: "isn", version: "1.2"},
}

// readTrio is a read holding [ownedTrio] beside a table in a schema of its
// own.
func readTrio() *catalog.Database {
	return withReadExtensions(&catalog.Database{
		Schemas: []catalog.Schema{{Name: "app"}, {Name: "audit"}, {Name: "extensions"}, {Name: "public"}},
		Tables:  []catalog.Table{{Schema: "app", Name: "users", Columns: []catalog.Column{{Name: "id", DataType: "integer"}}}},
	}, ownedTrio...)
}

// declareTrio is [readTrio] for a declared schema.
func declareTrio() *schemamodel.Database {
	return withDeclaredExtensions(&schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "app"}, {Name: "audit"}, {Name: "extensions"}, {Name: "public"}},
		Tables:  []schemamodel.Table{{StructName: "User", Schema: "app", Name: "users"}},
		Fields:  []schemamodel.Field{{StructName: "User", Name: "id", Type: "INTEGER", Primary: true}},
	}, ownedTrio...)
}

// TestExclude_FiltersOwnedExtensions holds the extension owner's objects to
// the exclusions on both sides of a comparison: by name, by a qualified
// pattern, by the excluded schema they are installed into, and a version
// field the server is left to choose.
func TestExclude_FiltersOwnedExtensions(t *testing.T) {
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
			read := readTrio()

			gotRead, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(ownerSelections(), read, test.patterns, "public")
			c.Assert(err, qt.IsNil)
			gotDeclared, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(ownerSelections(), declareTrio(), test.patterns, "public")
			c.Assert(err, qt.IsNil)

			c.Assert(readExtensions(gotRead), qt.DeepEquals, test.want)
			c.Assert(declaredExtensions(gotDeclared), qt.DeepEquals, test.want)
			c.Assert(gotRead.FeatureCoverage.SubjectRecords(), qt.HasLen, 0)
			c.Assert(readExtensions(read), qt.HasLen, 3)
		})
	}
}

// TestScope_ProjectsOwnedExtensions holds the extension owner's objects to
// the scope: a schema universe and a non-extension include keep every
// extension, an extension-only include keeps the ones it names, and the
// schema an extension is installed into stays with it.
func TestScope_ProjectsOwnedExtensions(t *testing.T) {
	tests := []struct {
		name        string
		scope       atlasfilter.Scope
		want        []string
		wantSchemas []string
	}{
		{name: "a schema universe", scope: atlasfilter.Scope{Selections: ownerSelections(), Schemas: []string{"app"}},
			want:        []string{"audit.isn:1.2", "extensions.citext:1.6", "public.pg_trgm:1.6"},
			wantSchemas: []string{"app"}},
		{name: "a non-extension include", scope: atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"app.users"}},
			want:        []string{"audit.isn:1.2", "extensions.citext:1.6", "public.pg_trgm:1.6"},
			wantSchemas: []string{"app", "audit", "extensions", "public"}},
		{name: "an extension-only include", scope: atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"extensions.citext"}},
			want:        []string{"extensions.citext:1.6"},
			wantSchemas: []string{"extensions"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := test.scope
			scope.DefaultSchema = "public"

			gotRead, err := atlasfilter.ScopeDatabase(readTrio(), scope)
			c.Assert(err, qt.IsNil)
			gotDeclared, err := atlasfilter.ScopeGenerated(declareTrio(), scope)
			c.Assert(err, qt.IsNil)

			c.Assert(readExtensions(gotRead), qt.DeepEquals, test.want)
			c.Assert(declaredExtensions(gotDeclared), qt.DeepEquals, test.want)
			c.Assert(schemaNames(gotRead.Schemas), qt.DeepEquals, test.wantSchemas)
			c.Assert(declaredSchemaNames(gotDeclared.Schemas), qt.DeepEquals, test.wantSchemas)
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

func declaredSchemaNames(schemas []schemamodel.Schema) []string {
	names := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		names = append(names, schema.Name)
	}
	return names
}
