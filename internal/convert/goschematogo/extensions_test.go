package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
)

// extensionSchema declares one table and the given extensions as the owner's
// objects, each scoped to targets when it names any.
func extensionSchema(owned map[string]pgextension.DesiredExtension, targets ...string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields: []schemamodel.Field{{StructName: "Doc", FieldName: "ID", Name: "id", Type: "INTEGER", Primary: true}},
	}
	objects := make([]schemaext.Object, 0, len(owned))
	for name, extension := range owned {
		object := must.Must(pgextension.DesiredObject(pgextension.Ref(name), extension))
		object.Targets = targets
		objects = append(objects, object)
	}
	db.FeatureObjects = must.Must(schemaext.NewObjects(objects...))
	db.FeatureCoverage = must.Must(pgextension.CompleteCoverage(schemaext.Desired))
	return db
}

// TestRender_WritesTheOwnedExtensionAnnotation pins the annotation the export
// writes for each shape of an owned extension. What a read reported the
// extension provides is not an attribute, so it is not written.
func TestRender_WritesTheOwnedExtensionAnnotation(t *testing.T) {
	tests := []struct {
		name      string
		extension pgextension.DesiredExtension
		targets   []string
		want      string
	}{
		{
			name:      "every value",
			extension: pgextension.DesiredExtension{Schema: "ext", Version: "1.6", Comment: "trigrams", IfNotExists: true, Provides: []string{"similarity"}},
			want:      `//ptah:schema:extension name="pg_trgm" schema="ext" if_not_exists="true" version="1.6" comment="trigrams"` + "\n",
		},
		{
			name: "no value",
			want: `//ptah:schema:extension name="pg_trgm"` + "\n",
		},
		{
			name:    "a target scope",
			targets: []string{"cockroachdb", "postgres"},
			want:    `//ptah:schema:extension name="pg_trgm" dialects="cockroachdb,postgres"` + "\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			source, err := renderOneFile(c, extensionSchema(map[string]pgextension.DesiredExtension{"pg_trgm": test.extension}, test.targets...))

			c.Assert(err, qt.IsNil)
			c.Assert(source, qt.Contains, test.want)
		})
	}
}

// TestRender_WritesOwnedExtensionsInNameOrder holds the export to name order,
// whatever order the owner's collection hands the objects out in.
func TestRender_WritesOwnedExtensionsInNameOrder(t *testing.T) {
	c := qt.New(t)

	source, err := renderOneFile(c, extensionSchema(map[string]pgextension.DesiredExtension{
		"uuid-ossp": {}, "citext": {}, "pg_trgm": {},
	}))

	c.Assert(err, qt.IsNil)
	c.Assert(source, qt.Matches, `(?s).*name="citext".*name="pg_trgm".*name="uuid-ossp".*`)
}
