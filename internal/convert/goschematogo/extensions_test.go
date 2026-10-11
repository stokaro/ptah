package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
)

// extensionSchema declares one table and the given extensions in the common
// model and as the owner's objects, each owner object scoped to targets when
// it names any.
func extensionSchema(common []schemamodel.Extension, owned map[string]pgextension.DesiredExtension, targets ...string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables:     []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields:     []schemamodel.Field{{StructName: "Doc", FieldName: "ID", Name: "id", Type: "INTEGER", Primary: true}},
		Extensions: common,
	}
	var objects []schemaext.Object
	for name, extension := range owned {
		object := must.Must(pgextension.DesiredObject(pgextension.Ref(name), extension))
		object.Targets = targets
		objects = append(objects, object)
	}
	if len(objects) > 0 {
		db.FeatureObjects = must.Must(schemaext.NewObjects(objects...))
		db.FeatureCoverage = must.Must(pgextension.CompleteCoverage(schemaext.Desired))
	}
	return db
}

// TestRender_WritesAnOwnedExtensionAsTheCommonOne holds the owner's extension
// to the annotation the common model's extension gets, in name order among
// the common ones, with its target scope as the dialects attribute.
func TestRender_WritesAnOwnedExtensionAsTheCommonOne(t *testing.T) {
	full := pgextension.DesiredExtension{Schema: "ext", Version: "1.6", Comment: "trigrams", IfNotExists: true, Provides: []string{"similarity"}}
	tests := []struct {
		name    string
		common  []schemamodel.Extension
		owned   map[string]pgextension.DesiredExtension
		targets []string
	}{
		{name: "every value", owned: map[string]pgextension.DesiredExtension{"pg_trgm": full},
			common: []schemamodel.Extension{{Name: "pg_trgm", Schema: "ext", Version: "1.6", Comment: "trigrams", IfNotExists: true}}},
		{name: "no value", owned: map[string]pgextension.DesiredExtension{"pg_trgm": {}}, common: []schemamodel.Extension{{Name: "pg_trgm"}}},
		{name: "a target scope", owned: map[string]pgextension.DesiredExtension{"pg_trgm": {}}, targets: []string{"cockroachdb", "postgres"},
			common: []schemamodel.Extension{{Name: "pg_trgm", Dialects: []string{"cockroachdb", "postgres"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			// Each side also declares a common extension on either side of the
			// tested one, so the order between the two models is measured too.
			neighbors := []schemamodel.Extension{{Name: "citext"}, {Name: "uuid-ossp"}}

			want, err := renderOneFile(c, extensionSchema(append(test.common, neighbors...), nil))
			c.Assert(err, qt.IsNil)
			got, err := renderOneFile(c, extensionSchema(neighbors, test.owned, test.targets...))

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, want)
		})
	}
}

// TestRender_WritesTheOwnedExtensionAnnotation pins the annotation itself, so
// the comparison above cannot pass by writing nothing for either model.
func TestRender_WritesTheOwnedExtensionAnnotation(t *testing.T) {
	c := qt.New(t)

	source, err := renderOneFile(c, extensionSchema(nil, map[string]pgextension.DesiredExtension{
		"pg_trgm": {Schema: "ext", Version: "1.6", Comment: "trigrams", IfNotExists: true}}, "postgres"))

	c.Assert(err, qt.IsNil)
	c.Assert(source, qt.Contains,
		`//ptah:schema:extension name="pg_trgm" schema="ext" if_not_exists="true" version="1.6" comment="trigrams" dialects="postgres"`)
}

func TestRender_RefusesAnExtensionBothModelsDeclare(t *testing.T) {
	c := qt.New(t)

	source, err := renderOneFile(c, extensionSchema([]schemamodel.Extension{{Name: "pg_trgm"}}, map[string]pgextension.DesiredExtension{"pg_trgm": {}}))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*extension "pg_trgm" is declared twice`)
	c.Assert(source, qt.Equals, "")
}
