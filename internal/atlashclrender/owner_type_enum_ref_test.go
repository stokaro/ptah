package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/definedtypetest"
)

// A column typed status in a document that declares an enum status and also a
// type status that a feature owner's object defines is not written as a
// reference into the enum block: the name means two declarations, and the
// reference would point at the wrong one. The column keeps the type name it
// was declared with. The control, the enum alone, writes the reference.
func TestRenderLeavesAnEnumNameAnOwnersTypeTakesAlone(t *testing.T) {
	tests := []struct {
		name       string
		objects    []schemaext.Object
		wantColumn []string
	}{
		{name: "an owner's domain of the enum's name",
			objects:    []schemaext.Object{definedtypetest.Object(objectidentity.KindDomain, "public", "status", "text")},
			wantColumn: []string{"type = status"}},
		{name: "control: the enum alone", wantColumn: []string{"type = enum.status"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Schemas:        []schemamodel.Schema{{Name: "public"}},
				Enums:          []schemamodel.Enum{{Name: "status", Schema: "public", Values: []string{"on", "off"}}},
				Tables:         []schemamodel.Table{{StructName: "T", Name: "t", Schema: "public"}},
				Fields:         []schemamodel.Field{{StructName: "T", Name: "s", Type: "status"}},
				FeatureObjects: must.Must(schemaext.NewObjects(test.objects...)),
			}

			rendered, err := atlashclrender.Render(db, atlashclrender.Options{HCL: hclext.None()})

			c.Assert(err, qt.IsNil)
			c.Assert(renderedLinesWithPrefix(string(rendered.Data), "type = "), qt.DeepEquals, test.wantColumn)
		})
	}
}
