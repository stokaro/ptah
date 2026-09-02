package atlashclrender

// White-box testing required: the current model has no unknown metadata field
// to exercise through Render, but the future-loss guard must reject a census
// entry until the HCL parser and renderer explicitly support it.

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
)

func TestHCLRepresentsExportMetadataRequiresAnExplicitSpelling(t *testing.T) {
	tests := []struct {
		name     string
		metadata schemamodel.ExportMetadata
		want     bool
	}{
		{name: "current table spelling", metadata: schemamodel.ExportMetadata{Kind: "table", Attribute: "api_name"}, want: true},
		{name: "current column spelling", metadata: schemamodel.ExportMetadata{Kind: "column", Attribute: "api_expose"}, want: true},
		{name: "future table spelling", metadata: schemamodel.ExportMetadata{Kind: "table", Attribute: "rest_name"}, want: false},
		{name: "future declaration kind", metadata: schemamodel.ExportMetadata{Kind: "view", Attribute: "api_name"}, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(hclRepresentsExportMetadata(test.metadata), qt.Equals, test.want)
		})
	}
}

func TestHCLRepresentsEveryTargetNameSpelling(t *testing.T) {
	targetNames := reflect.TypeFor[schemamodel.TargetNames]()
	for field := range targetNames.Fields() {
		attribute := schemamodel.TargetNameAttribute(field.Name)
		t.Run(attribute, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(hclRepresentsExportMetadata(schemamodel.ExportMetadata{
				Kind:      "table",
				Attribute: attribute,
			}), qt.IsTrue)
			c.Assert(hclRepresentsExportMetadata(schemamodel.ExportMetadata{
				Kind:      "column",
				Attribute: attribute,
			}), qt.IsTrue)
		})
	}
}
