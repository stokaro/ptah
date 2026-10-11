package annotation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
)

// keyShading adds a shade attribute of the primary key to the table
// directive, and a tint attribute of the table itself.
func keyShading() annotation.Extension {
	return annotation.Extension{
		Owner: "example.org/paint", Kinds: []schemaext.Kind{levelKind}, Coverage: annotation.Unlimited(levelCoverage),
		Attributes: []annotation.DirectiveAttributes{
			{Directive: "ptah:schema:table", Attributes: []annotation.Attribute{{Name: "shade", Value: "string"}},
				Decode: decodeLevel, PrimaryKey: true},
			{Directive: "ptah:schema:table", Attributes: []annotation.Attribute{{Name: "tint", Value: "string"}},
				Decode: func(attributes map[string]string) (schemaext.Facets, error) {
					return schemaext.NewFacets(&level{Value: "tint " + attributes["tint"]})
				}},
		},
	}
}

// TestSet_DecodePrimaryKeyAttributes pins that the attributes of a table's
// primary key and those of the table are decoded apart, so a facet of the
// key never lands on the table, nor one of the table on the key.
func TestSet_DecodePrimaryKeyAttributes(t *testing.T) {
	tests := []struct {
		name       string
		attributes map[string]string
		wantTable  []schemaext.Value
		wantKey    []schemaext.Value
	}{
		{name: "the key's attribute", attributes: map[string]string{"name": "t", "shade": "teal"},
			wantTable: make([]schemaext.Value, 0), wantKey: []schemaext.Value{&level{Value: "teal"}}},
		{name: "the table's attribute", attributes: map[string]string{"name": "t", "tint": "red"},
			wantTable: []schemaext.Value{&level{Value: "tint red"}}, wantKey: make([]schemaext.Value, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, err := annotation.NewSet(keyShading())
			c.Assert(err, qt.IsNil)

			table, err := set.DecodeAttributes("ptah:schema:table", test.attributes)
			c.Assert(err, qt.IsNil)
			key, err := set.DecodePrimaryKeyAttributes("ptah:schema:table", test.attributes)
			c.Assert(err, qt.IsNil)

			c.Assert(must.Must(table.Values()), qt.DeepEquals, test.wantTable)
			c.Assert(must.Must(key.Values()), qt.DeepEquals, test.wantKey)
			c.Assert(set.Attributes("ptah:schema:table"), qt.DeepEquals,
				[]annotation.Attribute{{Name: "shade", Value: "string"}, {Name: "tint", Value: "string"}})
		})
	}
}

// TestNewSet_PrimaryKeyAttributesFailurePath pins that only the table
// directive declares a primary key, and that such a group decodes facets
// alone: options would belong to the table, not to its key.
func TestNewSet_PrimaryKeyAttributesFailurePath(t *testing.T) {
	options := func(map[string]string) (map[string]string, error) { return nil, nil }
	tests := []struct {
		name  string
		group annotation.DirectiveAttributes
	}{
		{name: "a directive that declares no table", group: annotation.DirectiveAttributes{Directive: "ptah:schema:index",
			Attributes: []annotation.Attribute{{Name: "shade"}}, Decode: decodeLevel, PrimaryKey: true}},
		{name: "options", group: annotation.DirectiveAttributes{Directive: "ptah:schema:table",
			Attributes: []annotation.Attribute{{Name: "shade"}}, Decode: decodeLevel, Parameters: options, PrimaryKey: true}},
		{name: "options alone", group: annotation.DirectiveAttributes{Directive: "ptah:schema:table",
			Attributes: []annotation.Attribute{{Name: "shade"}}, Parameters: options, PrimaryKey: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := annotation.NewSet(annotation.Extension{Owner: "example.org/paint", Kinds: []schemaext.Kind{levelKind},
				Coverage: annotation.Unlimited(levelCoverage), Attributes: []annotation.DirectiveAttributes{test.group}})

			c.Assert(err, qt.ErrorMatches, `annotation extension of example.org/paint declares primary key attributes of "`+
				test.group.Directive+`", which needs the table directive and a decoder alone`)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}
