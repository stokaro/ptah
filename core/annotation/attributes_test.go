package annotation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
)

// glossing adds a gloss attribute to ptah:schema:index that reads the
// index's own type too, and spells WITH options. seen receives the
// attributes each call is handed.
func glossing(owner, attribute string, seen *[]map[string]string,
	options func(map[string]string) (map[string]string, error),
) annotation.Extension {
	return annotation.Extension{
		Owner: owner, Coverage: annotation.Unlimited(emptyClaim),
		Attributes: []annotation.DirectiveAttributes{{
			Directive:  "ptah:schema:index",
			Attributes: []annotation.Attribute{{Name: attribute, Value: "string"}},
			Reads:      []string{"type"},
			Decode: func(attributes map[string]string) (schemaext.Facets, error) {
				*seen = append(*seen, attributes)
				return schemaext.Facets{}, nil
			},
			Parameters: options,
		}},
	}
}

func optionsOf(attribute string) func(map[string]string) (map[string]string, error) {
	return func(attributes map[string]string) (map[string]string, error) {
		return map[string]string{"gloss": attributes[attribute]}, nil
	}
}

// TestSet_DecodeAttributes_ReadsTheDirectivesOwnAttribute pins that a
// directive's own attribute an owner reads reaches its decoder beside the
// owner's attributes, and calls it on its own: the index's type alone may be
// what makes the index the owner's.
func TestSet_DecodeAttributes_ReadsTheDirectivesOwnAttribute(t *testing.T) {
	tests := []struct {
		name       string
		attributes map[string]string
		wantSeen   []map[string]string
	}{
		{name: "the owner's attribute and the type", attributes: map[string]string{"name": "i", "gloss": "matte", "type": "gin"},
			wantSeen: []map[string]string{{"gloss": "matte", "type": "gin"}}},
		{name: "the type alone", attributes: map[string]string{"name": "i", "type": "gin"},
			wantSeen: []map[string]string{{"type": "gin"}}},
		{name: "neither", attributes: map[string]string{"name": "i"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var seen []map[string]string
			set, err := annotation.NewSet(glossing("example.org/gloss", "gloss", &seen, nil))
			c.Assert(err, qt.IsNil)

			facets, err := set.DecodeAttributes("ptah:schema:index", test.attributes)

			c.Assert(err, qt.IsNil)
			c.Assert(facets.IsZero(), qt.IsTrue)
			c.Assert(seen, qt.DeepEquals, test.wantSeen)
		})
	}
}

// TestSet_DecodeParameters_HappyPath pins the options owners spell, joined,
// and none where no owner spells one.
func TestSet_DecodeParameters_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		attributes map[string]string
		want       map[string]string
	}{
		{name: "an owner's option", attributes: map[string]string{"gloss": "matte"}, want: map[string]string{"gloss": "matte"}},
		{name: "no attribute of the owner's", attributes: map[string]string{"name": "i"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var seen []map[string]string
			set, err := annotation.NewSet(glossing("example.org/gloss", "gloss", &seen, optionsOf("gloss")))
			c.Assert(err, qt.IsNil)

			options, err := set.DecodeParameters("ptah:schema:index", test.attributes)

			c.Assert(err, qt.IsNil)
			c.Assert(options, qt.DeepEquals, test.want)
		})
	}
}

// TestSet_DecodeParameters_FailurePath refuses an option two owners spell,
// since neither may decide what the other's attribute meant.
func TestSet_DecodeParameters_FailurePath(t *testing.T) {
	c := qt.New(t)
	var seen []map[string]string
	set, err := annotation.NewSet(
		glossing("example.org/gloss", "gloss", &seen, optionsOf("gloss")),
		glossing("example.org/sheen", "sheen", &seen, optionsOf("sheen")),
	)
	c.Assert(err, qt.IsNil)

	options, err := set.DecodeParameters("ptah:schema:index", map[string]string{"gloss": "matte", "sheen": "satin"})

	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(err, qt.ErrorMatches, `.*option "gloss" of "ptah:schema:index" is returned by two owners`)
	c.Assert(options, qt.IsNil)
}

// TestNewSet_AttributesThatOnlySpellOptions accepts a group with options and
// no facets.
func TestNewSet_AttributesThatOnlySpellOptions(t *testing.T) {
	c := qt.New(t)
	var seen []map[string]string
	extension := glossing("example.org/gloss", "gloss", &seen, optionsOf("gloss"))
	extension.Attributes[0].Decode = nil

	set, err := annotation.NewSet(extension)
	c.Assert(err, qt.IsNil)
	facets, err := set.DecodeAttributes("ptah:schema:index", map[string]string{"gloss": "matte"})

	c.Assert(err, qt.IsNil)
	c.Assert(facets.IsZero(), qt.IsTrue)
	c.Assert(seen, qt.HasLen, 0)
}
