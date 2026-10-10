package yamlext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
)

// entryOwner adds a scalar shade key to tables and indexes, reading an
// index's type beside it, and a palette section to tables. seen receives the
// keys each decoder is handed.
func entryOwner(seen *[]map[string]string) yamlext.Extension {
	record := func(attributes map[string]string) (schemaext.Facets, error) {
		*seen = append(*seen, attributes)
		return schemaext.NewFacets(&level{Value: attributes["shade"]})
	}
	return yamlext.Extension{Owner: "example.org/paint", Kinds: []schemaext.Kind{levelKind}, Coverage: emptyClaim,
		EntryAttributes: []yamlext.EntryAttributes{
			{Entry: yamlext.EntryTable, Attributes: []string{"shade"}, Decode: record},
			{Entry: yamlext.EntryIndex, Attributes: []string{"shade"}, Reads: []string{"type"}, Decode: record,
				Parameters: func(attributes map[string]string) (map[string]string, error) {
					return map[string]string{"shade": attributes["shade"]}, nil
				}},
		},
		EntrySections: []yamlext.EntrySection{{Key: "palette", Decode: decodePalette}},
	}
}

func decodePalette(decode func(any) error, _ yamlext.Table) ([]yamlext.Contribution, error) {
	var names []string
	if err := decode(&names); err != nil {
		return nil, err
	}
	return []yamlext.Contribution{{Facet: &level{Value: names[0]}, Label: "a palette"}}, nil
}

func TestSet_EntryKey(t *testing.T) {
	var seen []map[string]string
	set := must.Must(yamlext.NewSet(entryOwner(&seen)))
	tests := []struct {
		name, entry, key        string
		wantFound, wantIsScalar bool
	}{
		{name: "a scalar of a table", entry: yamlext.EntryTable, key: "shade", wantFound: true, wantIsScalar: true},
		{name: "a section of a table", entry: yamlext.EntryTable, key: "palette", wantFound: true},
		{name: "a scalar of an index", entry: yamlext.EntryIndex, key: "shade", wantFound: true, wantIsScalar: true},
		{name: "a key no owner reads", entry: yamlext.EntryIndex, key: "palette"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			found, scalar := set.EntryKey(test.entry, test.key)

			c.Assert(found, qt.Equals, test.wantFound)
			c.Assert(scalar, qt.Equals, test.wantIsScalar)
		})
	}
	c := qt.New(t)
	c.Assert(set.EntryKeys(yamlext.EntryTable), qt.DeepEquals, []string{"palette", "shade"})
}

// TestSet_DecodeEntryAttributes_ReadsTheEntrysOwnKey pins that an index's own
// key reaches the decoder beside the owner's, and calls it on its own.
func TestSet_DecodeEntryAttributes_ReadsTheEntrysOwnKey(t *testing.T) {
	c := qt.New(t)
	var seen []map[string]string
	set := must.Must(yamlext.NewSet(entryOwner(&seen)))

	facets, err := set.DecodeEntryAttributes(yamlext.EntryIndex, map[string]string{"type": "gin", "name": "i"})
	options, optionsErr := set.DecodeEntryParameters(yamlext.EntryIndex, map[string]string{"shade": "teal"})
	unwritten, unwrittenErr := set.DecodeEntryAttributes(yamlext.EntryTable, map[string]string{"name": "t"})

	c.Assert(err, qt.IsNil)
	c.Assert(facets.IsZero(), qt.IsFalse)
	c.Assert(optionsErr, qt.IsNil)
	c.Assert(options, qt.DeepEquals, map[string]string{"shade": "teal"})
	c.Assert(unwrittenErr, qt.IsNil)
	c.Assert(unwritten.IsZero(), qt.IsTrue)
	c.Assert(seen, qt.DeepEquals, []map[string]string{{"type": "gin"}})
}

func TestSet_DecodeEntrySection_HappyPath(t *testing.T) {
	c := qt.New(t)
	var seen []map[string]string
	set := must.Must(yamlext.NewSet(entryOwner(&seen)))

	contributions, err := set.DecodeEntrySection("palette", func(target any) error {
		*target.(*[]string) = []string{"teal"}
		return nil
	}, yamlext.Table{Key: "rooms", Name: "rooms"})

	c.Assert(err, qt.IsNil)
	c.Assert(contributions, qt.DeepEquals, []yamlext.Contribution{{Facet: &level{Value: "teal"}, Label: "a palette"}})
}

func TestSet_DecodeEntrySection_FailurePath(t *testing.T) {
	c := qt.New(t)
	var seen []map[string]string
	set := must.Must(yamlext.NewSet(entryOwner(&seen)))

	contributions, err := set.DecodeEntrySection("shade", func(any) error { return nil }, yamlext.Table{})

	c.Assert(err, qt.ErrorMatches, `no selected owner reads key "shade" of a table`)
	c.Assert(contributions, qt.IsNil)
}

func TestNewSet_EntryKeys_FailurePath(t *testing.T) {
	var seen []map[string]string
	noDecoder := entryOwner(&seen)
	noDecoder.EntryAttributes[0].Decode = nil
	noEntry := entryOwner(&seen)
	noEntry.EntryAttributes[0].Entry = "views"
	other := entryOwner(&seen)
	other.Owner = "example.org/other"
	other.Kinds = nil
	tests := []struct {
		name       string
		extensions []yamlext.Extension
		wantErr    string
	}{
		{name: "attributes without a decoder", extensions: []yamlext.Extension{noDecoder},
			wantErr: `YAML extension of example.org/paint declares entry attributes without an entry or a decoder`},
		{name: "attributes of no entry", extensions: []yamlext.Extension{noEntry},
			wantErr: `YAML extension of example.org/paint declares entry attributes without an entry or a decoder`},
		{name: "a key two owners read", extensions: []yamlext.Extension{entryOwner(&seen), other},
			wantErr: `duplicate.*key "shade" of tables is read by example.org/paint and example.org/other`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := yamlext.NewSet(test.extensions...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}
