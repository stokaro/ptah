package yamlext_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
)

// entrySectionOwner is a document owner of a column key shade and an index
// key paint that reads the index's type with it.
func entrySectionOwner() yamlext.Extension {
	owner := batchOwner("example.org/a", "levels")
	owner.EntrySections = []yamlext.EntrySection{
		{Entry: yamlext.EntryColumn, Key: "shade"},
		{Entry: yamlext.EntryIndex, Key: "paint", Reads: []string{"type"}},
	}
	return owner
}

func TestEntrySections_HappyPath(t *testing.T) {
	c := qt.New(t)
	node := &yamlext.Node{Kind: yamlext.ScalarNode, Tag: "!!str", Value: "value"}
	kind := &yamlext.Node{Kind: yamlext.ScalarNode, Tag: "!!str", Value: "btree"}
	fragments := []yamlext.Fragment{
		{Path: []string{"tables", "a", "columns", "id", "shade"}, Table: 0, Name: "id", Node: node},
		{Path: []string{"tables", "a", "fields", "body", "shade"}, Table: 0, Name: "body", Node: node},
		{Path: []string{"tables", "a", "indexes", "by_id", "paint"}, Table: 0, Name: "by_id", Node: node, Inherited: map[string]*yamlext.Node{"type": kind}},
		{Path: []string{"indexes", "by_body", "paint"}, Table: 0, Name: "by_body", Node: node},
	}
	var received []yamlext.Fragment
	owner := entrySectionOwner()
	owner.Document = func(_ context.Context, input yamlext.Document) (yamlext.DocumentResult, error) {
		received = input.Fragments
		input.Fragments[2].Inherited["type"].Value = "mutated"
		return yamlext.DocumentResult{}, nil
	}
	set, err := yamlext.NewSet(owner)
	c.Assert(err, qt.IsNil)

	_, err = set.DecodeDocuments(t.Context(), yamlext.Document{Tables: yamlext.Tables{{Key: "a", Name: "a"}}, Fragments: fragments})

	c.Assert(err, qt.IsNil)
	c.Assert(received, qt.HasLen, 4)
	c.Assert(received[2].Inherited["type"].Value, qt.Equals, "mutated")
	c.Assert(kind.Value, qt.Equals, "btree", qt.Commentf("the owner reads a copy"))
	c.Assert(set.EntryReads(yamlext.EntryIndex, "paint"), qt.DeepEquals, []string{"type"})
	found, scalar := set.EntryKey(yamlext.EntryColumn, "shade")
	c.Assert(found, qt.IsTrue)
	c.Assert(scalar, qt.IsFalse)
	c.Assert(set.DocumentKey(yamlext.EntryIndex, "paint"), qt.IsTrue)
}

// TestEntrySections_FailurePath refuses a column or an index key an owner
// would decode locally, a key of an entry the frontend has no keys of, and
// reads that are empty, repeated, the key itself or of a table key.
func TestEntrySections_FailurePath(t *testing.T) {
	noEntryDecode := func(func(any) error, yamlext.Table) ([]yamlext.Contribution, error) { return nil, nil }
	local := sectionOwner("example.org/widget", "levels", noDecode)
	local.EntrySections = []yamlext.EntrySection{{Entry: yamlext.EntryColumn, Key: "shade", Decode: noEntryDecode}}
	cases := []struct {
		name      string
		extension yamlext.Extension
	}{
		{"a column key of a local owner", local},
		{"an entry the frontend has no keys of", withSections(yamlext.EntrySection{Entry: "routines", Key: "shade"})},
		{"reads of a table key", withSections(yamlext.EntrySection{Key: "paint", Reads: []string{"type"}})},
		{"an empty read", withSections(yamlext.EntrySection{Entry: yamlext.EntryIndex, Key: "paint", Reads: []string{" "}})},
		{"a repeated read", withSections(yamlext.EntrySection{Entry: yamlext.EntryIndex, Key: "paint", Reads: []string{"type", "type"}})},
		{"the key itself", withSections(yamlext.EntrySection{Entry: yamlext.EntryIndex, Key: "paint", Reads: []string{"paint"}})},
		{"one key twice", withSections(yamlext.EntrySection{Entry: yamlext.EntryColumn, Key: "shade"}, yamlext.EntrySection{Entry: yamlext.EntryColumn, Key: "shade"})},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := yamlext.NewSet(test.extension)

			c.Assert(err, qt.IsNotNil)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}

func withSections(sections ...yamlext.EntrySection) yamlext.Extension {
	owner := batchOwner("example.org/a", "levels")
	owner.EntrySections = sections
	return owner
}

// TestEntryFragments_FailurePath refuses, before any owner call, a column or
// an index fragment without the name of its entry, bound to another table's
// key, or carrying a key of its entry its owner does not read, and a
// top-level index fragment without a table.
func TestEntryFragments_FailurePath(t *testing.T) {
	node := &yamlext.Node{Kind: yamlext.ScalarNode, Tag: "!!str", Value: "value"}
	cases := []struct {
		name     string
		fragment yamlext.Fragment
	}{
		{"a column without its name", yamlext.Fragment{Path: []string{"tables", "a", "columns", "id", "shade"}, Table: 0, Node: node}},
		{"another table's key", yamlext.Fragment{Path: []string{"tables", "b", "columns", "id", "shade"}, Table: 0, Name: "id", Node: node}},
		{"an unread key of the entry", yamlext.Fragment{Path: []string{"tables", "a", "columns", "id", "shade"}, Table: 0, Name: "id", Node: node,
			Inherited: map[string]*yamlext.Node{"type": node}}},
		{"a top-level index without its table", yamlext.Fragment{Path: []string{"indexes", "by_id", "paint"}, Table: -1, Name: "by_id", Node: node}},
		{"a key of another entry", yamlext.Fragment{Path: []string{"tables", "a", "columns", "id", "paint"}, Table: 0, Name: "id", Node: node}},
		{"a path of no entry", yamlext.Fragment{Path: []string{"tables", "a", "checks", "id", "shade"}, Table: 0, Name: "id", Node: node}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			owner := entrySectionOwner()
			owner.Document = func(context.Context, yamlext.Document) (yamlext.DocumentResult, error) {
				calls++
				return yamlext.DocumentResult{}, nil
			}
			set, err := yamlext.NewSet(owner)
			c.Assert(err, qt.IsNil)

			result, err := set.DecodeDocuments(t.Context(), yamlext.Document{Tables: yamlext.Tables{{Key: "a"}}, Fragments: []yamlext.Fragment{test.fragment}})

			c.Assert(err, qt.IsNotNil)
			c.Assert(calls, qt.Equals, 0)
			c.Assert(result.Contributions, qt.IsNil)
		})
	}
}

// TestEntryContributions_FailurePath refuses a facet that names both a
// column and an index, and an object that names either.
func TestEntryContributions_FailurePath(t *testing.T) {
	cases := []struct {
		name         string
		contribution yamlext.Contribution
	}{
		{"a facet of a column and an index", yamlext.Contribution{Facet: &level{}, Column: "id", Index: "by_id"}},
		{"an object of a column", yamlext.Contribution{Object: &schemaext.Object{Value: &level{}}, Column: "id"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := entrySectionOwner()
			owner.Kinds = []schemaext.Kind{levelKind}
			owner.Document = func(context.Context, yamlext.Document) (yamlext.DocumentResult, error) {
				return yamlext.DocumentResult{Contributions: []yamlext.Contribution{test.contribution}}, nil
			}
			set, err := yamlext.NewSet(owner)
			c.Assert(err, qt.IsNil)

			result, err := set.DecodeDocuments(t.Context(), yamlext.Document{Tables: yamlext.Tables{{Key: "a"}}})

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Contributions, qt.IsNil)
		})
	}
}
