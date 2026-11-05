package hclext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
)

// treeKind is the model the tree owner reads a table's tree indexes into.
const treeKind schemaext.Kind = "example.org/tree/indexes"

// indexAttributes is an owner's shape of the frontend's index block, which
// adds a fanout.
func indexAttributes() hclext.Block {
	return hclext.Block{Site: hclext.IndexAttributes, Type: "index", Attributes: []hclext.Attribute{{Name: "fanout", Kind: hclext.Text}}}
}

// treeExtension reads the index blocks of type "tree" in place of the
// frontend, with blocks as its shapes.
func treeExtension(owner string, blocks ...hclext.Block) hclext.Extension {
	extension := extension(owner, treeKind, answeringDecoder{}, blocks...)
	extension.ReadsIndex = func(indexType string) bool { return indexType == "tree" }
	return extension
}

// The frontend claims an index block of a type an owner reads with the
// owner's shape of the index block, or with one that adds nothing where the
// owner has none; a block of another type is no owner's.
func TestSet_IndexShape_HappyPath(t *testing.T) {
	c := qt.New(t)
	shaped := must.Must(hclext.NewSet(treeExtension("example.org/tree", indexAttributes())))
	bare := must.Must(hclext.NewSet(treeExtension("example.org/tree")))

	withAttributes, owned, err := shaped.IndexShape("tree")
	withNone, bareOwned, bareErr := bare.IndexShape("tree")
	_, other, otherErr := shaped.IndexShape("btree")

	c.Assert(err, qt.IsNil)
	c.Assert(owned, qt.IsTrue)
	c.Assert(withAttributes.Block, qt.DeepEquals, indexAttributes())
	c.Assert(bareErr, qt.IsNil)
	c.Assert(bareOwned, qt.IsTrue)
	c.Assert(withNone.Block, qt.DeepEquals, hclext.Block{Site: hclext.IndexAttributes, Type: "index"})
	c.Assert(otherErr, qt.IsNil)
	c.Assert(other, qt.IsFalse)
}

func TestSet_IndexShape_FailurePath(t *testing.T) {
	t.Run("a type two owners read", func(t *testing.T) {
		c := qt.New(t)
		other := treeExtension("example.org/other")
		other.Kinds = []schemaext.Kind{"example.org/other/indexes"}
		set := must.Must(hclext.NewSet(treeExtension("example.org/tree"), other))

		shape, owned, err := set.IndexShape("tree")

		c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
		c.Assert(err, qt.ErrorMatches, `.*an index of type "tree" is read by example.org/tree and example.org/other`)
		c.Assert(owned, qt.IsFalse)
		c.Assert(shape.Block, qt.DeepEquals, hclext.Block{})
	})
	t.Run("an owner without a decoder", func(t *testing.T) {
		c := qt.New(t)
		reader := hclext.Extension{Owner: "example.org/tree", Kinds: []schemaext.Kind{treeKind}, ReadsIndex: func(string) bool { return true }}

		set, err := hclext.NewSet(reader)

		c.Assert(err, qt.ErrorMatches, `HCL extension of example.org/tree reads index blocks without a decoder`)
		c.Assert(set.Selected(), qt.IsFalse)
	})
}

// treeEncoding is the tree owner with an encoder that writes indexes.
func treeEncoding(indexes ...hclext.EncodedIndex) hclext.Extension {
	extension := treeExtension("example.org/tree", indexAttributes())
	extension.Encoder = answeringEncoder{result: hclext.EncodeResult{Indexes: indexes}}
	return extension
}

// treeRequest hands the tree owner table, which holds a facet of its model.
func treeRequest(table hclext.Table) hclext.EncodeRequest {
	return hclext.EncodeRequest{Tables: []hclext.TableFacets{{Table: table, Facets: must.Must(schemaext.NewFacets(&widget{kind: treeKind}))}}}
}

func TestSet_Encode_WritesAnOwnersIndex(t *testing.T) {
	c := qt.New(t)
	table := hclext.Table{Name: "records", Struct: "Record"}
	index := hclext.EncodedIndex{Table: table, Name: "by_id", Type: "tree", Columns: []string{"id"},
		Attributes: []hclext.EncodedAttribute{{Name: "fanout", Value: hclext.Value{Kind: hclext.Number, Text: "4"}}}}
	set := must.Must(hclext.NewSet(treeEncoding(index)))

	result, err := set.Encode(c.Context(), treeRequest(table))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Indexes, qt.DeepEquals, []hclext.EncodedIndex{index})
}

func TestSet_Encode_RefusesAnOwnersIndex(t *testing.T) {
	table := hclext.Table{Name: "records", Struct: "Record"}
	tree := func(change func(*hclext.EncodedIndex)) []hclext.EncodedIndex {
		index := hclext.EncodedIndex{Table: table, Name: "by_id", Type: "tree", Columns: []string{"id"}}
		change(&index)
		return []hclext.EncodedIndex{index}
	}
	refusal := func(indexType string) string {
		return `.*wrote index "by_id" of type "` + indexType + `" at a table it was not handed, without a column, twice or of a type it does not read`
	}
	for _, test := range []struct {
		name    string
		indexes []hclext.EncodedIndex
		want    string
	}{
		{"a table it was not handed", tree(func(index *hclext.EncodedIndex) { index.Table = hclext.Table{Name: "halls"} }), refusal("tree")},
		{"no column", tree(func(index *hclext.EncodedIndex) { index.Columns = nil }), refusal("tree")},
		{"a type it does not read", tree(func(index *hclext.EncodedIndex) { index.Type = "btree" }), refusal("btree")},
		{"one index twice", append(tree(func(*hclext.EncodedIndex) {}), tree(func(*hclext.EncodedIndex) {})...), refusal("tree")},
		{"an attribute its shape does not name", tree(func(index *hclext.EncodedIndex) {
			index.Attributes = []hclext.EncodedAttribute{{Name: "depth", Value: hclext.Value{Kind: hclext.Number, Text: "2"}}}
		}), `.*wrote attribute "depth" of index "by_id" twice or as one its index shape does not name`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set := must.Must(hclext.NewSet(treeEncoding(test.indexes...)))

			result, err := set.Encode(c.Context(), treeRequest(table))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(result.Indexes, qt.IsNil)
		})
	}
}
