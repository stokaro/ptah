package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/atlashcl"
)

// treeOwners registers an owner that reads the `tune` attribute of every
// index block, and reads the index blocks of type "tree" in place of the
// frontend, as the tuner decoder reads them; requests receives what it is
// handed.
func treeOwners(requests *[]hclext.Request) hclext.Set {
	return must.Must(engine.New(engine.Provider{
		ID:     "example.org/custom",
		Codecs: []schemaext.Codec{gadgetCodec()},
		HCL: []hclext.Extension{{
			Owner:      "example.org/custom",
			Kinds:      []schemaext.Kind{gadgetKind},
			Blocks:     []hclext.Block{{Site: hclext.IndexAttributes, Type: "index", Attributes: []hclext.Attribute{{Name: "tune", Kind: hclext.Text}}}},
			Decoder:    tunerDecoder{requests: requests},
			ReadsIndex: func(indexType string) bool { return indexType == "tree" },
		}},
	})).HCL()
}

const treeDocument = `table "t" {
  column "a" {
    type = int
  }
  column "b" {
    type = int
  }
  index "by_a" {
    unique  = true
    type    = "tree"
    comment = "a first"
    ops     = "int_ops"
    include = [column.b]
    tune    = "fast"
    columns = [column.a, column.b]
  }
  index "plain" {
    columns = [column.a]
  }
}
`

// TestParseWithOptions_HandsAnOwnedIndexToItsOwner builds no index of a block
// of a type its owner reads: the owner is handed what the frontend read of
// the block with its own attributes, and what it returns belongs to the
// table. A block of another type stays the frontend's.
func TestParseWithOptions_HandsAnOwnedIndexToItsOwner(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request

	db, err := atlashcl.ParseWithOptions(t.Context(), []byte(treeDocument), "schema.hcl", atlashcl.Options{HCL: treeOwners(&requests)})

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.DeepEquals, []hclext.Request{{Claims: []hclext.Claim{
		{Site: hclext.IndexAttributes, Block: "index", Name: "by_a", Attributes: map[string]hclext.Value{"tune": {Kind: hclext.Text, Text: "fast"}},
			Index: &hclext.Index{Table: "t", Type: "tree", Operator: "int_ops", Owned: true, Columns: []string{"a", "b"}, Include: []string{"b"}, Unique: true,
				Comment: "a first"}, At: "schema.hcl:8"},
		{Site: hclext.IndexAttributes, Block: "index", Name: "plain", Attributes: make(map[string]hclext.Value), Index: &hclext.Index{Table: "t"}, At: "schema.hcl:17"},
	}}})
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, "plain")
	tuned, found, err := schemaext.FacetAs[*gadget](db.Tables[0].Facets, gadgetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(tuned.Size, qt.Equals, "fast/tree")
}

// TestParseWithOptions_RefusesWhatAnOwnedIndexCannotCarry refuses a block of
// a type its owner reads that writes what the owner would never receive.
func TestParseWithOptions_RefusesWhatAnOwnedIndexCannotCarry(t *testing.T) {
	for _, test := range []struct {
		name, attribute, want string
	}{
		{"a condition", `where = "a > 0"`, `where`},
		{"nulls distinct", "nulls_distinct = false\n    unique = true", `nulls_distinct`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []hclext.Request
			source := "table \"t\" {\n  column \"a\" {\n    type = int\n  }\n  index \"by_a\" {\n    type = \"tree\"\n    " + test.attribute +
				"\n    columns = [column.a]\n  }\n}\n"

			db, err := atlashcl.ParseWithOptions(t.Context(), []byte(source), "schema.hcl", atlashcl.Options{HCL: treeOwners(&requests)})

			c.Assert(err, qt.ErrorMatches, `parse HCL schema at schema\.hcl:5,3-8: index "by_a" is a "tree" index, which its owner reads in place of the frontend, `+
				`and its owner receives no `+test.want+` of it`)
			c.Assert(db, qt.IsNil)
			c.Assert(requests, qt.HasLen, 0)
		})
	}
}

// TestParseWithOptions_RefusesAnotherOwnersAttributeOfAnOwnedIndex refuses an
// attribute another owner adds to index blocks on a block of a type a second
// owner reads: no index is built for the first owner to read it from.
func TestParseWithOptions_RefusesAnotherOwnersAttributeOfAnOwnedIndex(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request
	owners := must.Must(engine.New(
		engine.Provider{ID: "example.org/custom", Codecs: []schemaext.Codec{gadgetCodec()}, HCL: []hclext.Extension{{
			Owner: "example.org/custom", Kinds: []schemaext.Kind{gadgetKind}, Decoder: tunerDecoder{requests: &requests},
			Blocks: []hclext.Block{{Site: hclext.IndexAttributes, Type: "index", Attributes: []hclext.Attribute{{Name: "tune", Kind: hclext.Text}}}},
		}}},
		engine.Provider{ID: "example.org/tree", HCL: []hclext.Extension{{Owner: "example.org/tree", Decoder: tunerDecoder{requests: &requests},
			ReadsIndex: func(indexType string) bool { return indexType == "tree" }}}},
	)).HCL()
	source := "table \"t\" {\n  column \"a\" {\n    type = int\n  }\n  index \"by_a\" {\n    type = \"tree\"\n    tune = \"fast\"\n    columns = [column.a]\n  }\n}\n"

	db, err := atlashcl.ParseWithOptions(t.Context(), []byte(source), "schema.hcl", atlashcl.Options{HCL: owners})

	c.Assert(err, qt.ErrorMatches, `parse HCL schema at schema\.hcl:5,3-8: index "by_a" is a "tree" index, which its owner reads in place of the frontend, `+
		`and its owner receives no tune of it`)
	c.Assert(db, qt.IsNil)
	c.Assert(requests, qt.HasLen, 0)
}
