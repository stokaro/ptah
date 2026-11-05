package atlashclrender_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/atlashclrender"
)

// indexWriter writes, for every table it is handed, the indexes it was given.
type indexWriter struct{ indexes []hclext.EncodedIndex }

func (w indexWriter) EncodeHCL(context.Context, hclext.EncodeRequest) (hclext.EncodeResult, error) {
	return hclext.EncodeResult{Indexes: w.indexes}, nil
}

// treeIndexOwners registers an owner that reads the index blocks of type
// "tree" in place of the frontend, adds a fanout to an index block, and
// writes indexes.
func treeIndexOwners(indexes ...hclext.EncodedIndex) hclext.Set {
	return must.Must(engine.New(engine.Provider{
		ID:     "example.org/custom",
		Codecs: []schemaext.Codec{gadgetCodec()},
		HCL: []hclext.Extension{{Owner: "example.org/custom", Kinds: []schemaext.Kind{gadgetKind}, Encoder: indexWriter{indexes: indexes},
			Blocks:     []hclext.Block{{Site: hclext.IndexAttributes, Type: "index", Attributes: []hclext.Attribute{{Name: "fanout", Kind: hclext.Text}}}},
			Decoder:    answeringHCLDecoder{},
			ReadsIndex: func(indexType string) bool { return indexType == "tree" }}},
	})).HCL()
}

// answeringHCLDecoder declares nothing for any claim.
type answeringHCLDecoder struct{}

func (answeringHCLDecoder) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	return hclext.Result{Declarations: make([]hclext.Declaration, len(request.Claims))}, nil
}

// treeTable is a table holding a gadget, so its owner is handed it, and an
// index of its own.
func treeTable() *schemamodel.Database {
	return &schemamodel.Database{
		Tables:  []schemamodel.Table{{StructName: "T", Name: "t", Facets: gadgetFacets("tree")}},
		Fields:  []schemamodel.Field{{StructName: "T", Name: "id", Type: "INTEGER"}, {StructName: "T", Name: "body", Type: "TEXT"}},
		Indexes: []schemamodel.Index{{StructName: "T", Name: "plain", Fields: []string{"id"}}},
	}
}

// TestRender_WritesAnOwnersIndex writes an index an owner writes whole as an
// index block of its table, among the table's own by name, in the layout of
// an index of the common model with the owner's attributes after its include.
func TestRender_WritesAnOwnersIndex(t *testing.T) {
	c := qt.New(t)
	table := hclext.Table{Name: "t", Struct: "T"}
	owners := treeIndexOwners(
		hclext.EncodedIndex{Table: table, Name: "by_body", Type: "tree", Columns: []string{"body"}, Include: []string{"id"}, Unique: true, Comment: "bodies",
			Ops: "text_ops", Attributes: []hclext.EncodedAttribute{{Name: "fanout", Value: hclext.Value{Kind: hclext.Number, Text: "4"}}}},
		hclext.EncodedIndex{Table: table, Name: "zz", Type: "tree", Columns: []string{"id"}},
	)

	result, err := atlashclrender.Render(treeTable(), atlashclrender.Options{HCL: owners})

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains, `  index "by_body" {
    unique = true
    type = "tree"
    comment = "bodies"
    ops = "text_ops"
    include = [column.id]
    fanout = 4
    columns = [column.body]
  }
  index "plain" {
    columns = [column.id]
  }
  index "zz" {
    type = "tree"
    columns = [column.id]
  }
`)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
}

func TestRender_RefusesAnOwnersIndex(t *testing.T) {
	table := hclext.Table{Name: "t", Struct: "T"}
	for _, test := range []struct {
		name  string
		index hclext.EncodedIndex
		want  string
	}{
		{"a name the table's own index takes", hclext.EncodedIndex{Table: table, Name: "plain", Type: "tree", Columns: []string{"id"}},
			`an HCL encoder wrote index "plain" of table "t", which the document writes no table for or writes already`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := atlashclrender.Render(treeTable(), atlashclrender.Options{HCL: treeIndexOwners(test.index)})

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(result.Data, qt.IsNil)
		})
	}
}
