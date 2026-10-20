package atlashclrender_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

type tableChildrenDecoder struct{ claims *[]hclext.Claim }

func (d tableChildrenDecoder) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	*d.claims = append(*d.claims, request.Claims...)
	return hclext.Result{Declarations: make([]hclext.Declaration, len(request.Claims))}, nil
}

// TestRender_OwnerTableChildrenRoundTrip exercises the data contract through
// both frontends, retaining order, local column identity, and SQL text.
func TestRender_OwnerTableChildrenRoundTrip(t *testing.T) {
	for _, structName := range []string{"T", "", "Renamed", "OtherTable"} {
		t.Run("struct_"+structName, func(t *testing.T) {
			c := qt.New(t)
			var claims []hclext.Claim
			column := hclext.Value{Kind: hclext.Ref, Reference: &hclext.Reference{Kind: "column", Name: "tenant.id"}}
			blocks := []hclext.EncodedBlock{{Type: "layout", Placement: hclext.WithinTable,
				Table:      &hclext.Table{Schema: "app", Name: "t", Struct: structName},
				Attributes: []hclext.EncodedAttribute{{Name: "columns", Value: hclext.Value{Kind: hclext.Refs, References: []hclext.Reference{*column.Reference}}}},
				Children: []hclext.EncodedChild{
					{Type: "by", Unlabeled: true, Attributes: []hclext.EncodedAttribute{{Name: "column", Value: column}}},
					{Type: "by", Unlabeled: true, Attributes: []hclext.EncodedAttribute{{Name: "expr", Value: hclext.Value{Kind: hclext.SQL, Text: `lower("name")`}}}},
					{Type: "by", Unlabeled: true, Attributes: []hclext.EncodedAttribute{{Name: "column", Value: column}}},
				},
			}}
			owners := must.Must(hclext.NewSet(hclext.Extension{Owner: "example.org/custom", Kinds: []schemaext.Kind{gadgetKind},
				Encoder: blocksEncoder{blocks: blocks}, Decoder: tableChildrenDecoder{claims: &claims},
				Blocks: []hclext.Block{{Site: hclext.InTable, Type: "layout",
					Attributes: []hclext.Attribute{{Name: "columns", Kind: hclext.Refs, Refers: "column"}},
					Children: []hclext.Child{{Type: "by", Unlabeled: true, Attributes: []hclext.Attribute{
						{Name: "column", Kind: hclext.Ref, Refers: "column", Optional: true}, {Name: "expr", Kind: hclext.SQL},
					}}},
				}},
			}))
			document := refsDocument()
			document.Tables = append(document.Tables, schemamodel.Table{Schema: "other", Name: "t", StructName: "OtherTable"})
			result, err := atlashclrender.Render(document, atlashclrender.Options{HCL: owners})
			c.Assert(err, qt.IsNil)
			_, err = atlashcl.ParseWithOptions(t.Context(), result.Data, "schema.hcl", atlashcl.Options{HCL: owners})
			c.Assert(err, qt.IsNil)
			c.Assert(claims, qt.HasLen, 1)
			c.Assert(claims[0].Table, qt.DeepEquals, &hclext.Table{Schema: "app", Name: "t", Struct: "app.t"})
			c.Assert(claims[0].Attributes["columns"], qt.DeepEquals, blocks[0].Attributes[0].Value)
			c.Assert(claims[0].Children, qt.HasLen, 3)
			c.Assert(claims[0].Children[0].Attributes["column"], qt.DeepEquals, column)
			c.Assert(claims[0].Children[1].Attributes["expr"], qt.DeepEquals, blocks[0].Children[1].Attributes[0].Value)
			c.Assert(claims[0].Children[2].Attributes["column"], qt.DeepEquals, column)
			c.Assert(string(result.Data), qt.Contains, "    by {\n      column = column[\"tenant.id\"]\n    }")
		})
	}
}
