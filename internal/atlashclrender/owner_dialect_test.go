package atlashclrender_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/atlascompat"
	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/internal/atlashclrender"
)

// scopedGadgetSource reads and writes a custom model through the public HCL
// contract. Its exported selector keeps that model under another default.
type scopedGadgetSource struct{}

func (scopedGadgetSource) EncodeHCL(_ context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	blocks := make([]hclext.EncodedBlock, 0, len(request.Objects))
	for _, object := range request.Objects {
		blocks = append(blocks, hclext.EncodedBlock{Type: "gadget", Name: object.Ref.Name.Source,
			Schema: object.Ref.Schema.Source, Placement: hclext.AfterTables, Object: object.Ref,
			Attributes: []hclext.EncodedAttribute{{Name: "size", Value: hclext.Value{
				Kind: hclext.String, Text: object.Value.(*gadget).Size,
			}}},
		})
	}
	return hclext.EncodeResult{Blocks: blocks}, nil
}

func (scopedGadgetSource) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	result := hclext.Result{Declarations: make([]hclext.Declaration, 0, len(request.Claims))}
	for _, claim := range request.Claims {
		result.Declarations = append(result.Declarations, hclext.Declaration{
			Objects: []schemaext.Object{gadgetObject(claim.Schema, claim.Name, claim.Attributes["size"].Text)},
		})
	}
	return result, nil
}

func TestRender_ScopedModelSurvivesAnotherCallerDialect(t *testing.T) {
	c := qt.New(t)
	owners := must.Must(hclext.NewSet(hclext.Extension{
		Owner: "example.org/custom", Kinds: []schemaext.Kind{gadgetKind},
		Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: "gadget", Naming: hclext.SchemaAndName,
			Dialects: []string{"custom"}, Attributes: []hclext.Attribute{{Name: "size", Kind: hclext.String}}}},
		Encoder: scopedGadgetSource{}, Decoder: scopedGadgetSource{},
	}))
	original := gadgetDocument(gadgetObject("app", "g", "large"))
	rendered, err := atlashclrender.Render(original, atlashclrender.Options{HCL: owners})
	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered.Data), qt.Contains, `dialect = "custom"`)
	parsed, err := atlascompat.ParseAtlasHCLWithOptions(rendered.Data, "custom.hcl", atlascompat.ParseHCLOptions{
		HCL: owners, Dialect: "oracle",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.FeatureObjects, qt.DeepEquals, original.FeatureObjects)
	again, err := atlashclrender.Render(parsed, atlashclrender.Options{HCL: owners})
	c.Assert(err, qt.IsNil)
	c.Assert(again.Data, qt.DeepEquals, rendered.Data)
}
