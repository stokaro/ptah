package atlashcl_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/atlashcl"
)

const gadgetKind schemaext.Kind = "example.org/custom/gadget"

// gadget is an object of the owner example.org/custom, which a `gadget`
// block declares.
type gadget struct {
	Size string
}

func (*gadget) Kind() schemaext.Kind { return gadgetKind }

func (g *gadget) Clone() schemaext.Value { return new(*g) }

func (g *gadget) Equal(other schemaext.Value) bool {
	o, ok := other.(*gadget)
	return ok && *o == *g
}

func gadgetCodec() schemaext.Codec {
	encode := func(value schemaext.Payload) (json.RawMessage, error) { return json.Marshal(value.(*gadget).Size) }
	return schemaext.Codec{Prototype: &gadget{}, Representation: schemaext.Desired, Version: 1,
		Definition: json.RawMessage(`{"type":"string"}`),
		Clone:      func(value schemaext.Payload) (schemaext.Payload, error) { return value.(*gadget).Clone(), nil },
		Encode:     encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			size, err := schemaext.DecodeJSON[string](data)
			return &gadget{Size: size}, err
		},
	}
}

// gadgetDecoder declares a gadget for each claim, refuses the one named
// "refused", and records the requests it is given and the claims made.
type gadgetDecoder struct {
	requests *[]hclext.Request
}

func (d gadgetDecoder) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	*d.requests = append(*d.requests, request)
	declarations := make([]hclext.Declaration, 0, len(request.Claims))
	for i, claim := range request.Claims {
		if claim.Name == "refused" {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: fmt.Errorf("gadget %q is refused", claim.Name)}
		}
		ref := objectidentity.ID{Kind: objectidentity.Kind(gadgetKind), Name: objectidentity.Part{Source: claim.Name, Normalized: claim.Name}}
		declarations = append(declarations, hclext.Declaration{Objects: []schemaext.Object{
			{Ref: ref, Value: &gadget{Size: claim.Attributes["size"].Text}}}})
	}
	return hclext.Result{Declarations: declarations}, nil
}

// gadgetOwners registers the owner example.org/custom, whose `gadget` block
// takes a string `size` and a `note` read as text, and whose claim counts the
// documents that ask for it.
func gadgetOwners(requests *[]hclext.Request, claims *int) hclext.Set {
	return must.Must(engine.New(engine.Provider{
		ID:     "example.org/custom",
		Codecs: []schemaext.Codec{gadgetCodec()},
		HCL: []hclext.Extension{{
			Owner: "example.org/custom",
			Kinds: []schemaext.Kind{gadgetKind},
			Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: "gadget", Naming: hclext.SchemaAndName,
				Attributes: []hclext.Attribute{{Name: "size", Kind: hclext.String}, {Name: "note", Kind: hclext.Text}}}},
			Decoder: gadgetDecoder{requests: requests},
			Coverage: func() (schemaext.Coverage, error) {
				*claims++
				return schemaext.Coverage{}, nil
			},
		}},
	})).HCL()
}

const gadgetDocument = `variable "note" {
  type    = string
  default = "from a variable"
}

schema "app" {}

gadget "app" "small" {
  size = "s"
  note = var.note
}

gadget "large" {
  schema = schema.app
  size   = "l"
  note   = 5
}
`

// TestParseWithOptions_ReachesTheOwnersBlocks hands an owner registered
// through the runtime every block of its in one call, with the labels read as
// a schema and a name and each attribute evaluated as its kind -- a variable
// and a number become text -- and adds the objects it declares. The owner's
// claim is asked for once.
func TestParseWithOptions_ReachesTheOwnersBlocks(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request
	claims := 0

	db, err := atlashcl.ParseWithOptions([]byte(gadgetDocument), "schema.hcl", atlashcl.Options{HCL: gadgetOwners(&requests, &claims)})

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.DeepEquals, []hclext.Request{{Claims: []hclext.Claim{
		{Block: "gadget", Schema: "app", Name: "small", Attributes: map[string]hclext.Value{
			"size": {Kind: hclext.String, Text: "s"}, "note": {Kind: hclext.Text, Text: "from a variable"}}},
		{Block: "gadget", Schema: "app", Name: "large", Attributes: map[string]hclext.Value{
			"size": {Kind: hclext.String, Text: "l"}, "note": {Kind: hclext.Text, Text: "5"}}},
	}}})
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 2)
	c.Assert(claims, qt.Equals, 1)
}

// TestParseWithOptions_RefusesAnOwnersBlockAsItsOwn refuses an owner's block
// the way a block of the reader's own is refused, positioned at the block:
// its labels, a nested block, an unknown attribute, an attribute of the wrong
// kind, a second declaration of one object, and the owner's own refusal.
func TestParseWithOptions_RefusesAnOwnersBlockAsItsOwn(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{name: "three labels", document: "gadget \"a\" \"b\" \"c\" {}\n",
			wantErr: `parse HCL schema at schema.hcl:1,1-7: gadget block requires one or two name labels`},
		{name: "a nested block", document: "gadget \"g\" {\n  inner {}\n}\n",
			wantErr: `parse HCL schema at schema.hcl:2,3-8: unsupported gadget block "inner"`},
		{name: "an unknown attribute", document: "gadget \"g\" {\n  color = \"red\"\n}\n",
			wantErr: `parse HCL schema at schema.hcl:1,1-7: unsupported gadget attribute "color"`},
		{name: "a string attribute of another kind", document: "gadget \"g\" {\n  size = 5\n}\n",
			wantErr: `parse HCL schema at schema.hcl:1,1-7: gadget attribute "size" must be a string`},
		{name: "one object declared twice", document: "gadget \"g\" {}\ngadget \"g\" {}\n",
			wantErr: `parse HCL schema at schema.hcl:2,1-7: gadget g is declared twice: duplicate feature identity: .*`},
		{name: "the owner's refusal", document: "gadget \"ok\" {}\ngadget \"refused\" {}\n",
			wantErr: `parse HCL schema at schema.hcl:2,1-7: gadget "refused" is refused`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []hclext.Request
			claims := 0

			db, err := atlashcl.ParseWithOptions([]byte(test.document), "schema.hcl", atlashcl.Options{HCL: gadgetOwners(&requests, &claims)})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParseWithOptions_DropsAnOwnersUnknownNameUnderTheTolerance drops an
// unknown attribute of an owner's block, and records it, as it drops one of
// the reader's own; the owner never sees it.
func TestParseWithOptions_DropsAnOwnersUnknownNameUnderTheTolerance(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request
	var ignored []atlashcl.IgnoredName
	claims := 0

	db, err := atlashcl.ParseWithOptions([]byte("gadget \"g\" {\n  color = \"red\"\n}\n"), "schema.hcl", atlashcl.Options{
		HCL: gadgetOwners(&requests, &claims), IgnoreUnknownNames: true,
		RecordIgnored: func(name atlashcl.IgnoredName) { ignored = append(ignored, name) },
	})

	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 1)
	c.Assert(ignored, qt.DeepEquals, []atlashcl.IgnoredName{{Name: "color", Kind: "attribute", Scope: "gadget", Filename: "schema.hcl", Line: 2}})
	c.Assert(requests[0].Claims[0].Attributes, qt.HasLen, 0)
}

// TestParseWithOptions_WithoutTheOwnerRefusesItsBlock is the control: without
// the owner a gadget is a block the reader does not model.
func TestParseWithOptions_WithoutTheOwnerRefusesItsBlock(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.ParseWithOptions([]byte("gadget \"g\" {}\n"), "schema.hcl", atlashcl.Options{HCL: hclext.None()})

	c.Assert(err, qt.ErrorMatches, `parse HCL schema at schema.hcl:1,1-7: unsupported top-level block "gadget"`)
	c.Assert(db, qt.IsNil)
}

// TestParseWithOptions_RefusesAnUnselectedOwnerSet refuses the zero set, so a
// caller that forgot to select owners is told rather than read without them.
func TestParseWithOptions_RefusesAnUnselectedOwnerSet(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.ParseWithOptions([]byte("schema \"app\" {}\n"), "schema.hcl", atlashcl.Options{})

	c.Assert(err, qt.ErrorIs, hclext.ErrUnselected)
	c.Assert(db, qt.IsNil)
}
