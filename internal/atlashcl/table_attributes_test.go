package atlashcl_test

import (
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/atlashcl"
)

// tableAttributeDecoder keeps evaluated claims and adds a facet only for a
// declared attribute. An explicit false remains different from no declaration.
type tableAttributeDecoder struct{ requests *[]hclext.Request }

func (d tableAttributeDecoder) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	*d.requests = append(*d.requests, withoutRanges(request))
	result := hclext.Result{Declarations: make([]hclext.Declaration, len(request.Claims))}
	for i, claim := range request.Claims {
		if claim.Name == "refused" {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: fmt.Errorf("table attributes refused")}
		}
		if value, found := claim.Attributes["cache_hint"]; found {
			facets, err := schemaext.NewFacets(&gadget{Size: fmt.Sprint(value.Bool)})
			if err != nil {
				return hclext.Result{}, err
			}
			result.Declarations[i].Facets = facets
		}
	}
	return result, nil
}

func tableAttributeOwners(requests *[]hclext.Request) hclext.Set {
	return must.Must(engine.New(engine.Provider{ID: "example.org/custom", Codecs: []schemaext.Codec{gadgetCodec()},
		HCL: []hclext.Extension{{Owner: "example.org/custom", Kinds: []schemaext.Kind{gadgetKind},
			Blocks:  []hclext.Block{{Site: hclext.TableAttributes, Type: "table", Attributes: []hclext.Attribute{{Name: "cache_hint", Kind: hclext.Bool}}}},
			Decoder: tableAttributeDecoder{requests: requests}}},
	})).HCL()
}

const tableAttributeDocument = `variable "enabled" {
  type = bool
  default = true
}
schema "left" {}
schema "right" {}
table "right" "cache" {
  cache_hint = false
  column "id" { type = int }
}
table "left" "cache" {
  cache_hint = var.enabled
  column "name" { type = text }
}
table "plain" {
  column "id" { type = int }
}
table "nullable" {
  cache_hint = null
  column "id" { type = int }
}
`

func TestParse_TableAttributesAttachToTheDeclaredTable(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request
	db, err := atlashcl.ParseWithOptions(t.Context(), []byte(tableAttributeDocument), "schema.hcl", atlashcl.Options{HCL: tableAttributeOwners(&requests)})
	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].Claims, qt.HasLen, 4)
	c.Assert(requests[0].Claims[0].Table, qt.DeepEquals, &hclext.Table{Schema: "right", Name: "cache", Struct: "right.cache"})
	c.Assert(requests[0].Claims[1].Table, qt.DeepEquals, &hclext.Table{Schema: "left", Name: "cache", Struct: "left.cache"})
	c.Assert(requests[0].Claims[0].Attributes, qt.DeepEquals, map[string]hclext.Value{"cache_hint": {Kind: hclext.Bool, Bool: false}})
	c.Assert(requests[0].Claims[1].Attributes, qt.DeepEquals, map[string]hclext.Value{"cache_hint": {Kind: hclext.Bool, Bool: true}})
	c.Assert(requests[0].Claims[2].Attributes, qt.HasLen, 0)
	c.Assert(requests[0].Claims[3].Attributes, qt.HasLen, 0)
	facets := make(map[string]schemaext.Value)
	for _, table := range db.Tables {
		value, _, err := table.Facets.Get(gadgetKind)
		c.Assert(err, qt.IsNil)
		facets[table.Schema+"/"+table.Name] = value
	}
	c.Assert(facets, qt.DeepEquals, map[string]schemaext.Value{"left/cache": &gadget{Size: "true"}, "right/cache": &gadget{Size: "false"}, "/plain": nil, "/nullable": nil})
	c.Assert(db.Fields, qt.HasLen, 4)
}

func TestParse_TableAttributesFailureReturnsNoSchema(t *testing.T) {
	for _, test := range []struct{ name, source, want string }{
		{name: "invalid boolean", source: "table \"t\" { cache_hint = \"true\" }", want: `(?s).*cache_hint.*bool.*`},
		{name: "unknown attribute", source: "table \"t\" { unrelated = true }", want: `(?s).*unsupported table attribute "unrelated".*`},
		{name: "later refusal", source: "table \"first\" { cache_hint = true }\ntable \"refused\" { cache_hint = false }", want: `(?s).*schema.hcl:2.*table attributes refused.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []hclext.Request
			db, err := atlashcl.ParseWithOptions(t.Context(), []byte(test.source), "schema.hcl", atlashcl.Options{HCL: tableAttributeOwners(&requests)})
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(db, qt.IsNil)
		})
	}
}

func TestParse_TableAttributesRequireASelectedOwner(t *testing.T) {
	c := qt.New(t)
	db, err := atlashcl.ParseWithOptions(t.Context(), []byte(tableAttributeDocument), "schema.hcl", atlashcl.Options{HCL: hclext.None()})
	c.Assert(err, qt.ErrorMatches, `(?s).*unsupported table attribute "cache_hint".*`)
	c.Assert(db, qt.IsNil)
}
