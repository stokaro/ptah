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

func materializedAttributeOwners(requests *[]hclext.Request, attribute string) hclext.Set {
	return must.Must(engine.New(engine.Provider{ID: "example.org/custom", Codecs: []schemaext.Codec{gadgetCodec()},
		HCL: []hclext.Extension{{Owner: "example.org/custom", Kinds: []schemaext.Kind{gadgetKind},
			Blocks:  []hclext.Block{{Site: hclext.MaterializedViewAttributes, Type: "materialized", Attributes: []hclext.Attribute{{Name: attribute, Kind: hclext.Bool}}}},
			Decoder: tableAttributeDecoder{requests: requests}}},
	})).HCL()
}

func TestParse_MaterializedAttributesAttachToTheDeclaredView(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request
	db, err := atlashcl.ParseWithOptions(t.Context(), []byte(`schema "a" {}
schema "b" {}
materialized "a" "mv" {
 as = "SELECT 1"
 cache_hint = false
}
materialized "b" "mv" {
 as = "SELECT 2"
 cache_hint = true
}
materialized "plain" { as = "SELECT 3" }
`), "schema.hcl", atlashcl.Options{HCL: materializedAttributeOwners(&requests, "cache_hint")})
	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].Claims, qt.HasLen, 3)
	c.Assert(db.MaterializedViews, qt.HasLen, 3)
	first, found, err := schemaext.FacetAs[*gadget](db.MaterializedViews[0].Facets, gadgetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(first.Size, qt.Equals, "false")
	second, found, err := schemaext.FacetAs[*gadget](db.MaterializedViews[1].Facets, gadgetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(second.Size, qt.Equals, "true")
	c.Assert(db.MaterializedViews[2].Facets.IsZero(), qt.IsTrue)
}

func TestParse_MaterializedAttributesCannotReplaceNativeValues(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request
	db, err := atlashcl.ParseWithOptions(t.Context(), []byte("materialized \"mv\" { as = \"SELECT 1\" }"), "schema.hcl",
		atlashcl.Options{HCL: materializedAttributeOwners(&requests, "as")})
	c.Assert(err, qt.ErrorMatches, `(?s).*owner attribute "as" conflicts with a materialized attribute.*`)
	c.Assert(db, qt.IsNil)
	c.Assert(requests, qt.HasLen, 0)
}
