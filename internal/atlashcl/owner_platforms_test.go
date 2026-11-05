package atlashcl_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/atlashcl"
)

// platformDecoder records its requests and answers each claim with a gadget
// facet whose size is the claim's ttl.
type platformDecoder struct {
	requests *[]hclext.Request
}

func (d platformDecoder) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	*d.requests = append(*d.requests, withoutRanges(request))
	declarations := make([]hclext.Declaration, len(request.Claims))
	for i, claim := range request.Claims {
		declarations[i].Facets = must.Must(schemaext.NewFacets(&gadget{Size: claim.Attributes["ttl"].Text + "/" + claim.Attributes["unit"].Text}))
	}
	return hclext.Result{Declarations: declarations}, nil
}

// parsePlatforms parses source with an owner of the custom platform group,
// read under the alias customs too, and returns the requests the owner
// received.
func parsePlatforms(ctx context.Context, source string) (*schemamodel.Database, []hclext.Request, error) {
	var requests []hclext.Request
	owners := must.Must(engine.New(engine.Provider{
		ID:     "example.org/custom",
		Codecs: []schemaext.Codec{gadgetCodec()},
		HCL: []hclext.Extension{{
			Owner: "example.org/custom",
			Kinds: []schemaext.Kind{gadgetKind},
			Blocks: []hclext.Block{{Site: hclext.TablePlatform, Type: "custom", Aliases: []string{"customs"},
				Attributes: []hclext.Attribute{{Name: "ttl", Kind: hclext.String}, {Name: "unit", Kind: hclext.String}}}},
			Decoder: platformDecoder{requests: &requests},
		}},
	})).HCL()
	db, err := atlashcl.ParseWithOptions(ctx, []byte(source), "schema.hcl", atlashcl.Options{HCL: owners})
	return db, requests, err
}

// platformTables writes the custom group of users in two blocks, one under
// the alias, beside a group of another target, and none for orders.
const platformTables = `schema "app" {}
table "users" {
  schema = schema.app
  column "id" {
    type = int
  }
  platform "custom" {
    override "ttl" {
      value = "P1D"
    }
  }
  platform "mysql" {
    override "engine" {
      value = "InnoDB"
    }
  }
  platform "customs" {
    override "unit" {
      value = "SECONDS"
    }
  }
}
table "orders" {
  column "id" {
    type = int
  }
}
`

// TestParseWithOptions_ClaimsATablePlatformGroup hands the owner the group
// of its type and alias as one claim, at the group's first block, with the
// table it stands in and every override as a string; adds the facets to that
// table alone; and keeps every other group among the table's overrides. A
// table without the group is not claimed.
func TestParseWithOptions_ClaimsATablePlatformGroup(t *testing.T) {
	c := qt.New(t)

	db, requests, err := parsePlatforms(t.Context(), platformTables)

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.DeepEquals, []hclext.Request{{Claims: []hclext.Claim{{Site: hclext.TablePlatform, Block: "custom",
		Attributes: map[string]hclext.Value{"ttl": {Kind: hclext.String, Text: "P1D"}, "unit": {Kind: hclext.String, Text: "SECONDS"}}, At: "schema.hcl:7",
		Table: &hclext.Table{Schema: "app", Name: "users", Struct: "app.users"}}}}})
	c.Assert(db.Tables, qt.HasLen, 2)
	claimed, found, err := schemaext.FacetAs[*gadget](db.Tables[0].Facets, gadgetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(claimed.Size, qt.Equals, "P1D/SECONDS")
	c.Assert(db.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{"mysql": {"engine": "InnoDB"}})
	c.Assert(db.Tables[1].Facets.Len(), qt.Equals, 0)
}

func TestParseWithOptions_ClaimsATablePlatformGroup_FailurePath(t *testing.T) {
	group := func(body string) string {
		return "table \"users\" {\n  column \"id\" {\n    type = int\n  }\n" + body + "}\n"
	}
	for _, test := range []struct {
		name   string
		source string
		want   string
	}{
		{"an override the group does not take", group("  platform \"custom\" {\n    override \"ttl_column\" {\n      value = \"ts\"\n    }\n  }\n"),
			`parse HCL schema at schema.hcl:6,5-13: unknown table platform "custom" override "ttl_column": the group takes ttl, unit`},
		{"an override in another case", group("  platform \"custom\" {\n    override \"TTL\" {\n      value = \"P1D\"\n    }\n  }\n"),
			`parse HCL schema at schema.hcl:6,5-13: unknown table platform "custom" override "TTL": override names are lower case, as "ttl"`},
		{"a key the alias writes again", group("  platform \"custom\" {\n    override \"ttl\" {\n      value = \"P1D\"\n    }\n  }\n" +
			"  platform \"customs\" {\n    override \"ttl\" {\n      value = \"P2D\"\n    }\n  }\n"),
			`parse HCL schema at schema.hcl:11,5-13: table platform override "ttl" for dialect "customs" is duplicated`},
		{"an attribute of the group", group("  platform \"custom\" {\n    ttl = \"P1D\"\n  }\n"),
			`parse HCL schema at schema.hcl:5,3-11: unsupported table platform attribute "ttl"`},
		{"a block of the group that is no override", group("  platform \"custom\" {\n    tier \"a\" {}\n  }\n"),
			`parse HCL schema at schema.hcl:6,5-9: unsupported table platform block "tier"`},
		{"an override without its value", group("  platform \"custom\" {\n    override \"ttl\" {}\n  }\n"),
			`parse HCL schema at schema.hcl:6,5-13: table platform override "ttl" requires value`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, _, err := parsePlatforms(t.Context(), test.source)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(db, qt.IsNil)
		})
	}
}
