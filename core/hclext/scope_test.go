package hclext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
)

func scopedShape(dialects []string, unscoped bool) hclext.Block {
	shape := block("domain")
	shape.Dialects, shape.Unscoped = dialects, unscoped
	shape.DeclaresType = true
	return shape
}

func TestSet_DialectSelectsTheBlockOwner(t *testing.T) {
	for _, test := range []struct{ dialect, owner, normalized string }{
		{"", "example.org/pg", ""},
		{"pgx", "example.org/pg", "postgres"},
		{"oracle", "example.org/oracle", "oracle"},
		{"Custom-One", "example.org/custom", "custom-one"},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			recorded := make(map[string]*[]hclext.Request)
			var extensions []hclext.Extension
			for _, row := range []struct {
				owner, dialect string
				unscoped       bool
			}{
				{"example.org/pg", "postgres", true}, {"example.org/oracle", "oracle", false}, {"example.org/custom", "custom-one", false},
			} {
				requests := new([]hclext.Request)
				recorded[row.owner] = requests
				extensions = append(extensions, extension(row.owner, schemaext.Kind(row.owner+"/domain"),
					recordingDecoder{kind: schemaext.Kind(row.owner + "/domain"), requests: requests}, scopedShape([]string{row.dialect}, row.unscoped)))
			}
			set := must.Must(hclext.NewSet(extensions...))
			selected := set.ForDialect(test.dialect)
			shape, found := selected.Lookup(hclext.TopLevel, "domain")
			c.Assert(found, qt.IsTrue)
			declared, err := selected.Decode(t.Context(), []hclext.Match{{Shape: shape, Claim: hclext.Claim{Name: "d"}}})
			c.Assert(err, qt.IsNil)
			c.Assert(declared, qt.HasLen, 1)
			c.Assert(string(declared[0].Objects[0].Value.Kind()), qt.Equals, test.owner+"/domain")
			c.Assert(*recorded[test.owner], qt.HasLen, 1)
			c.Assert((*recorded[test.owner])[0].Claims[0].Dialect, qt.Equals, test.normalized)
			original, found := set.Lookup(hclext.TopLevel, "domain")
			c.Assert(found, qt.IsTrue)
			c.Assert(original.Block.Dialects, qt.DeepEquals, []string{"postgres"})
			c.Assert(set.ForDialect("custom-two").DeclaresType("domain"), qt.IsTrue)
			_, found = set.ForDialect("custom-two").Lookup(hclext.TopLevel, "domain")
			c.Assert(found, qt.IsFalse)
		})
	}
}

func TestNewSet_RefusesOverlappingBlockDialects(t *testing.T) {
	for _, test := range []struct {
		name          string
		first, second hclext.Block
	}{
		{"aliases", scopedShape([]string{"pgx"}, false), scopedShape([]string{"postgres"}, false)},
		{"unscoped", scopedShape([]string{"postgres"}, true), scopedShape([]string{"oracle"}, true)},
		{"universal first", block("domain"), scopedShape([]string{"oracle"}, false)},
		{"universal last", scopedShape([]string{"oracle"}, false), block("domain")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := hclext.NewSet(extension("example.org/a", "example.org/a/x", answeringDecoder{}, test.first),
				extension("example.org/b", "example.org/b/x", answeringDecoder{}, test.second))
			c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
			c.Assert(got.Selected(), qt.IsFalse)
		})
	}
}

func TestNewSet_RefusesInvalidBlockDialects(t *testing.T) {
	for _, test := range []struct {
		name    string
		shape   hclext.Block
		wantErr string
	}{
		{"empty dialect", scopedShape([]string{" "}, false), `HCL block "domain" of example.org/a declares an empty dialect`},
		{"no concrete dialect", scopedShape(nil, true), `HCL block "domain" of example.org/a selects unscoped declarations without a dialect`},
		{"nested", hclext.Block{Site: hclext.InTable, Type: "domain", Dialects: []string{"oracle"}}, `HCL in-table block "domain" of example.org/a scopes its dialect, which only a top-level block does`},
		{"reserved attribute", hclext.Block{Site: hclext.TopLevel, Type: "domain", Naming: hclext.SchemaAndName, Dialects: []string{"oracle"}, Attributes: []hclext.Attribute{{Name: "dialect", Kind: hclext.String}}}, `.*declares attribute "dialect" twice or as its naming's own`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := hclext.NewSet(extension("example.org/a", "example.org/a/x", answeringDecoder{}, test.shape))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got.Selected(), qt.IsFalse)
		})
	}
}

func TestSet_EncodeBindsTheReaderDialect(t *testing.T) {
	c := qt.New(t)
	original := hclext.EncodedBlock{Type: "domain", Name: "d", Placement: hclext.UserTypes}
	owner := extension("example.org/oracle", "example.org/oracle/domain", answeringDecoder{}, scopedShape([]string{"oracle"}, false))
	owner.Encoder = answeringEncoder{result: hclext.EncodeResult{Blocks: []hclext.EncodedBlock{original}}}
	set := must.Must(hclext.NewSet(owner))
	owner.Blocks[0].Dialects[0] = "postgres"
	result, err := set.Encode(t.Context(), hclext.EncodeRequest{})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Blocks, qt.HasLen, 1)
	c.Assert(result.Blocks[0].Dialect, qt.Equals, "oracle")
	c.Assert(owner.Encoder.(answeringEncoder).result.Blocks[0], qt.DeepEquals, original)
}

func TestSet_EncodeRefusesAnotherReader(t *testing.T) {
	for _, test := range []struct {
		name  string
		block hclext.EncodedBlock
	}{
		{"another dialect", hclext.EncodedBlock{Type: "domain", Name: "d", Placement: hclext.UserTypes, Dialect: "postgres"}},
		{"an unclaimed block", hclext.EncodedBlock{Type: "gadget", Name: "d", Placement: hclext.AfterTables, Dialect: "oracle"}},
		{"an attribute overrides selection", hclext.EncodedBlock{Type: "domain", Name: "d", Placement: hclext.UserTypes,
			Attributes: []hclext.EncodedAttribute{{Name: "dialect", Value: hclext.Value{Kind: hclext.String, Text: "postgres"}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := extension("example.org/oracle", "example.org/oracle/domain", answeringDecoder{}, scopedShape([]string{"oracle"}, false))
			owner.Encoder = answeringEncoder{result: hclext.EncodeResult{Blocks: []hclext.EncodedBlock{test.block}}}
			set := must.Must(hclext.NewSet(owner))
			result, err := set.Encode(t.Context(), hclext.EncodeRequest{})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Blocks, qt.IsNil)
		})
	}
}
