package providerhost

// White-box testing required: the HCL extension is built from a negotiated
// description in a private method, and only a started provider process reaches
// it through the public API. The fixture drives one engine through the
// frontend; the shapes of two engines and the facet ownership refusal are only
// observable here.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/ptaherr"
)

// hclHost is a host whose engines declare HCL claims on a negotiated
// source.hcl, each a top-level block of the engine's name.
func hclHost(engines ...string) *Host {
	host := &Host{description: &protocol.Description{ProviderId: "example.org/fixture", Features: []string{provider.HCLSourceFeature}}, targets: make(map[string]*target)}
	for _, id := range engines {
		engine := &protocol.Engine{Id: id, Aliases: []string{id + "-alias"}, Features: []string{provider.HCLSourceFeature}, HclSource: &protocol.HCLSourceDeclaration{
			HeaderKinds: []string{"node"},
			Blocks: []*protocol.HCLSourceBlock{{Site: protocol.HCLSourceBlock_TOP_LEVEL, Type: id, Naming: protocol.HCLSourceBlock_SCHEMA_AND_NAME,
				Selector: "node", Models: []string{"example.org/fixture/" + id}, GrantKind: id,
				Attributes: []*protocol.HCLSourceAttribute{{Name: "on", Kind: protocol.HCLSourceAttribute_REFERENCE, Refers: "table", Optional: true}},
				Children:   []*protocol.HCLSourceChild{{Type: "part", Unlabeled: true}}}}}}
		host.description.Engines = append(host.description.Engines, engine)
		host.targets[id] = &target{host: host, engine: engine}
	}
	return host
}

func TestHCLExtensions_HappyPath(t *testing.T) {
	t.Run("one engine", func(t *testing.T) {
		c := qt.New(t)

		extensions := hclHost("fixture").hclExtensions()

		c.Assert(extensions, qt.HasLen, 1)
		extension := extensions[0]
		c.Assert(extension.Owner, qt.Equals, "example.org/fixture")
		c.Assert(extension.CapturedCoverage, qt.IsTrue)
		c.Assert(extension.Header, qt.DeepEquals, []coverage.Kind{"node"})
		c.Assert(extension.Blocks, qt.DeepEquals, []hclext.Block{{Site: hclext.TopLevel, Type: "fixture", Naming: hclext.SchemaAndName, GrantKind: "fixture",
			Attributes: []hclext.Attribute{{Name: "on", Kind: hclext.Ref, Refers: "table", Optional: true}},
			Children:   []hclext.Child{{Type: "part", Unlabeled: true, Attributes: make([]hclext.Attribute, 0)}}}})
	})
	t.Run("two engines share one extension", func(t *testing.T) {
		c := qt.New(t)

		extensions := hclHost("first", "second").hclExtensions()

		c.Assert(extensions[0].Blocks, qt.HasLen, 2)
		c.Assert(extensions[0].Header, qt.DeepEquals, []coverage.Kind{"node"})
		_, err := hclext.NewSet(extensions...)
		c.Assert(err, qt.IsNil)
	})
	t.Run("no engine without the negotiated feature", func(t *testing.T) {
		c := qt.New(t)
		host := hclHost("fixture")
		host.description.Features = nil

		c.Assert(host.hclExtensions(), qt.IsNil)
	})
}

// TestHCLFacetOwner_FailurePath refuses a facet of something other than what
// the block's site declares a setting of.
func TestHCLFacetOwner_FailurePath(t *testing.T) {
	index := &protocol.Reference{Engine: "fixture", Kind: "index", Path: []string{"items"}, Name: "by_id"}
	for _, row := range []struct {
		name     string
		fragment *protocol.SourceFragment
		bound    *protocol.Reference
		owner    *protocol.Reference
	}{
		{"another index", &protocol.SourceFragment{Subject: index, HclBlock: &protocol.HCLSourceBlock{Site: protocol.HCLSourceBlock_INDEX_ATTRIBUTES}}, nil,
			&protocol.Reference{Engine: "fixture", Kind: "index", Path: []string{"items"}, Name: "other"}},
		{"another table than the bound one", &protocol.SourceFragment{HclBlock: &protocol.HCLSourceBlock{Site: protocol.HCLSourceBlock_IN_TABLE}},
			&protocol.Reference{Kind: "table", Name: "items"}, &protocol.Reference{Engine: "fixture", Kind: "table", Name: "orders"}},
		{"a column of the bound table", &protocol.SourceFragment{HclBlock: &protocol.HCLSourceBlock{Site: protocol.HCLSourceBlock_TABLE_ATTRIBUTES}},
			&protocol.Reference{Kind: "table", Name: "items"}, &protocol.Reference{Engine: "fixture", Kind: "column", Path: []string{"items"}, Name: "id"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			err := hclFacetOwner(row.fragment, row.bound, row.owner)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorMatches, `.*decoded HCL facet is owned by .*, which is not what its block declares a setting of`)
		})
	}
}

// TestCheckHCLSource_FailurePath refuses one block two engines claim.
func TestCheckHCLSource_FailurePath(t *testing.T) {
	c := qt.New(t)
	host := hclHost("first", "second")
	host.description.Engines[1].HclSource.Blocks[0].Type = "first"

	err := checkHCLSource(host.description)

	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*HCL block "first" at site TOP_LEVEL is claimed by engines "first" and "second"; a block has one owner`)
}
