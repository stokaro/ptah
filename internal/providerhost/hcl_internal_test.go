package providerhost

// White-box testing required: the HCL extension is built from a negotiated
// description in a private method, and only a started provider process reaches
// it through the public API. The fixture drives one engine through the
// frontend; the shapes of two engines, the facet ownership refusal, and that a
// document without a header entry the engine reads is never sent to the
// provider are only observable here.

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

const platformHeaderKind schemaext.Kind = "example.org/fixture/table-changefeeds"

type platformHeaderValue struct{}

func (*platformHeaderValue) Kind() schemaext.Kind { return platformHeaderKind }

// TestHCLCoverageHeaderKeepsAnUnreadablePlatformSubject ports the YDB half of
// TestSchemaInspectHCLKeepsAnOwnersLimitInTheHeader. A platform block speaks
// for a table only when the read knows its settings. An unreadable table keeps
// an explicit subject limit in the export header, while a complete table and
// the model's kind-wide claim stay out of it.
func TestHCLCoverageHeaderKeepsAnUnreadablePlatformSubject(t *testing.T) {
	c := qt.New(t)
	const providerID = "example.org/fixture"
	codec := schemaext.Codec{Prototype: &platformHeaderValue{}, Representation: schemaext.Desired, Version: 1,
		Definition: json.RawMessage(`{"type":"object"}`),
		Clone:      func(schemaext.Payload) (schemaext.Payload, error) { return &platformHeaderValue{}, nil },
		Encode:     func(schemaext.Payload) (json.RawMessage, error) { return json.RawMessage(`{}`), nil },
		Decode:     func(json.RawMessage) (schemaext.Payload, error) { return &platformHeaderValue{}, nil },
		Canonical:  func(schemaext.Payload) (json.RawMessage, error) { return json.RawMessage(`{}`), nil }}
	registry, err := schemaext.NewRegistry(schemaext.OwnedCodec{Owner: providerID, Codec: codec})
	c.Assert(err, qt.IsNil)
	identity, found := registry.Identity(platformHeaderKind, schemaext.Desired)
	c.Assert(found, qt.IsTrue)
	engine := &protocol.Engine{Id: "ydb", Features: []string{provider.EncodeSourceFeature}, HclSource: &protocol.HCLSourceDeclaration{Blocks: []*protocol.HCLSourceBlock{{
		Site: protocol.HCLSourceBlock_TABLE_PLATFORM, Models: []string{string(platformHeaderKind)},
	}}}}
	key := modelKey{kind: platformHeaderKind, representation: schemaext.Desired}
	host := &Host{description: &protocol.Description{ProviderId: providerID, Features: []string{provider.EncodeSourceFeature}}, models: map[modelKey]*modelContract{
		key: {key: key, engines: map[string]*protocol.ValueModel{"ydb": {Kind: string(platformHeaderKind), Representation: protocol.ValueInput_DESIRED}}},
	}, targets: make(map[string]*target)}
	host.targets[engine.GetId()] = &target{host: host, engine: engine}
	owner := &hclOwner{host: host, engines: []*protocol.Engine{engine}}
	// This test needs the header codec but no provider process or model batch.
	owner.once.Do(func() {})
	owner.headers = func() (headerCodecs, error) {
		return headerCodecs{registry: registry, held: map[schemaext.Kind]schemaext.CodecIdentity{platformHeaderKind: identity}}, nil
	}
	ref := func(name string) objectidentity.ID {
		return objectidentity.ID{Kind: objectidentity.KindTable,
			Schema: objectidentity.Part{Source: "app", Normalized: "app"}, Name: objectidentity.Part{Source: name, Normalized: name}}
	}
	for _, row := range []struct {
		name  string
		state schemaext.KnowledgeState
	}{{"uninspected", schemaext.Uninspected}, {"unrepresentable", schemaext.Unrepresentable}} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			known, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: identity,
				Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete})}}, []schemaext.SubjectCoverage{
				{Kind: platformHeaderKind, Subject: ref("readable"), Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
				{Kind: platformHeaderKind, Subject: ref("unreadable"), Knowledge: schemaext.Knowledge{State: row.state, Reason: "unknown changefeed settings"}},
			})
			c.Assert(err, qt.IsNil)

			encoded, err := owner.EncodeHCL(t.Context(), hclext.EncodeRequest{Target: "ydb", Coverage: known,
				Tables: []hclext.TableFacets{{Table: hclext.Table{Schema: "app", Name: "unreadable"}}}})

			c.Assert(err, qt.IsNil)
			lines := encoded.Header
			c.Assert(lines, qt.HasLen, 1)
			c.Assert(lines[0], qt.Contains, string(platformHeaderKind))
			c.Assert(lines[0], qt.Contains, "unknown changefeed settings")
			decoded, found, err := registry.DecodeCoverageHeader(t.Context(), schemaext.Desired, "// "+lines[0]+"\n")
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(decoded.SubjectRecords(), qt.HasLen, 1)
			c.Assert(decoded.Lookup(platformHeaderKind, ref("unreadable")).State, qt.Equals, row.state)
			c.Assert(decoded.KindRecords()[0].Knowledge.Unscoped().State, qt.Equals, schemaext.Uninspected)
		})
	}
	complete, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: identity,
		Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete})}}, []schemaext.SubjectCoverage{{
		Kind: platformHeaderKind, Subject: ref("readable"), Knowledge: schemaext.Knowledge{State: schemaext.Complete},
	}})
	c.Assert(err, qt.IsNil)
	encoded, err := owner.EncodeHCL(t.Context(), hclext.EncodeRequest{Target: "ydb", Coverage: complete,
		Tables: []hclext.TableFacets{{Table: hclext.Table{Schema: "app", Name: "readable"}}}})
	c.Assert(err, qt.IsNil)
	c.Assert(encoded.Header, qt.HasLen, 0)
	feed := &protocol.Reference{Engine: "ydb", Kind: "table", Schema: "app", Name: "readable", Database: "/local"}
	values := []*protocol.EncodeSourceValue{{Value: &protocol.NormalizedValue{Id: "feed", Value: &protocol.Envelope{
		Kind: string(platformHeaderKind), Owner: feed}, NormalizedOwner: feed}}}
	withLoss, err := owner.coverageAfterHCLLosses(complete, "ydb", values, []*protocol.EncodeLoss{{ValueId: "feed", Message: "changefeed tagged is not represented in HCL"}})
	c.Assert(err, qt.IsNil)
	c.Assert(complete.Lookup(platformHeaderKind, ref("readable")).State, qt.Equals, schemaext.Complete)
	c.Assert(withLoss.Lookup(platformHeaderKind, ref("readable")).State, qt.Equals, schemaext.Unrepresentable)
	lines, err := owner.coverageHeader(t.Context(), host.targets["ydb"].encodeBatch(protocol.SourceDocument_HCL), withLoss)
	c.Assert(err, qt.IsNil)
	c.Assert(lines, qt.HasLen, 1)
	c.Assert(lines[0], qt.Contains, "changefeed tagged is not represented in HCL")
	decoded, found, err := registry.DecodeCoverageHeader(t.Context(), schemaext.Desired, "// "+lines[0]+"\n")
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(decoded.Lookup(platformHeaderKind, ref("readable")).State, qt.Equals, schemaext.Unrepresentable)
}

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

// TestHCLCoverage_WithoutAHeaderEntry asks the extension for the claim of a
// document that writes a block of the engine and no header entry the engine
// reads, either no header at all or an entry of another kind. The document
// claims nothing, and the provider is not asked: this host holds no provider
// client, so asking it would fail the test.
func TestHCLCoverage_WithoutAHeaderEntry(t *testing.T) {
	claim := hclext.Claim{Block: "fixture", Schema: "app", Name: "n1", At: "schema.hcl:1", Range: hclext.Range{Filename: "schema.hcl",
		Start: hclext.Position{Byte: 0, Line: 1, Column: 1}, End: hclext.Position{Byte: 20, Line: 1, Column: 21}}}
	for _, row := range []struct {
		name   string
		limits []coverage.Object
	}{
		{"no header", nil},
		{"an entry of a kind the engine does not read", []coverage.Object{{Kind: "other"}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			extension := hclHost("fixture").hclExtensions()[0]

			known, err := extension.Coverage(hclext.CoverageRequest{Claims: []hclext.Claim{claim}, Limits: row.limits})

			c.Assert(err, qt.IsNil)
			c.Assert(known.IsZero(), qt.IsTrue)
		})
	}
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
