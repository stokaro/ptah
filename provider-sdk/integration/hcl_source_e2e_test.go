//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

// hclClaimsClient starts the hcl-claims program with a host that lists
// hostFeatures.
func hclClaimsClient(c *qt.C, hostFeatures ...string) *transport.Client {
	c.Helper()
	binary := buildFixture(c, "hcl-claims")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	return start(c, transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture",
		RequiredFeatures: []string{provider.DecodeSourceFeature}, HostFeatures: hostFeatures})
}

// hclDocument is an HCL document with a top-level retention block that names
// the table events, and the retention attribute of the index by_body of the
// table records, which inherits the index type indexType.
func hclDocument(client *transport.Client, indexType string) *protocol.DecodeSourceRequest {
	engine := client.Description().GetEngines()[0]
	claim := func(site protocol.HCLSourceBlock_Site) *protocol.HCLSourceBlock {
		for _, block := range engine.GetHclSource().GetBlocks() {
			if block.GetSite() == site {
				return proto.CloneOf(block)
			}
		}
		return nil
	}
	span := func(start, end uint32) *protocol.SourceSpan {
		return &protocol.SourceSpan{Start: &protocol.SourcePosition{ByteOffset: uint64(start), Line: 1, Column: start + 1},
			End: &protocol.SourcePosition{ByteOffset: uint64(end), Line: 1, Column: end + 1}}
	}
	text := func(key, value string, at *protocol.SourceSpan) *protocol.SourceMapping {
		return &protocol.SourceMapping{Entries: []*protocol.SourceEntry{{Key: key, KeySpan: at, Value: &protocol.SourceValue{Span: at, Value: &protocol.SourceValue_Text{Text: value}}}}}
	}
	top, index := claim(protocol.HCLSourceBlock_TOP_LEVEL), claim(protocol.HCLSourceBlock_INDEX_ATTRIBUTES)
	fragments := []*protocol.SourceFragment{
		{Id: "0", HclBlock: top, Selector: top.GetSelector(), Span: span(0, 40), Content: &protocol.SourceFragment_Block{Block: &protocol.SourceBlock{
			Type: "retention", Span: span(0, 40), Labels: []*protocol.SourceLabel{{Value: "public", Span: span(10, 18)}, {Value: "events", Span: span(19, 27)}},
			Attributes: text("seconds", "30", span(30, 38))}}},
		{Id: "1", HclBlock: index, Selector: index.GetSelector(), Span: span(41, 90), BindingId: "records",
			Subject:             &protocol.Reference{Engine: "fixture", Kind: "index", Schema: "public", Path: []string{"records"}, Name: "by_body"},
			Content:             &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: span(41, 90), Value: &protocol.SourceValue_Mapping{Mapping: text("retention_seconds", "9", span(60, 80))}}},
			InheritedAttributes: &protocol.SourceMapping{Entries: []*protocol.SourceEntry{{Key: "type", Value: &protocol.SourceValue{Span: span(81, 89), Value: &protocol.SourceValue_Text{Text: indexType}}}}}},
	}
	document := &protocol.SourceDocument{Id: "document", Format: protocol.SourceDocument_HCL, File: "schema.hcl", ByteLength: 100,
		Models: []*protocol.ValueModel{proto.CloneOf(engine.GetValueModels()[0])}, Fragments: fragments,
		Bindings: []*protocol.SourceBinding{{Id: "records", Object: &protocol.Reference{Engine: "fixture", Kind: "table", Schema: "public", Name: "records"}}}}
	return &protocol.DecodeSourceRequest{Engine: "fixture", Documents: []*protocol.SourceDocument{document}}
}

// TestHCLSource_HappyPath negotiates the HCL claims and decodes a top-level
// block into a setting of the table its labels name, and an index attribute
// into a value of the index, each traced to its fragment's position.
func TestHCLSource_HappyPath(t *testing.T) {
	c := qt.New(t)
	client := hclClaimsClient(c, provider.HCLSourceFeature)
	c.Assert(client.Description().GetEngines()[0].GetHclSource().GetBlocks(), qt.HasLen, 3)
	request := hclDocument(client, "BTREE")

	decoded, err := client.DecodeSource(c.Context(), request)

	c.Assert(err, qt.IsNil)
	fragments := decoded.GetDocuments()[0].GetFragments()
	top, index := fragments[0].GetValues()[0], fragments[1].GetValues()[0]
	c.Assert(string(top.GetValue().GetCanonicalProjection()), qt.Equals, `{"seconds":30}`)
	c.Assert(top.GetValue().GetValue().GetOwner().GetName(), qt.Equals, "events")
	c.Assert(string(index.GetValue().GetCanonicalProjection()), qt.Equals, `{"seconds":9}`)
	c.Assert(proto.Equal(index.GetValue().GetValue().GetOwner(), request.GetDocuments()[0].GetFragments()[1].GetSubject()), qt.IsTrue)
	span, err := provider.ResolveSourceTrace("fixture", request.GetDocuments()[0], index.GetSource())
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(span, request.GetDocuments()[0].GetFragments()[1].GetSpan()), qt.IsTrue)
}

// TestHCLSource_FailurePath refuses an index whose inherited type the program
// cannot hold a retention on, a document whose header limit names a table it
// declares a retention of, and drops the claims for a host that did not list
// the feature.
func TestHCLSource_FailurePath(t *testing.T) {
	t.Run("an inherited type the program refuses", func(t *testing.T) {
		c := qt.New(t)
		client := hclClaimsClient(c, provider.HCLSourceFeature)

		decoded, err := client.DecodeSource(c.Context(), hclDocument(client, "HASH"))

		c.Assert(err, qt.ErrorMatches, `provider decode source failed \(Unknown\)`)
		c.Assert(decoded, qt.IsNil)
	})
	t.Run("a header limit on a declared table", func(t *testing.T) {
		c := qt.New(t)
		client := hclClaimsClient(c, provider.HCLSourceFeature)
		request := hclDocument(client, "BTREE")
		request.Documents[0].Limits = []*protocol.SchemaCoverageLimit{{Kind: "retention", Name: "events", Reason: "not-described", Provenance: "declared"}}

		decoded, err := client.DecodeSource(c.Context(), request)

		c.Assert(err, qt.ErrorMatches, `provider decode source failed \(Unknown\)`)
		c.Assert(decoded, qt.IsNil)
	})
	t.Run("a host without the feature", func(t *testing.T) {
		c := qt.New(t)
		client := hclClaimsClient(c)

		engine := client.Description().GetEngines()[0]

		c.Assert(engine.GetHclSource(), qt.IsNil)
		c.Assert(engine.GetFeatures(), qt.Not(qt.Contains), provider.HCLSourceFeature)
	})
}
