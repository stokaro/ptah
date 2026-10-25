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

// entryKeysClient starts the entry-keys program with a host that lists
// hostFeatures.
func entryKeysClient(c *qt.C, hostFeatures ...string) *transport.Client {
	c.Helper()
	binary := buildFixture(c, "entry-keys")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	return start(c, transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture",
		RequiredFeatures: []string{provider.DecodeSourceFeature}, HostFeatures: hostFeatures})
}

// indexRetention is a YAML document with the retention key of the index
// by_body of the table records, which inherits the index type indexType.
func indexRetention(client *transport.Client, indexType string) *protocol.DecodeSourceRequest {
	engine := client.Description().GetEngines()[0]
	var claim *protocol.YAMLSourceKey
	for _, declared := range engine.GetYamlSourceKeys() {
		if declared.GetScope() == protocol.YAMLSourceKey_INDEX {
			claim = proto.CloneOf(declared)
		}
	}
	span := func(start, end uint32) *protocol.SourceSpan {
		return &protocol.SourceSpan{Start: &protocol.SourcePosition{ByteOffset: uint64(start), Line: 1, Column: start + 1},
			End: &protocol.SourcePosition{ByteOffset: uint64(end), Line: 1, Column: end + 1}}
	}
	seconds := &protocol.SourceEntry{Key: "seconds", KeySpan: span(10, 17), Value: &protocol.SourceValue{Span: span(19, 21), Value: &protocol.SourceValue_Number{Number: "9"}}}
	fragment := &protocol.SourceFragment{Id: "0", YamlKey: claim, Selector: claim.GetSelector(), Span: span(9, 22), BindingId: "records",
		Subject: &protocol.Reference{Engine: "fixture", Kind: "index", Path: []string{"records"}, Name: "by_body"},
		Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: span(9, 22), Value: &protocol.SourceValue_Mapping{
			Mapping: &protocol.SourceMapping{Entries: []*protocol.SourceEntry{seconds}}}}},
		InheritedAttributes: &protocol.SourceMapping{Entries: []*protocol.SourceEntry{{Key: "type", Value: &protocol.SourceValue{Span: span(30, 34),
			Value: &protocol.SourceValue_Text{Text: indexType}}}}}}
	document := &protocol.SourceDocument{Id: "document", Format: protocol.SourceDocument_YAML, File: "schema.yaml", ByteLength: 40,
		Models: []*protocol.ValueModel{proto.CloneOf(engine.GetValueModels()[0])}, Fragments: []*protocol.SourceFragment{fragment},
		Bindings: []*protocol.SourceBinding{{Id: "records", EntryKey: "records", Object: &protocol.Reference{Engine: "fixture", Kind: "table", Name: "records"}}}}
	return &protocol.DecodeSourceRequest{Engine: "fixture", Documents: []*protocol.SourceDocument{document}}
}

// TestEntryKeys_HappyPath negotiates the column and index scopes and the
// inherited lists, and decodes an index key that inherits the index type into
// a value of the index, whose trace resolves to the fragment's position.
func TestEntryKeys_HappyPath(t *testing.T) {
	c := qt.New(t)
	client := entryKeysClient(c, provider.EntryKeysFeature)
	engine := client.Description().GetEngines()[0]
	scopes := make([]protocol.YAMLSourceKey_Scope, 0, len(engine.GetYamlSourceKeys()))
	for _, claim := range engine.GetYamlSourceKeys() {
		scopes = append(scopes, claim.GetScope())
	}
	c.Assert(scopes, qt.DeepEquals, []protocol.YAMLSourceKey_Scope{
		protocol.YAMLSourceKey_DOCUMENT, protocol.YAMLSourceKey_TABLE, protocol.YAMLSourceKey_COLUMN, protocol.YAMLSourceKey_INDEX,
	})
	request := indexRetention(client, "btree")

	decoded, err := client.DecodeSource(c.Context(), request)

	c.Assert(err, qt.IsNil)
	value := decoded.GetDocuments()[0].GetFragments()[0].GetValues()[0]
	c.Assert(string(value.GetValue().GetCanonicalProjection()), qt.Equals, `{"seconds":9}`)
	c.Assert(proto.Equal(value.GetValue().GetValue().GetOwner(), request.GetDocuments()[0].GetFragments()[0].GetSubject()), qt.IsTrue)
	span, err := provider.ResolveSourceTrace("fixture", request.GetDocuments()[0], value.GetSource())
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(span, request.GetDocuments()[0].GetFragments()[0].GetSpan()), qt.IsTrue)
}

// TestEntryKeys_FailurePath refuses an index whose inherited type the
// program cannot hold a retention on, and drops the entry scopes and the
// inherited lists for a host that did not list the feature.
func TestEntryKeys_FailurePath(t *testing.T) {
	t.Run("an inherited type the program refuses", func(t *testing.T) {
		c := qt.New(t)
		client := entryKeysClient(c, provider.EntryKeysFeature)

		decoded, err := client.DecodeSource(c.Context(), indexRetention(client, "hash"))

		c.Assert(err, qt.ErrorMatches, `provider decode source failed \(Unknown\)`)
		c.Assert(decoded, qt.IsNil)
	})
	t.Run("a host without the feature", func(t *testing.T) {
		c := qt.New(t)
		client := entryKeysClient(c)
		engine := client.Description().GetEngines()[0]

		scopes := make([]protocol.YAMLSourceKey_Scope, 0, len(engine.GetYamlSourceKeys()))
		for _, claim := range engine.GetYamlSourceKeys() {
			scopes = append(scopes, claim.GetScope())
		}
		inherited := 0
		for _, claim := range engine.GetGoSourceDirectives() {
			inherited += len(claim.GetInherited())
		}

		c.Assert(scopes, qt.DeepEquals, []protocol.YAMLSourceKey_Scope{protocol.YAMLSourceKey_DOCUMENT, protocol.YAMLSourceKey_TABLE})
		c.Assert(inherited, qt.Equals, 0)
		c.Assert(engine.GetFeatures(), qt.Not(qt.Contains), provider.EntryKeysFeature)
	})
}
