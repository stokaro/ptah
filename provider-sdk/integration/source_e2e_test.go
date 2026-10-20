//go:build integration

package integration_test

import (
	"context"
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

func TestIndependentProviderDecodesCapturedSourcesWithoutSession(t *testing.T) {
	c := qt.New(t)
	binary := buildFixture(c, "provider")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	client := start(c, transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture", RequiredFeatures: []string{provider.DecodeSourceFeature}})
	model := client.Description().GetEngines()[0].GetValueModels()[0]
	for _, format := range []protocol.SourceDocument_Format{protocol.SourceDocument_YAML, protocol.SourceDocument_HCL, protocol.SourceDocument_GO_ANNOTATIONS} {
		t.Run(format.String(), func(t *testing.T) {
			c := qt.New(t)
			request := capturedRetention(model, format)
			original := proto.CloneOf(request)
			result, err := client.DecodeSource(c.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(proto.Equal(original, request), qt.IsTrue)
			c.Assert(result.GetDocuments(), qt.HasLen, 1)
			decoded := result.Documents[0]
			c.Assert(decoded.Fragments, qt.HasLen, 2)
			c.Assert(string(decoded.Fragments[0].Values[0].Value.CanonicalProjection), qt.Equals, `{"seconds":9007199254740993}`)
			c.Assert(string(decoded.Fragments[1].Values[0].Value.CanonicalProjection), qt.Equals, `{"seconds":60}`)
			c.Assert(decoded.Coverage.Subjects[0].Knowledge.State, qt.Equals, protocol.ModelKnowledge_COMPLETE)
			c.Assert(decoded.Coverage.Subjects[1].Knowledge.State, qt.Equals, protocol.ModelKnowledge_DEFAULTED)
			c.Assert(proto.Equal(decoded.Fragments[0].Values[0].Value.Value.Owner, request.Documents[0].Bindings[1].Object), qt.IsTrue)
			span, err := provider.ResolveSourceTrace("fixture", request.Documents[0], decoded.Fragments[0].Values[0].Source)
			c.Assert(err, qt.IsNil)
			c.Assert(proto.Equal(span, request.Documents[0].Fragments[0].Span), qt.IsTrue)
			again, err := client.DecodeSource(c.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(proto.Equal(result, again), qt.IsTrue)
		})
	}
	for _, test := range []struct {
		name  string
		value *protocol.SourceValue
	}{
		{"explicit null", &protocol.SourceValue{Value: &protocol.SourceValue_Null{Null: &protocol.Empty{}}}},
		{"overflow", &protocol.SourceValue{Value: &protocol.SourceValue_Number{Number: "18446744073709551616"}}},
		{"text", &protocol.SourceValue{Value: &protocol.SourceValue_Text{Text: "60"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := capturedRetention(model, protocol.SourceDocument_YAML)
			bad := proto.CloneOf(test.value)
			bad.Span = captureSpan()
			request.Documents[0].Fragments[1].GetValue().GetMapping().Entries = append(request.Documents[0].Fragments[1].GetValue().GetMapping().Entries, &protocol.SourceEntry{Key: "seconds", KeySpan: captureSpan(), Value: bad})
			result, err := client.DecodeSource(c.Context(), request)
			c.Assert(err, qt.IsNotNil)
			c.Assert(result, qt.IsNil)
		})
	}
	request := capturedRetention(model, protocol.SourceDocument_YAML)
	request.Documents[0].Fragments[1].GetValue().GetMapping().Entries = append(request.Documents[0].Fragments[1].GetValue().GetMapping().Entries, &protocol.SourceEntry{Key: "unknown", KeySpan: captureSpan(), Value: &protocol.SourceValue{Span: captureSpan(), Value: &protocol.SourceValue_Boolean{Boolean: false}}})
	refused, err := client.DecodeSource(c.Context(), request)
	c.Assert(err, qt.IsNotNil)
	c.Assert(refused, qt.IsNil)
	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	refused, err = client.DecodeSource(canceled, capturedRetention(model, protocol.SourceDocument_YAML))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(refused, qt.IsNil)
}

func captureSpan() *protocol.SourceSpan {
	return &protocol.SourceSpan{Start: &protocol.SourcePosition{Line: 1, Column: 1}, End: &protocol.SourcePosition{ByteOffset: 100, Line: 1, Column: 101}}
}

func capturedRetention(model *protocol.ValueModel, format protocol.SourceDocument_Format) *protocol.DecodeSourceRequest {
	doc := &protocol.SourceDocument{Id: "document", Format: format, File: "not-read-from-disk", ByteLength: 100, Models: []*protocol.ValueModel{proto.CloneOf(model)}}
	for _, name := range []string{"first", "later"} {
		doc.Bindings = append(doc.Bindings, &protocol.SourceBinding{Id: name, Object: &protocol.Reference{Engine: "fixture", Kind: "table", Name: name, Database: "db", Schema: "a.b", Path: []string{"c/d", "e"}}})
	}
	for _, name := range []string{"later", "first"} {
		mapping := &protocol.SourceMapping{Entries: []*protocol.SourceEntry{{Key: "on", KeySpan: captureSpan(), Value: &protocol.SourceValue{Span: captureSpan(), Value: &protocol.SourceValue_Text{Text: name}}}}}
		fragment := &protocol.SourceFragment{Id: name, Selector: "retention", Span: captureSpan()}
		switch format {
		case protocol.SourceDocument_HCL:
			fragment.Content = &protocol.SourceFragment_Block{Block: &protocol.SourceBlock{Type: "retention", Span: captureSpan(), Attributes: mapping}}
		default:
			fragment.Content = &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: captureSpan(), Value: &protocol.SourceValue_Mapping{Mapping: mapping}}}
		}
		if name == "later" {
			mapping.Entries = append(mapping.Entries, &protocol.SourceEntry{Key: "seconds", KeySpan: captureSpan(), Value: &protocol.SourceValue{Span: captureSpan(), Value: &protocol.SourceValue_Number{Number: "9007199254740993"}}})
		}
		doc.Fragments = append(doc.Fragments, fragment)
	}
	return &protocol.DecodeSourceRequest{Engine: "fixture", Documents: []*protocol.SourceDocument{doc}}
}
