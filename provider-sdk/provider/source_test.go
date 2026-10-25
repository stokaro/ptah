package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func sourceSpan() *protocol.SourceSpan {
	return &protocol.SourceSpan{Start: &protocol.SourcePosition{Line: 1, Column: 1}, End: &protocol.SourcePosition{ByteOffset: 100, Line: 1, Column: 101}}
}

func sourceDocument() *protocol.SourceDocument {
	return &protocol.SourceDocument{Id: "doc", Format: protocol.SourceDocument_YAML, File: "schema.yaml", ByteLength: 100,
		Bindings: []*protocol.SourceBinding{{Id: "table", Object: &protocol.Reference{Engine: "fixture", Kind: "table", Name: "a.b", Schema: "c", Path: []string{"d/e", "f"}}, StructName: "Record", EntryKey: "record"}},
		Fragments: []*protocol.SourceFragment{{Id: "settings", Selector: "retention", BindingId: "table", Span: sourceSpan(),
			Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Number{Number: "9007199254740993"}}}}}}
}

func TestSourceNumbersRemainExactText(t *testing.T) {
	for _, number := range []string{"0", "-0", "9007199254740993", "18446744073709551616", "0.12345678901234567890123456789", "1e1000000", "-1.25E-90"} {
		t.Run(number, func(t *testing.T) {
			c := qt.New(t)
			document := sourceDocument()
			document.Fragments[0].GetValue().Value = &protocol.SourceValue_Number{Number: number}
			original := proto.CloneOf(document)
			c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.IsNil)
			c.Assert(proto.Equal(original, document), qt.IsTrue)
			wire, err := proto.Marshal(document)
			c.Assert(err, qt.IsNil)
			decoded := new(protocol.SourceDocument)
			c.Assert(proto.Unmarshal(wire, decoded), qt.IsNil)
			c.Assert(decoded.Fragments[0].GetValue().GetNumber(), qt.Equals, number)
		})
	}
	for _, number := range []string{"", " 1", "1 ", "01", "+1", "NaN", "Infinity", "0x10", "1/3", "1.", ".1", "1e", "true", `"1"`, "[]", "1 2"} {
		t.Run("invalid "+number, func(t *testing.T) {
			c := qt.New(t)
			document := sourceDocument()
			document.Fragments[0].GetValue().Value = &protocol.SourceValue_Number{Number: number}
			c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestSourceDocumentsRefuseAmbiguousOrLostProvenance(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.SourceDocument)
	}{
		{"missing file", func(d *protocol.SourceDocument) { d.File = "" }},
		{"unknown format", func(d *protocol.SourceDocument) { d.Format = 99 }},
		{"nil binding", func(d *protocol.SourceDocument) { d.Bindings[0] = nil }},
		{"duplicate binding", func(d *protocol.SourceDocument) { d.Bindings = append(d.Bindings, proto.CloneOf(d.Bindings[0])) }},
		{"foreign binding", func(d *protocol.SourceDocument) { d.Bindings[0].Object.Engine = "other" }},
		{"unknown binding", func(d *protocol.SourceDocument) { d.Fragments[0].BindingId = "unknown" }},
		{"nil fragment", func(d *protocol.SourceDocument) { d.Fragments[0] = nil }},
		{"duplicate fragment", func(d *protocol.SourceDocument) { d.Fragments = append(d.Fragments, proto.CloneOf(d.Fragments[0])) }},
		{"missing selector", func(d *protocol.SourceDocument) { d.Fragments[0].Selector = "" }},
		{"missing content", func(d *protocol.SourceDocument) { d.Fragments[0].Content = nil }},
		{"typed nil content", func(d *protocol.SourceDocument) { d.Fragments[0].Content = (*protocol.SourceFragment_Value)(nil) }},
		{"wrong format", func(d *protocol.SourceDocument) { d.Format = protocol.SourceDocument_HCL }},
		{"missing location", func(d *protocol.SourceDocument) { d.Fragments[0].Span = nil }},
		{"outside file", func(d *protocol.SourceDocument) { d.ByteLength = 99 }},
		{"inconsistent columns", func(d *protocol.SourceDocument) { d.Fragments[0].Span.End.Column = 100 }},
		{"invented first line", func(d *protocol.SourceDocument) { d.Fragments[0].Span.Start.Column = 2 }},
		{"missing value position", func(d *protocol.SourceDocument) { d.Fragments[0].GetValue().Span = nil }},
		{"missing value", func(d *protocol.SourceDocument) { d.Fragments[0].GetValue().Value = nil }},
		{"typed nil value", func(d *protocol.SourceDocument) { d.Fragments[0].GetValue().Value = (*protocol.SourceValue_Text)(nil) }},
		{"nil null", func(d *protocol.SourceDocument) { d.Fragments[0].GetValue().Value = &protocol.SourceValue_Null{} }},
		{"nil sequence", func(d *protocol.SourceDocument) { d.Fragments[0].GetValue().Value = &protocol.SourceValue_Sequence{} }},
		{"nil mapping", func(d *protocol.SourceDocument) { d.Fragments[0].GetValue().Value = &protocol.SourceValue_Mapping{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			document := sourceDocument()
			c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.IsNil)
			test.mutate(document)
			c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestSourceTreeDistinguishesNullEmptyAndMissing(t *testing.T) {
	c := qt.New(t)
	document := sourceDocument()
	value := document.Fragments[0].GetValue()
	value.Value = &protocol.SourceValue_Mapping{Mapping: &protocol.SourceMapping{Entries: []*protocol.SourceEntry{{
		Key: "explicit", KeySpan: sourceSpan(), Value: &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Null{Null: &protocol.Empty{}}},
	}}}}
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.IsNil)
	mapping := value.GetMapping()
	mapping.Entries = append(mapping.Entries, proto.CloneOf(mapping.Entries[0]))
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.ErrorIs, provider.ErrProtocol)
	mapping.Entries = nil
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.IsNil)
	value.Value = &protocol.SourceValue_Sequence{Sequence: &protocol.SourceSequence{}}
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.IsNil)
	value.Value = &protocol.SourceValue_Boolean{Boolean: false}
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.IsNil)
	value.Value = &protocol.SourceValue_Text{Text: ""}
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.IsNil)
}

func TestSourceTreeRefusesCyclesAndDepthBeforeTransport(t *testing.T) {
	c := qt.New(t)
	document := sourceDocument()
	value := document.Fragments[0].GetValue()
	value.Value = &protocol.SourceValue_Sequence{Sequence: &protocol.SourceSequence{Values: []*protocol.SourceValue{value}}}
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.ErrorIs, provider.ErrProtocol)
	document = sourceDocument()
	value = document.Fragments[0].GetValue()
	for range provider.MaxSourceDepth + 1 {
		child := &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Text{Text: "leaf"}}
		value.Value = &protocol.SourceValue_Sequence{Sequence: &protocol.SourceSequence{Values: []*protocol.SourceValue{child}}}
		value = child
	}
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.ErrorIs, provider.ErrProtocol)
}

func TestSourceFormatsRetainBlocksAndRawSQL(t *testing.T) {
	c := qt.New(t)
	document := sourceDocument()
	document.Format = protocol.SourceDocument_HCL
	block := &protocol.SourceBlock{Type: "retention", Labels: []*protocol.SourceLabel{{Value: "Name", Span: sourceSpan()}}, Span: sourceSpan()}
	document.Fragments[0].Content = &protocol.SourceFragment_Block{Block: block}
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.IsNil)
	block.Labels[0].Span = nil
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.ErrorIs, provider.ErrProtocol)
	block.Labels[0].Span = sourceSpan()
	block.Children = []*protocol.SourceBlock{block}
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.ErrorIs, provider.ErrProtocol)

	document = sourceDocument()
	document.Format = protocol.SourceDocument_SQL
	document.Fragments[0].Content = &protocol.SourceFragment_Sql{Sql: "SELECT 1;"}
	document.Fragments[0].Span.End = &protocol.SourcePosition{ByteOffset: 9, Line: 1, Column: 10}
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.IsNil)
	document.Fragments[0].Content = &protocol.SourceFragment_Sql{Sql: "SELECT 1; -- lost source position"}
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.ErrorIs, provider.ErrProtocol)
}

func TestSourceInheritedDefaultsDoNotInventAuthoredPositions(t *testing.T) {
	c := qt.New(t)
	document := sourceDocument()
	document.Fragments[0].InheritedAttributes = &protocol.SourceMapping{Entries: []*protocol.SourceEntry{{Key: "default", Value: &protocol.SourceValue{Value: &protocol.SourceValue_Text{Text: "value"}}}}}
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.IsNil)
	document.Fragments[0].GetValue().Value = &protocol.SourceValue_Mapping{Mapping: document.Fragments[0].InheritedAttributes}
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document}), qt.ErrorIs, provider.ErrProtocol)
}

func TestSourceBatchRequiresUniqueDocuments(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.ValidateSourceDocuments("fixture", nil), qt.IsNil)
	c.Assert(provider.ValidateSourceDocuments("", nil), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{nil}), qt.ErrorIs, provider.ErrProtocol)
	document := sourceDocument()
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{document, proto.CloneOf(document)}), qt.ErrorIs, provider.ErrProtocol)
}
