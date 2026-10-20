package provider_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func TestSourceTracePreservesTypedPathsAndMissingDefaults(t *testing.T) {
	c := qt.New(t)
	doc := sourceDocument()
	leaf := doc.Fragments[0].GetValue()
	doc.Fragments[0].Content = &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Mapping{Mapping: &protocol.SourceMapping{Entries: []*protocol.SourceEntry{{Key: "0", KeySpan: sourceSpan(), Value: &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Sequence{Sequence: &protocol.SourceSequence{Values: []*protocol.SourceValue{leaf}}}}}}}}}}
	trace := &protocol.SourceTrace{DocumentId: "doc", FragmentId: "settings", Root: protocol.SourceTrace_CONTENT, Path: []*protocol.SourcePathStep{{Step: &protocol.SourcePathStep_Key{Key: "0"}}, {Step: &protocol.SourcePathStep_Index{Index: 0}}}}
	span, err := provider.ResolveSourceTrace("fixture", doc, trace)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(span, leaf.Span), qt.IsTrue)
	span.Start.Column = 99
	c.Assert(leaf.Span.Start.Column, qt.Equals, uint32(1))
	for _, path := range [][]*protocol.SourcePathStep{
		{{Step: &protocol.SourcePathStep_Index{Index: 0}}},
		{{Step: &protocol.SourcePathStep_Key{Key: "missing"}}},
		{{Step: &protocol.SourcePathStep_Key{Key: "0"}}, {Step: &protocol.SourcePathStep_Key{Key: "0"}}},
		{{Step: &protocol.SourcePathStep_Key{Key: "0"}}, {Step: &protocol.SourcePathStep_Index{Index: 1}}},
		{nil},
		{{Step: (*protocol.SourcePathStep_Key)(nil)}},
	} {
		trace.Path = path
		span, err = provider.ResolveSourceTrace("fixture", doc, trace)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(span, qt.IsNil)
	}
	doc.Fragments[0].InheritedAttributes = &protocol.SourceMapping{Entries: []*protocol.SourceEntry{{Key: "default", Value: &protocol.SourceValue{Value: &protocol.SourceValue_Boolean{Boolean: false}}}}}
	trace.Root = protocol.SourceTrace_INHERITED_ATTRIBUTES
	trace.Path = []*protocol.SourcePathStep{{Step: &protocol.SourcePathStep_Key{Key: "default"}}}
	span, err = provider.ResolveSourceTrace("fixture", doc, trace)
	c.Assert(err, qt.IsNil)
	c.Assert(span, qt.IsNil)
	trace.Path = []*protocol.SourcePathStep{{Step: &protocol.SourcePathStep_Key{Key: "missing"}}}
	span, err = provider.ResolveSourceTrace("fixture", doc, trace)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(span, qt.IsNil)
}

func TestSourceBlockTraceSeparatesLabelsAttributesAndChildren(t *testing.T) {
	c := qt.New(t)
	doc := sourceDocument()
	doc.Format = protocol.SourceDocument_HCL
	leaf := doc.Fragments[0].GetValue()
	child := &protocol.SourceBlock{Type: "child", Span: sourceSpan(), Labels: []*protocol.SourceLabel{{Value: "label", Span: sourceSpan()}}, Attributes: &protocol.SourceMapping{Entries: []*protocol.SourceEntry{{Key: "child", KeySpan: sourceSpan(), Value: leaf}}}}
	doc.Fragments[0].Content = &protocol.SourceFragment_Block{Block: &protocol.SourceBlock{Type: "parent", Span: sourceSpan(), Children: []*protocol.SourceBlock{child}}}
	for _, tail := range []*protocol.SourcePathStep{{Step: &protocol.SourcePathStep_Attribute{Attribute: "child"}}, {Step: &protocol.SourcePathStep_Label{Label: 0}}} {
		trace := &protocol.SourceTrace{DocumentId: doc.Id, FragmentId: "settings", Root: protocol.SourceTrace_CONTENT, Path: []*protocol.SourcePathStep{{Step: &protocol.SourcePathStep_Child{Child: 0}}, tail}}
		span, err := provider.ResolveSourceTrace("fixture", doc, trace)
		c.Assert(err, qt.IsNil)
		c.Assert(proto.Equal(span, sourceSpan()), qt.IsTrue)
	}
	for _, path := range [][]*protocol.SourcePathStep{
		{{Step: &protocol.SourcePathStep_Key{Key: "child"}}},
		{{Step: &protocol.SourcePathStep_Child{Child: 1}}},
		{{Step: &protocol.SourcePathStep_Child{Child: 0}}, {Step: &protocol.SourcePathStep_Label{Label: 1}}},
		{{Step: &protocol.SourcePathStep_Child{Child: 0}}, {Step: &protocol.SourcePathStep_Label{Label: 0}}, {Step: &protocol.SourcePathStep_Key{Key: "value"}}},
	} {
		span, err := provider.ResolveSourceTrace("fixture", doc, &protocol.SourceTrace{DocumentId: doc.Id, FragmentId: "settings", Root: protocol.SourceTrace_CONTENT, Path: path})
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(span, qt.IsNil)
	}
}

func TestSourceLimitsIncludeSQLFragmentsAndNestedValues(t *testing.T) {
	c := qt.New(t)
	doc := sourceDocument()
	value := doc.Fragments[0].GetValue()
	for range provider.MaxSourceDepth {
		value = &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Sequence{Sequence: &protocol.SourceSequence{Values: []*protocol.SourceValue{value}}}}
	}
	doc.Fragments[0].Content = &protocol.SourceFragment_Value{Value: value}
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{doc}), qt.IsNil)
	doc.Fragments[0].Content = &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Sequence{Sequence: &protocol.SourceSequence{Values: []*protocol.SourceValue{value}}}}}
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{doc}), qt.ErrorIs, provider.ErrProtocol)
	sql := &protocol.SourceDocument{Id: "sql", File: "captured.sql", Format: protocol.SourceDocument_SQL, ByteLength: 1}
	for i := range provider.MaxSourceNodes - 1 {
		sql.Fragments = append(sql.Fragments, &protocol.SourceFragment{Id: fmt.Sprint(i), Selector: "sql", Span: &protocol.SourceSpan{Start: &protocol.SourcePosition{Line: 1, Column: 1}, End: &protocol.SourcePosition{ByteOffset: 1, Line: 1, Column: 2}}, Content: &protocol.SourceFragment_Sql{Sql: "x"}})
	}
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{sql}), qt.IsNil)
	extra := proto.CloneOf(sql.Fragments[0])
	extra.Id = "extra"
	sql.Fragments = append(sql.Fragments, extra)
	c.Assert(provider.ValidateSourceDocuments("fixture", []*protocol.SourceDocument{sql}), qt.ErrorIs, provider.ErrProtocol)
}
