package provider_test

import (
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// ExampleResolveSourceTrace shows how a host locates a captured value without
// reopening its source file. Positions refer to the original captured bytes.
func ExampleResolveSourceTrace() {
	span := &protocol.SourceSpan{Start: &protocol.SourcePosition{Line: 1, Column: 1}, End: &protocol.SourcePosition{ByteOffset: 2, Line: 1, Column: 3}}
	document := &protocol.SourceDocument{Id: "settings", File: "settings.yaml", Format: protocol.SourceDocument_YAML, ByteLength: 2,
		Fragments: []*protocol.SourceFragment{{Id: "retention", Selector: "retention", Span: span,
			Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Number{Number: "60"}}}}}}
	trace := &protocol.SourceTrace{DocumentId: "settings", FragmentId: "retention", Root: protocol.SourceTrace_CONTENT}
	location, err := provider.ResolveSourceTrace("fixture", document, trace)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%s:%d:%d (%d bytes)\n", document.File, location.Start.Line, location.Start.Column, location.End.ByteOffset-location.Start.ByteOffset)
	// Output: settings.yaml:1:1 (2 bytes)
}
