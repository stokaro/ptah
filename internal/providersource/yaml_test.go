package providersource_test

import (
	"context"
	"encoding/binary"
	"testing"
	"unicode/utf16"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/internal/providersource"
)

func capture(c *qt.C, source []byte) yamlext.Document {
	c.Helper()
	var document yamlext.Document
	owners, err := yamlext.NewSet(yamlext.Extension{Owner: "example.org/test", Sections: []yamlext.Section{{Key: "values"}, {Key: "other"}},
		EntrySections: []yamlext.EntrySection{{Key: "values"}}, Document: func(_ context.Context, input yamlext.Document) (yamlext.DocumentResult, error) {
			document = input
			return yamlext.DocumentResult{}, nil
		}})
	c.Assert(err, qt.IsNil)
	_, err = yamlschema.Parse(c.Context(), owners, source)
	c.Assert(err, qt.IsNil)
	return document
}

func sourceEngine() *protocol.Engine {
	return &protocol.Engine{Id: "example", YamlSourceKeys: []*protocol.YAMLSourceKey{
		{Scope: protocol.YAMLSourceKey_DOCUMENT, Key: "values", Selector: "values"},
		{Scope: protocol.YAMLSourceKey_TABLE, Key: "values", Selector: "table-values"},
	}, ValueModels: []*protocol.ValueModel{{Kind: "example.org/test/value", Representation: protocol.ValueInput_DESIRED, SourceFormats: []protocol.SourceDocument_Format{protocol.SourceDocument_YAML}}}}
}

func TestYAMLNumbersRemainExact(t *testing.T) {
	for _, row := range []struct{ source, want string }{
		{"9007199254740993", "9007199254740993"},
		{"184467440737095516160", "184467440737095516160"},
		{"!!int 0x10000000000000001", "18446744073709551617"},
		{"1e1000", "1e1000"}, {"&n 1e1000", "1e1000"},
		{"!!float 1.234567890123456789e1000", "1.234567890123456789e1000"},
	} {
		t.Run(row.source, func(t *testing.T) {
			c := qt.New(t)
			document := capture(c, []byte("values: "+row.source+"\n"))
			request, err := providersource.YAML(c.Context(), sourceEngine(), document)
			c.Assert(err, qt.IsNil)
			c.Assert(request.Documents[0].Fragments[0].GetValue().GetNumber(), qt.Equals, row.want)
		})
	}
}

func TestYAMLStringsRetainExplicitStringSyntax(t *testing.T) {
	for _, source := range []string{"'1e1000'", `"1e1000"`, "!!str 1e1000", "! 1e1000", "&a ! 1e1000", "! &a 1e1000", "&a # note\n  ! 1e1000", "&a # note\n  \"1e1000\"", "|\n  1e1000"} {
		t.Run(source, func(t *testing.T) {
			c := qt.New(t)
			document := capture(c, []byte("values: "+source+"\n"))
			request, err := providersource.YAML(c.Context(), sourceEngine(), document)
			c.Assert(err, qt.IsNil)
			_, text := request.Documents[0].Fragments[0].GetValue().GetValue().(*protocol.SourceValue_Text)
			c.Assert(text, qt.IsTrue)
		})
	}
}

func TestYAMLRejectsUnrepresentableScalars(t *testing.T) {
	for _, source := range []string{".nan", ".inf", "-.inf", "!custom x", "2001-12-15", "{1: value}", "{1e1000: value}"} {
		t.Run(source, func(t *testing.T) {
			c := qt.New(t)
			document := capture(c, []byte("values: "+source+"\n"))
			request, err := providersource.YAML(c.Context(), sourceEngine(), document)
			c.Assert(err, qt.IsNotNil)
			c.Assert(request, qt.IsNil)
		})
	}
}

func TestYAMLBindingsScopeAndAliases(t *testing.T) {
	c := qt.New(t)
	source := []byte("values: &v {count: 9007199254740993, nil: null, yes: true}\nother: ignored\ntables:\n  later:\n    name: records\n    schema: app\n    columns:\n      id: {type: INTEGER}\n    values: *v\n")
	document := capture(c, source)
	request, err := providersource.YAML(c.Context(), sourceEngine(), document)
	c.Assert(err, qt.IsNil)
	output := request.Documents[0]
	c.Assert(output.File, qt.Equals, "<unnamed YAML>")
	c.Assert(output.ByteLength, qt.Equals, uint64(len(source)))
	c.Assert(output.Models, qt.HasLen, 1)
	c.Assert(output.Bindings, qt.HasLen, 1)
	c.Assert(output.Bindings[0].Id, qt.Equals, "later")
	c.Assert(output.Bindings[0].Object.Engine, qt.Equals, "example")
	c.Assert(output.Bindings[0].Object.Schema, qt.Equals, "app")
	c.Assert(output.Bindings[0].Object.Name, qt.Equals, "records")
	c.Assert(output.Fragments, qt.HasLen, 2)
	c.Assert(output.Fragments[1].BindingId, qt.Equals, "later")
	c.Assert(output.Fragments[1].Selector, qt.Equals, "table-values")
	alias := output.Fragments[1].GetValue()
	c.Assert(alias.Span.Start.Line, qt.Equals, uint32(9))
	entries := alias.GetMapping().Entries
	c.Assert(entries[0].Value.GetNumber(), qt.Equals, "9007199254740993")
	c.Assert(entries[0].Value.Span.Start.Line, qt.Equals, uint32(1))
	c.Assert(entries[1].Value.GetNull(), qt.IsNotNil)
	c.Assert(entries[2].Value.GetBoolean(), qt.IsTrue)
}

func TestYAMLMergePrecedence(t *testing.T) {
	c := qt.New(t)
	document := capture(c, []byte("other: &v {a: 1, b: 2}\nvalues: {<<: [*v, {a: 9, c: 3}], b: 4}\n"))
	request, err := providersource.YAML(c.Context(), sourceEngine(), document)
	c.Assert(err, qt.IsNil)
	entries := request.Documents[0].Fragments[0].GetValue().GetMapping().Entries
	c.Assert(entries, qt.HasLen, 3)
	c.Assert(entries[0].Key, qt.Equals, "b")
	c.Assert(entries[0].Value.GetNumber(), qt.Equals, "4")
	c.Assert(entries[1].Key, qt.Equals, "a")
	c.Assert(entries[1].Value.GetNumber(), qt.Equals, "1")
	c.Assert(entries[2].Key, qt.Equals, "c")
	c.Assert(entries[2].Value.GetNumber(), qt.Equals, "3")
}

func TestYAMLUTF16PreservesOriginalCoordinates(t *testing.T) {
	for _, order := range []interface {
		binary.ByteOrder
		binary.AppendByteOrder
	}{binary.LittleEndian, binary.BigEndian} {
		t.Run(order.String(), func(t *testing.T) {
			c := qt.New(t)
			source := order.AppendUint16(nil, 0xfeff)
			for _, unit := range utf16.Encode([]rune("values: [1e1000, 'é😀']\r\n")) {
				source = order.AppendUint16(source, unit)
			}
			document := capture(c, source)
			request, err := providersource.YAML(c.Context(), sourceEngine(), document)
			c.Assert(err, qt.IsNil)
			output := request.Documents[0]
			c.Assert(output.ByteLength, qt.Equals, uint64(len(source)))
			values := output.Fragments[0].GetValue().GetSequence().Values
			c.Assert(values[0].GetNumber(), qt.Equals, "1e1000")
			c.Assert(values[1].GetText(), qt.Equals, "é😀")
			c.Assert(values[1].Span.Start.ByteOffset, qt.Equals, uint64(36))
			c.Assert(values[1].Span.End.ByteOffset, qt.Equals, uint64(46))
		})
	}
}

func TestYAMLEmptyInputAndCancellation(t *testing.T) {
	c := qt.New(t)
	document := capture(c, []byte("tables: {}\n"))
	request, err := providersource.YAML(c.Context(), sourceEngine(), document)
	c.Assert(err, qt.IsNil)
	c.Assert(request.Documents[0].Models, qt.HasLen, 1)
	c.Assert(request.Documents[0].Fragments, qt.HasLen, 0)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	request, err = providersource.YAML(ctx, sourceEngine(), document)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(request, qt.IsNil)
}

func TestYAMLRefusesCyclicAliasGraph(t *testing.T) {
	c := qt.New(t)
	document := capture(c, []byte("values: &v 1\n"))
	node := document.Fragments[0].Node
	node.Kind = yamlext.AliasNode
	node.Alias = node
	request, err := providersource.YAML(c.Context(), sourceEngine(), document)
	c.Assert(err, qt.ErrorMatches, ".*cyclic YAML value.*")
	c.Assert(request, qt.IsNil)
}

func TestYAMLRefusesExpansionBeyondDepthLimit(t *testing.T) {
	c := qt.New(t)
	document := capture(c, []byte("values: [1]\n"))
	root := document.Fragments[0].Node
	node := root.Content[0]
	for range 66 {
		node = &yamlext.Node{Kind: yamlext.SequenceNode, Tag: "!!seq", Range: root.Range, Content: []*yamlext.Node{node}}
	}
	document.Fragments[0].Node = node
	request, err := providersource.YAML(c.Context(), sourceEngine(), document)
	c.Assert(err, qt.ErrorMatches, ".*exceeds source limits.*")
	c.Assert(request, qt.IsNil)
}

func TestYAMLSelectsEachEnginesOwnClaims(t *testing.T) {
	c := qt.New(t)
	document := capture(c, []byte("values: 12\nother: 34\n"))
	first := sourceEngine()
	second := sourceEngine()
	second.Id = "other-engine"
	second.YamlSourceKeys[0].Key = "other"
	for _, row := range []struct {
		engine *protocol.Engine
		want   string
	}{{first, "12"}, {second, "34"}} {
		t.Run(row.engine.Id, func(t *testing.T) {
			c := qt.New(t)
			request, err := providersource.YAML(c.Context(), row.engine, document)
			c.Assert(err, qt.IsNil)
			c.Assert(request.Engine, qt.Equals, row.engine.Id)
			c.Assert(request.Documents[0].Fragments, qt.HasLen, 1)
			c.Assert(request.Documents[0].Fragments[0].GetValue().GetNumber(), qt.Equals, row.want)
		})
	}
}
