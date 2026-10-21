package yamlschema_test

import (
	"context"
	"encoding/binary"
	"testing"
	"unicode/utf16"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

func captureOwnerDocument(t *testing.T, source []byte) yamlext.Document {
	t.Helper()
	c := qt.New(t)
	var captured yamlext.Document
	calls := 0
	owners, err := yamlext.NewSet(documentOwner(func(_ context.Context, document yamlext.Document) (yamlext.DocumentResult, error) {
		calls++
		captured = document
		return yamlext.DocumentResult{}, nil
	}))
	c.Assert(err, qt.IsNil)
	_, err = yamlschema.Parse(t.Context(), owners, source)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	return captured
}

func TestOwnerRangesSliceOriginalContent(t *testing.T) {
	cases := []struct{ name, source, want string }{
		{"quoted Unicode", "shades: \"é😀\" # comment\n", `"é😀"`},
		{"escaped quote", `shades: "a\"b\u00e9"`, `"a\"b\u00e9"`},
		{"single quote", "shades: 'a''b'\n", "'a''b'"},
		{"plain multiline", "shades: first\n  second # comment\npalettes: []\n", "first\n  second"},
		{"literal keep", "shades: |+\n  é\n\npalettes: []\n", "|+\n  é"},
		{"folded CRLF", "shades: >-\r\n  first\r\n  second\r\npalettes: []\r\n", ">-\r\n  first\r\n  second"},
		{"block mapping", "shades:\n  a: 1\n  b: 2 # tail\npalettes: []\n", "a: 1\n  b: 2"},
		{"block sequence", "shades:\n  - a\n  - b\npalettes: []\n", "- a\n  - b"},
		{"nested flow", "shades: [{a: 1}, {b: [2, 3]}] # tail\n", "[{a: 1}, {b: [2, 3]}]"},
		{"empty flow", "shades: {}", "{}"},
		{"implicit null", "shades:\n", ""},
		{"empty literal", "shades: |\npalettes: []\n", "|\n"},
		{"empty tagged string", "shades: !!str\n", "!!str"},
		{"properties", "shades: &label !!str 1e1000\n", "&label !!str 1e1000"},
		{"nonspecific tag", "shades: ! 1e1000\n", "! 1e1000"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			document := captureOwnerDocument(t, []byte(test.source))
			span := document.Fragments[0].Node.Range
			c.Assert(string(document.Source[span.Start.ByteOffset:span.End.ByteOffset]), qt.Equals, test.want)
		})
	}
}

func TestOwnerPositionsCountOriginalBytes(t *testing.T) {
	c := qt.New(t)
	document := captureOwnerDocument(t, []byte("shades: {é: \"😀\"}\r\npalettes: [x]\r\n"))
	mapping := document.Fragments[0].Node
	c.Assert(mapping.Content[0].Range, qt.DeepEquals, yamlext.SourceRange{
		Start: yamlext.SourcePosition{ByteOffset: 9, Line: 1, Column: 10},
		End:   yamlext.SourcePosition{ByteOffset: 11, Line: 1, Column: 12},
	})
	c.Assert(mapping.Content[1].Range, qt.DeepEquals, yamlext.SourceRange{
		Start: yamlext.SourcePosition{ByteOffset: 13, Line: 1, Column: 14},
		End:   yamlext.SourcePosition{ByteOffset: 19, Line: 1, Column: 20},
	})
	c.Assert(document.Fragments[1].Node.Range, qt.DeepEquals, yamlext.SourceRange{
		Start: yamlext.SourcePosition{ByteOffset: 32, Line: 2, Column: 11},
		End:   yamlext.SourcePosition{ByteOffset: 35, Line: 2, Column: 14},
	})
}

func TestOwnerAliasRangesDistinguishUseAndDeclaration(t *testing.T) {
	c := qt.New(t)
	document := captureOwnerDocument(t, []byte("shades: &colors [é, blue]\npalettes: *colors\n"))
	definition := document.Fragments[0].Node
	alias := document.Fragments[1].Node
	c.Assert(alias.Alias, qt.Equals, definition)
	c.Assert(string(document.Source[alias.Range.Start.ByteOffset:alias.Range.End.ByteOffset]), qt.Equals, "*colors")
	c.Assert(string(document.Source[definition.Range.Start.ByteOffset:definition.Range.End.ByteOffset]), qt.Equals, "&colors [é, blue]")
	c.Assert(string(document.Source[definition.Content[0].Range.Start.ByteOffset:definition.Content[0].Range.End.ByteOffset]), qt.Equals, "é")
}

func TestOwnerRangesPreserveSourceEncoding(t *testing.T) {
	cases := []struct {
		name   string
		encode func(string) []byte
		prefix int
		width  int
	}{
		{"UTF-8 BOM", func(s string) []byte { return append([]byte{0xef, 0xbb, 0xbf}, []byte(s)...) }, 3, 1},
		{"UTF-16 LE", func(s string) []byte { return encodeUTF16(s, binary.LittleEndian, []byte{0xff, 0xfe}) }, 2, 2},
		{"UTF-16 BE", func(s string) []byte { return encodeUTF16(s, binary.BigEndian, []byte{0xfe, 0xff}) }, 2, 2},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := test.encode("shades: \"é😀\"\n")
			document := captureOwnerDocument(t, source)
			span := document.Fragments[0].Node.Range
			c.Assert(document.Source, qt.DeepEquals, source)
			c.Assert(span.Start.ByteOffset, qt.Equals, test.prefix+8*test.width)
			c.Assert(span.Start.Column, qt.Equals, span.Start.ByteOffset+1)
			c.Assert(source[span.Start.ByteOffset:span.End.ByteOffset], qt.DeepEquals, test.encode(`"é😀"`)[test.prefix:])
		})
	}
}

func encodeUTF16(value string, order binary.AppendByteOrder, prefix []byte) []byte {
	encoded := prefix
	for _, unit := range utf16.Encode([]rune(value)) {
		encoded = order.AppendUint16(encoded, unit)
	}
	return encoded
}

func TestOwnerRangesRecognizeYAMLLineBreaks(t *testing.T) {
	for _, separator := range []string{"\n", "\r\n", "\r", "\u0085", "\u2028", "\u2029"} {
		t.Run(separator, func(t *testing.T) {
			c := qt.New(t)
			source := "shades: [é]" + separator + "palettes: []"
			document := captureOwnerDocument(t, []byte(source))
			span := document.Fragments[1].Node.Range
			c.Assert(span.Start.Line, qt.Equals, 2)
			c.Assert(span.Start.Column, qt.Equals, 11)
			c.Assert(string(document.Source[span.Start.ByteOffset:span.End.ByteOffset]), qt.Equals, "[]")
		})
	}
}
