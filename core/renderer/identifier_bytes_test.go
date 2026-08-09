package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer"
)

// The identifiers this file renders, named by code point rather than typed
// literally so the bytes each one stands for are stated and not inferred from
// how an editor happened to normalize the file. TestIdentifierFixtureBytes pins
// the encoding of each, so a change to these constants reddens there rather
// than quietly weakening every assertion below.
const (
	// asciiIdentifier is the CONTROL. Every dialect renders it byte for byte
	// both before and after the fix for stokaro/ptah#1352, so a suite that
	// only ever compares output to input would pass on it alone.
	asciiIdentifier = "orders"

	// multiByteIdentifier is U+00C4 LATIN CAPITAL LETTER A WITH DIAERESIS,
	// UTF-8 C3 84. Rendering it through a byte-wise accumulator produced
	// C3 83 C2 84 -- `Ã` followed by U+0084 -- which is a different relation
	// name, not a rejected one.
	multiByteIdentifier = "Ä"

	// multiByteSchema is U+00D6 LATIN CAPITAL LETTER O WITH DIAERESIS followed
	// by ASCII, UTF-8 C3 96 6E 74. It qualifies the table name so the dot
	// scanning path -- the one that owns the defect -- is actually entered.
	multiByteSchema = "Önt"

	// undecodableIdentifier is the single byte C4, which is `Ä` in Latin-1 and
	// not valid UTF-8 at all. It separates the two candidate fixes: slicing
	// the input hands this byte back untouched, while decoding to runes turns
	// it into U+FFFD (EF BF BD) and renames the object just as silently as the
	// defect being fixed here did.
	undecodableIdentifier = "\xc4"
)

// TestIdentifierFixtureBytes pins the encoding of each fixture identifier. The
// rendering assertions below compare Go strings, which is a byte comparison, so
// they are only claims about bytes as long as these constants hold the bytes
// their comments name.
func TestIdentifierFixtureBytes(t *testing.T) {
	c := qt.New(t)

	c.Assert([]byte(asciiIdentifier), qt.DeepEquals, []byte{'o', 'r', 'd', 'e', 'r', 's'})
	c.Assert([]byte(multiByteIdentifier), qt.DeepEquals, []byte{0xc3, 0x84})
	c.Assert([]byte(multiByteSchema), qt.DeepEquals, []byte{0xc3, 0x96, 'n', 't'})
	c.Assert([]byte(undecodableIdentifier), qt.DeepEquals, []byte{0xc4})
}

// TestRenderSQL_IdentifierKeepsSourceBytes renders a table whose name carries
// bytes above 0x7f and requires the rendered identifier to be those same bytes.
//
// Every dialect here quotes identifiers, so none of these names is refused or
// folded: whatever comes out names a relation, and if the bytes differ it names
// the WRONG one. Before stokaro/ptah#1352 was fixed, all three dialects widened
// each byte of the name as its own code point.
//
// One row per dialect. The only thing that varies between them is the quoting
// syntax, which each row carries as a func rather than as a branch in the body.
//
// The four name assertions are Checks, not Asserts, so restoring the defect
// reports all four verdicts for every dialect in one run. That is what makes
// the ASCII control readable: it stays green under the mutation that reddens
// the other three, which is the difference between this suite testing the
// encoding and merely testing that output echoes input.
func TestRenderSQL_IdentifierKeepsSourceBytes(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		quote   func(part string) string
	}{
		{name: "postgres", dialect: "postgres", quote: quoteWithDoubleQuotes},
		{name: "sqlite", dialect: "sqlite", quote: quoteWithDoubleQuotes},
		{name: "sqlserver", dialect: "sqlserver", quote: quoteWithBrackets},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			ascii, err := renderTableNamed(tt.dialect, asciiIdentifier)
			c.Assert(err, qt.IsNil)
			c.Check(ascii, qt.Contains, "CREATE TABLE "+tt.quote(asciiIdentifier),
				qt.Commentf("ASCII control"))

			multiByte, err := renderTableNamed(tt.dialect, multiByteIdentifier)
			c.Assert(err, qt.IsNil)
			c.Check(multiByte, qt.Contains, "CREATE TABLE "+tt.quote(multiByteIdentifier),
				qt.Commentf("multi-byte name, rendered % x", multiByte))

			qualified, err := renderTableNamed(tt.dialect, multiByteSchema+"."+multiByteIdentifier)
			c.Assert(err, qt.IsNil)
			c.Check(qualified, qt.Contains,
				"CREATE TABLE "+tt.quote(multiByteSchema)+"."+tt.quote(multiByteIdentifier),
				qt.Commentf("schema-qualified multi-byte name, rendered % x", qualified))

			undecodable, err := renderTableNamed(tt.dialect, undecodableIdentifier)
			c.Assert(err, qt.IsNil)
			c.Check(undecodable, qt.Contains, "CREATE TABLE "+tt.quote(undecodableIdentifier),
				qt.Commentf("name that is not valid UTF-8, rendered % x", undecodable))
		})
	}
}

func renderTableNamed(dialect, name string) (string, error) {
	table := ast.NewCreateTable(name).
		AddColumn(ast.NewColumn("id", "INT").SetPrimary())
	return renderer.RenderSQL(dialect, table)
}

func quoteWithDoubleQuotes(part string) string {
	return `"` + part + `"`
}

func quoteWithBrackets(part string) string {
	return "[" + part + "]"
}
