package banner_test

import (
	"bytes"
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/banner"
)

// wordmark is the art the three binaries share, written out here rather than
// read from the package.
//
// A literal, because the point of this assertion is that the identity does not
// move: comparing the package's logo to itself would pass whatever it became.
const wordmark = " _____  _          _\n" +
	"|  __ \\| |_   __ _| |__\n" +
	"| |__) | __| / _` | '_ \\\n" +
	"|  ___/| |_ | (_| | | | |\n" +
	"|_|     \\__| \\__,_|_| |_|\n"

// TestText_RendersTheIdentity pins the whole block, not a substring of it.
//
// The blank lines are part of the answer: the banner is written immediately
// before a command's help, and a version line running into the first line of
// that help is the failure a Contains assertion would not see.
func TestText_RendersTheIdentity(t *testing.T) {
	c := qt.New(t)

	got := banner.Text("ptah", "1.2.3")

	c.Assert(got, qt.Equals, wordmark+
		"\nDatabase schema management, without the ceremony.\n"+
		"\nptah v1.2.3\nhttps://ptah.run\n\n")
}

// TestText_NamesTheToolHappyPath is the half that differs between the three
// binaries: one wordmark, three names.
func TestText_NamesTheToolHappyPath(t *testing.T) {
	tests := []struct {
		name    string
		tool    string
		version string
		want    string
	}{
		{name: "the native binary", tool: "ptah", version: "1.2.3", want: "ptah v1.2.3"},
		{name: "the drop-in", tool: "ptah-compat", version: "1.2.3", want: "ptah-compat v1.2.3"},
		{name: "the language server", tool: "ptah-ls", version: "1.2.3", want: "ptah-ls v1.2.3"},
		{
			name: "a drop-in installed under the name it replaces",
			tool: "atlas", version: "1.2.3", want: "atlas v1.2.3",
		},
		{
			name: "a version already carrying its v",
			tool: "ptah", version: "v1.2.3", want: "ptah v1.2.3",
		},
		{
			name: "a development build with no stamp",
			tool: "ptah", version: "", want: "ptah",
		},
		{
			name: "a version that is whitespace",
			tool: "ptah", version: "  ", want: "ptah",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := banner.Text(test.tool, test.version)

			c.Assert(got, qt.Contains, "\n"+test.want+"\n"+banner.URL+"\n")
			c.Assert(got, qt.Contains, wordmark)
		})
	}
}

// TestText_CarriesNothingATerminalWouldInterpret is the requirement that the
// banner render the same in a plain terminal, a Windows console, and a
// captured log.
//
// Byte by byte rather than by eye: an escape or a box-drawing character
// pasted into the art would look right in the terminal it was pasted from and
// wrong everywhere else, which is the class of defect no visual review catches.
func TestText_CarriesNothingATerminalWouldInterpret(t *testing.T) {
	c := qt.New(t)

	got := banner.Text("ptah", "1.2.3")

	c.Assert(strings.IndexFunc(got, func(r rune) bool { return r > 0x7E }), qt.Equals, -1,
		qt.Commentf("the banner has a non-ASCII rune"))
	c.Assert(strings.ContainsRune(got, 0x1B), qt.IsFalse, qt.Commentf("the banner has an escape"))
	c.Assert(strings.ContainsRune(got, '\r'), qt.IsFalse,
		qt.Commentf("the banner has a carriage return"))
}

// TestPrintIf_WritesWhenSomebodyIsLooking is the positive half, and the reason
// the decision is a parameter.
func TestPrintIf_WritesWhenSomebodyIsLooking(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	banner.PrintIf(&out, true, "ptah", "1.2.3")

	c.Assert(out.String(), qt.Equals, banner.Text("ptah", "1.2.3"))
}

// TestPrintIf_WritesNothingWhenNobodyIs is the negative half.
func TestPrintIf_WritesNothingWhenNobodyIs(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	banner.PrintIf(&out, false, "ptah", "1.2.3")

	c.Assert(out.String(), qt.Equals, "")
}

// TestPrint_StaysOutOfCapturedOutput is the compatibility guarantee, asked of
// the function the binaries actually call.
//
// Every writer here is one a program reads: an in-process buffer is a test or
// a caller collecting output, and a pipe is a shell redirect or a language
// server's client. A banner in any of them is bytes a consumer did not ask
// for, which is what stokaro/ptah#967 established for this tree's streams.
func TestPrint_StaysOutOfCapturedOutput(t *testing.T) {
	t.Run("an in-process buffer", func(t *testing.T) {
		c := qt.New(t)
		var out bytes.Buffer

		banner.Print(&out, "ptah", "1.2.3")

		c.Assert(out.String(), qt.Equals, "")
	})

	t.Run("a pipe", func(t *testing.T) {
		c := qt.New(t)
		reader, writer, err := os.Pipe()
		c.Assert(err, qt.IsNil)
		t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })

		banner.Print(writer, "ptah", "1.2.3")
		c.Assert(writer.Close(), qt.IsNil)

		read := make([]byte, 1)
		count, _ := reader.Read(read)
		c.Assert(count, qt.Equals, 0)
	})
}

// TestWanted_ClassifiesTheWriter is the gate on its own.
//
// A file that is not a character device is the case worth naming: it is an
// *os.File, so a gate that stopped at the type assertion would pass every
// other test here and print the banner into every redirect.
func TestWanted_ClassifiesTheWriter(t *testing.T) {
	t.Run("a buffer is not a terminal", func(t *testing.T) {
		c := qt.New(t)

		c.Assert(banner.Wanted(&bytes.Buffer{}), qt.IsFalse)
	})

	t.Run("a pipe is not a terminal", func(t *testing.T) {
		c := qt.New(t)
		reader, writer, err := os.Pipe()
		c.Assert(err, qt.IsNil)
		t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })

		c.Assert(banner.Wanted(writer), qt.IsFalse)
	})

	t.Run("a regular file is not a terminal", func(t *testing.T) {
		c := qt.New(t)
		file, err := os.Create(t.TempDir() + "/redirect")
		c.Assert(err, qt.IsNil)
		t.Cleanup(func() { _ = file.Close() })

		c.Assert(banner.Wanted(file), qt.IsFalse)
	})
}
