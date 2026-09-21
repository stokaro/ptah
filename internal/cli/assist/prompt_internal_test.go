package assist

// White-box testing required: the interactive surface reads its line through
// `prompter`, and neither the interface nor the two implementations are
// exported. What matters here cannot be reached from outside the package
// either: which implementation `newPrompter` picks is the seam that keeps
// every scripted run and every test in this package working, and the
// completion callback is a decision made per keystroke that no black-box run
// can drive. The terminal half needs a terminal and is checked by driving the
// built binary under a pseudo-terminal instead.

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestNewPrompterPicksThePlainReaderWithoutATerminal is the seam the whole
// change rests on. Every test in this package drives the surface through a
// strings.Reader, and CI drives it through a pipe; if either got the terminal
// implementation it would try to put a pipe into raw mode.
func TestNewPrompterPicksThePlainReaderWithoutATerminal(t *testing.T) {
	tests := []struct {
		name string
		// Built per case rather than held in the row, because the cases that
		// discriminate need real operating-system files and a row cannot own
		// their cleanup.
		open func(c *qt.C) (io.Reader, io.Writer)
	}{
		{name: "string reader", open: func(*qt.C) (io.Reader, io.Writer) {
			return strings.NewReader("hi\n"), &strings.Builder{}
		}},
		{name: "os.Stdin with a non-file writer", open: func(*qt.C) (io.Reader, io.Writer) {
			return os.Stdin, &strings.Builder{}
		}},
		// The rows that matter. Both sides are *os.File here, so the type
		// assertions pass and only the terminal check can refuse: this is a
		// redirected run, which is what CI and `go test` actually are.
		// Without these the tty check can be deleted and this test stays green.
		{name: "both sides are pipes", open: openPipes},
		{name: "both sides are files on disk", open: openFiles},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			in, out := test.open(c)

			input := newPrompter(in, out)

			_, plain := input.(*plainPrompter)
			c.Assert(plain, qt.IsTrue)
			c.Assert(input.close(), qt.IsNil)
		})
	}
}

// openPipes gives a reader and a writer that are both *os.File and neither a
// terminal.
func openPipes(c *qt.C) (io.Reader, io.Writer) {
	c.Helper()

	readEnd, writeEnd, err := os.Pipe()
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_ = readEnd.Close()
		_ = writeEnd.Close()
	})
	return readEnd, writeEnd
}

// openFiles is the other redirected shape: stdin from a file, stdout to one.
func openFiles(c *qt.C) (io.Reader, io.Writer) {
	c.Helper()

	dir := c.TempDir()
	in, err := os.Create(filepath.Join(dir, "in"))
	c.Assert(err, qt.IsNil)
	out, err := os.Create(filepath.Join(dir, "out"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_ = in.Close()
		_ = out.Close()
	})
	return in, out
}

// TestPlainPrompterReadsOneLine_HappyPath pins what the scripted path returns:
// the line without its terminator, whichever terminator was used.
func TestPlainPrompterReadsOneLine_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "unix newline", input: "what tables are there?\n", want: "what tables are there?"},
		{name: "carriage return pair", input: "a question\r\n", want: "a question"},
		{name: "directive", input: "/exit\n", want: "/exit"},
		{name: "empty line", input: "\n", want: ""},
		{name: "inner spacing is kept", input: "  spaced  out  \n", want: "  spaced  out  "},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out := &strings.Builder{}
			input := newPrompter(strings.NewReader(test.input), out)

			line, err := input.ask("> ")

			c.Assert(err, qt.IsNil)
			c.Assert(line, qt.Equals, test.want)
			c.Assert(out.String(), qt.Equals, "> ")
		})
	}
}

// TestPlainPrompterReadsOneLine_FailurePath covers the end of input. A last
// line with no newline on it is returned with the error rather than dropped,
// which is how the loop tells "they typed something and closed stdin" from
// "they closed stdin".
func TestPlainPrompterReadsOneLine_FailurePath(t *testing.T) {
	t.Run("end of input with a partial line", func(t *testing.T) {
		c := qt.New(t)
		input := newPrompter(strings.NewReader("no newline"), &strings.Builder{})

		line, err := input.ask("> ")

		c.Assert(err, qt.ErrorIs, io.EOF)
		c.Assert(line, qt.Equals, "no newline")
	})

	t.Run("end of input with nothing typed", func(t *testing.T) {
		c := qt.New(t)
		input := newPrompter(strings.NewReader(""), &strings.Builder{})

		line, err := input.ask("> ")

		c.Assert(err, qt.ErrorIs, io.EOF)
		c.Assert(line, qt.Equals, "")
	})
}

// TestPlainPrompterSharesOneReader is the property the surface depends on and
// the reason `prompter` exists at all: the approval prompt and the question
// prompt read the same stdin, so a second buffered reader would hold the rest
// of the first one's buffer and the question after an approval would go
// missing.
func TestPlainPrompterSharesOneReader(t *testing.T) {
	t.Run("an approval and the question after it", func(t *testing.T) {
		c := qt.New(t)
		input := newPrompter(strings.NewReader("o\nwhat next?\n/exit\n"), &strings.Builder{})

		approval, approvalErr := input.confirm("Allow? ")
		question, questionErr := input.ask("> ")
		directive, directiveErr := input.ask("> ")

		c.Assert(approvalErr, qt.IsNil)
		c.Assert(questionErr, qt.IsNil)
		c.Assert(directiveErr, qt.IsNil)
		c.Assert(approval, qt.Equals, "o")
		c.Assert(question, qt.Equals, "what next?")
		c.Assert(directive, qt.Equals, "/exit")
	})
}

// TestDirectivesWithPrefix pins the completion set, including that a prefix
// matching more than one directive returns them all: the caller completes only
// on a single match, and a set that silently collapsed to one would complete
// `/e` to whichever happened to sort first.
func TestDirectivesWithPrefix(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		want   []string
	}{
		{name: "one match", prefix: "/he", want: []string{"/help"}},
		{name: "another single match", prefix: "/tr", want: []string{"/trace"}},
		{name: "two matches", prefix: "/t", want: []string{"/tools", "/trace"}},
		{name: "every directive", prefix: "/", want: []string{
			"/exit", "/help", "/quit", "/session", "/tools", "/trace",
		}},
		{name: "no match", prefix: "/zz", want: nil},
		{name: "not a directive", prefix: "what", want: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(directivesWithPrefix(test.prefix), qt.DeepEquals, test.want)
		})
	}
}

// TestCompleteAnswersOnlyTabOnASingleDirective covers the callback's decision.
// It is asked on every keystroke, so declining is the common answer and the
// expensive mistake is completing when it should not: rewriting the line under
// someone who was typing prose.
func TestCompleteAnswersOnlyTabOnASingleDirective(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		pos     int
		key     rune
		wantOK  bool
		newLine string
		newPos  int
	}{
		{name: "tab on one match completes", line: "/he", pos: 3, key: '\t',
			wantOK: true, newLine: "/help ", newPos: 6},
		{name: "tab on an ambiguous prefix declines", line: "/t", pos: 2, key: '\t'},
		{name: "tab on prose declines", line: "what tables", pos: 11, key: '\t'},
		{name: "a letter declines", line: "/he", pos: 3, key: 'l'},
		{name: "tab mid-line declines", line: "/help", pos: 2, key: '\t'},
		{name: "tab on an unknown directive declines", line: "/zz", pos: 3, key: '\t'},
		{name: "tab on an empty line declines", line: "", pos: 0, key: '\t'},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			p := &termPrompter{}

			line, pos, ok := p.complete(test.line, test.pos, test.key)

			c.Assert(ok, qt.Equals, test.wantOK)
			c.Assert(line, qt.Equals, test.newLine)
			c.Assert(pos, qt.Equals, test.newPos)
		})
	}
}

// TestRememberSkipsRepeatsAndBlanks keeps the history walkable. Asking the same
// question twice should not mean pressing Up twice to get past it.
func TestRememberSkipsRepeatsAndBlanks(t *testing.T) {
	t.Run("a repeated line is recorded once", func(t *testing.T) {
		c := qt.New(t)
		p := newTermPrompter(os.Stdin, os.Stdout)

		p.remember("first")
		p.remember("first")
		p.remember("second")
		p.remember("first")

		c.Assert(p.history, qt.DeepEquals, []string{"first", "second", "first"})
	})
}
