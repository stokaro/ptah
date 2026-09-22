package assist

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"golang.org/x/term"
)

// The interactive surface reads a line two ways, and which one it gets depends
// on what is on the other end of stdin.
//
// At a real terminal the line is edited: arrow keys move the cursor, Up walks
// the history, Alt-arrows move by word, Home and End jump, Ctrl-W and Ctrl-U
// delete, Tab completes a directive, and a multi-line paste arrives as one
// line instead of being run a line at a time. Reading with bufio gives none of
// that. The terminal driver is in cooked mode, so it knows about Backspace and
// nothing else: an arrow key reaches the program as the three bytes the
// terminal sent, and `ESC [ D` ends up inside the question. That was the
// behaviour this file replaces -- typing `hello` and pressing Left left
// `hello^[[D` on screen and sent it to the model.
//
// Everywhere else -- a pipe, a file, a test, CI -- stdin is not a terminal,
// raw mode is neither available nor wanted, and the bufio path stays exactly
// as it was. That seam is what keeps the surface scriptable, and every
// existing test drives it.

// prompter reads one line from whoever is answering.
//
// It is an interface because there are two implementations and the surface
// must not know which it has: the approval prompt and the question prompt go
// through the same one, for the reason the plain implementation documents.
type prompter interface {
	// ask reads a question, and remembers it so Up walks back to it.
	//
	// It returns io.EOF when the person left -- end of input, Ctrl-D, or
	// Ctrl-C. A line already typed when that happens is returned with the
	// error, the way bufio reports a last line with no newline on it.
	ask(prompt string) (string, error)

	// confirm reads an answer to a prompt the program asked, and does not
	// remember it. Walking Up through "o", "s" and "n" to reach the last real
	// question is not a history worth having, which is the whole reason this
	// is a second method rather than a boolean on the first.
	confirm(prompt string) (string, error)

	// close puts the terminal back the way it was found. Safe to call twice,
	// and safe to call on a prompter that never changed anything.
	close() error
}

// newPrompter picks the implementation from what stdin actually is.
//
// The check is on the file descriptor rather than on a flag: a person who
// pipes a here-document into `ptah assist` wants the scripted behaviour
// without saying so, and a person at a keyboard wants the editor without
// saying so either. Both halves of the terminal are required -- reading in raw
// mode while output goes somewhere else leaves the echo on a screen nobody is
// looking at.
func newPrompter(in io.Reader, out io.Writer) prompter {
	if !atTerminal(in, out) {
		return &plainPrompter{reader: bufio.NewReader(in), out: out}
	}
	return newTermPrompter(in.(*os.File), out.(*os.File))
}

// atTerminal reports whether both halves are a terminal.
//
// Both, deliberately: reading in raw mode while output goes somewhere else
// leaves the echo on a screen nobody is looking at. It is the one decision
// that separates the interactive surface from the scripted one, so it is
// written once and both callers ask it.
func atTerminal(in io.Reader, out io.Writer) bool {
	inFile, inOK := in.(*os.File)
	outFile, outOK := out.(*os.File)
	return inOK && outOK &&
		term.IsTerminal(int(inFile.Fd())) && term.IsTerminal(int(outFile.Fd()))
}

// plainPrompter is the scripted path: one buffered reader, as before.
//
// One reader for the whole surface, deliberately. The approval prompt and the
// question prompt read the same stdin, and two buffered readers would each
// hold a partial line, so the second would consume what the first was waiting
// for.
type plainPrompter struct {
	reader *bufio.Reader
	out    io.Writer
}

func (p *plainPrompter) ask(prompt string) (string, error) { return p.read(prompt) }

func (p *plainPrompter) confirm(prompt string) (string, error) { return p.read(prompt) }

// read is both of them: with no line editor there is no history to keep out of.
func (p *plainPrompter) read(prompt string) (string, error) {
	fmt.Fprint(p.out, prompt)
	line, err := p.reader.ReadString('\n')
	return strings.TrimRight(line, "\r\n"), err
}

func (p *plainPrompter) close() error { return nil }

// termPrompter is the interactive path, on golang.org/x/term.
//
// Raw mode is entered for the duration of one line and left immediately
// afterwards, rather than held for the session. Everything else this surface
// prints -- the model's answer as it streams, the tool trace, an error -- is
// then written to an ordinary terminal and needs no newline translation. Held
// across the whole session, every one of those writers would have to go
// through the Terminal to avoid a staircase, and the one that was forgotten
// would be the one nobody tested.
type termPrompter struct {
	in       *os.File
	out      *os.File
	terminal *term.Terminal
	// history is kept here rather than left to the Terminal so that an answer
	// at an approval prompt does not land in the question history. Walking Up
	// through "o", "s", "n" to reach the last real question is not history.
	history []string
}

func newTermPrompter(in, out *os.File) *termPrompter {
	p := &termPrompter{in: in, out: out}
	p.terminal = term.NewTerminal(struct {
		io.Reader
		io.Writer
	}{in, out}, "")
	p.terminal.AutoCompleteCallback = p.complete
	return p
}

// ask edits one line and keeps it for the Up key.
//
// Recording happens here rather than inside `edit`, so that which lines are
// remembered is decided by the method a caller chose and is readable at the
// call site. A blank line is not recorded: pressing Enter on an empty prompt
// is not a question, and it would put a gap in the history to walk past.
func (p *termPrompter) ask(prompt string) (string, error) {
	line, err := p.edit(prompt)
	if strings.TrimSpace(line) != "" {
		p.remember(line)
	}
	return line, err
}

// confirm edits one line and forgets it.
func (p *termPrompter) confirm(prompt string) (string, error) {
	return p.edit(prompt)
}

// edit reads one line through the line editor.
//
// The raw-mode window is as small as it can be: entered here, left before
// returning, whatever happens in between. A caller that returns an error while
// the terminal is raw leaves a shell with no echo, which is the kind of damage
// a program is not forgiven for.
func (p *termPrompter) edit(prompt string) (string, error) {
	state, err := term.MakeRaw(int(p.in.Fd()))
	if err != nil {
		// No raw mode: fall back rather than fail. A terminal that refuses it
		// still reads lines, just without the editing.
		fmt.Fprint(p.out, prompt)
		line, readErr := bufio.NewReader(p.in).ReadString('\n')
		return strings.TrimRight(line, "\r\n"), readErr
	}
	defer func() { _ = term.Restore(int(p.in.Fd()), state) }()

	// Bracketed paste is what makes a pasted block one question. Without it
	// the terminal sends the newlines inside the paste as Enter, and a
	// three-line paste asks three questions, the first two of them fragments.
	// It is turned off again below, because a terminal left in that mode
	// confuses the shell this program returns to.
	p.terminal.SetBracketedPasteMode(true)
	defer p.terminal.SetBracketedPasteMode(false)

	p.terminal.SetPrompt(prompt)
	line, err := p.terminal.ReadLine()
	return strings.TrimRight(line, "\r\n"), err
}

// remember appends to the history the Up key walks, newest last, without
// repeating the line that is already on top.
func (p *termPrompter) remember(line string) {
	if len(p.history) > 0 && p.history[len(p.history)-1] == line {
		return
	}
	p.history = append(p.history, line)
	p.terminal.History.Add(line)
}

func (p *termPrompter) close() error {
	return nil
}

// complete finishes a directive on Tab.
//
// Only directives, and only when the line is one: a question is prose, and
// there is nothing to complete in prose that would not be a guess. The
// callback is asked on every key, so anything that is not Tab is declined at
// once and the Terminal handles the key itself.
func (p *termPrompter) complete(line string, pos int, key rune) (string, int, bool) {
	if key != '\t' || !strings.HasPrefix(line, "/") {
		return "", 0, false
	}
	// Completing from the middle of a line would have to decide what to do
	// with the tail, and every answer to that is a surprise.
	if pos != len(line) {
		return "", 0, false
	}
	matches := directivesWithPrefix(line)
	if len(matches) != 1 {
		return "", 0, false
	}
	completed := matches[0] + " "
	return completed, len(completed), true
}

// interactiveDirectives is every directive the loop answers, including the
// spellings runDirective accepts as synonyms, so Tab completes what the loop
// will actually run rather than what the help text lists.
var interactiveDirectives = []string{"/exit", "/help", "/quit", "/session", "/tools", "/trace"}

// directivesWithPrefix is the completion set for one partial directive, in a
// stable order so the single-match test means the same thing on every run.
func directivesWithPrefix(prefix string) []string {
	var matches []string
	for _, directive := range interactiveDirectives {
		if strings.HasPrefix(directive, prefix) {
			matches = append(matches, directive)
		}
	}
	sort.Strings(matches)
	return matches
}
