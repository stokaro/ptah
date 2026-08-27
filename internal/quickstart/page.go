package quickstart

import "fmt"

// Shell names one of the two shells a quick-start page can be written for.
//
// The fence language is what selects it, so the page and the runner cannot
// disagree about which shell a block was written in.
type Shell string

const (
	// Bash is the shell a ```bash block is run in, on Linux and macOS.
	Bash Shell = "bash"
	// PowerShell is the shell a ```powershell block is run in, on Windows.
	PowerShell Shell = "powershell"
)

// Shells lists every shell a page may be written for, in the order the runner
// reports them.
func Shells() []Shell { return []Shell{Bash, PowerShell} }

// Stream names the output stream an expectation is asserted against.
//
// The page states it in the sentence that introduces the block, so the reader
// and the runner read the same claim. A block that names neither is refused.
type Stream string

const (
	// Stdout is the stream selected by "on standard output:".
	Stdout Stream = "standard output"
	// Stderr is the stream selected by "on standard error:".
	Stderr Stream = "standard error"
)

// ActionKind distinguishes a command the reader runs from a file the reader
// writes.
type ActionKind string

const (
	// ActionStep is a ```bash or ```powershell block: something to run.
	ActionStep ActionKind = "step"
	// ActionFile is an ```sql block whose introduction names a path: something
	// to write to disk before the next step runs.
	ActionFile ActionKind = "file"
)

// Expectation is one output block, bound to the step above it and to the
// stream its introduction names.
type Expectation struct {
	// Line is the 1-indexed line of the block's opening fence.
	Line int
	// Stream is the stream the introducing sentence selected.
	Stream Stream
	// Lines are the expected lines, in order. They are asserted by
	// containment, in order, because the page's own word is "includes".
	Lines []string
}

// Action is one thing the page tells the reader to do, in page order.
type Action struct {
	// Kind is what the reader does with it.
	Kind ActionKind
	// Line is the 1-indexed line of the block's opening fence.
	Line int
	// Path is the file to write, for ActionFile.
	Path string
	// Body is the block's content: the script for ActionStep, the file
	// contents for ActionFile.
	Body string
	// Expectations are the output blocks bound to this step. Always empty for
	// ActionFile.
	Expectations []Expectation
	// Number is the step's 1-based position among the steps of its program.
	// Zero for ActionFile. It is what a failure report names, and what the
	// generated script's sentinels count.
	Number int
}

// Program is everything one page asks one shell to do, in page order.
type Program struct {
	// Shell is the shell the steps are written for.
	Shell Shell
	// Actions are the file writes and steps, interleaved as the page has them.
	Actions []Action
}

// Steps returns the number of commands the program runs.
func (p *Program) Steps() int {
	count := 0
	for _, action := range p.Actions {
		if action.Kind == ActionStep {
			count++
		}
	}
	return count
}

// Expectations returns the number of output blocks the program asserts.
func (p *Program) Expectations() int {
	count := 0
	for _, action := range p.Actions {
		count += len(action.Expectations)
	}
	return count
}

// Page is one opted-in documentation page and the programs it publishes.
type Page struct {
	// Path is the page's path as the caller named it, and as a failure reports
	// it.
	Path string
	// Title is the frontmatter title, for reporting.
	Title string
	// Programs holds one entry per shell the page has steps for.
	Programs map[Shell]*Program
}

// Program returns the page's program for one shell, and whether it has one.
func (p *Page) Program(shell Shell) (*Program, bool) {
	program, ok := p.Programs[shell]
	return program, ok
}

// ShellsPresent returns the shells the page carries steps for, in Shells order.
func (p *Page) ShellsPresent() []Shell {
	present := make([]Shell, 0, len(p.Programs))
	for _, shell := range Shells() {
		if _, ok := p.Programs[shell]; ok {
			present = append(present, shell)
		}
	}
	return present
}

// ExtractError is a page shape the runner refuses, with the line that carries
// it.
//
// Every one of these is a shape that would otherwise run nothing and report
// nothing: an assertion nobody can attribute to a stream, a file nobody writes,
// a step in a tab for the other operating system.
type ExtractError struct {
	// Path is the page the problem is on.
	Path string
	// Line is the 1-indexed line to look at.
	Line int
	// Problem is what is wrong, in one sentence.
	Problem string
}

func (e *ExtractError) Error() string {
	return fmt.Sprintf("%s:%d: %s", e.Path, e.Line, e.Problem)
}
