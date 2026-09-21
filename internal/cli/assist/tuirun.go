package assist

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"

	"ptah.run/internal/aiprovider"
	"ptah.run/internal/assistloop"
)

// The pieces the interactive program needs that are not about the event loop:
// the spinner, what a directive answers, what a finished run prints, and the
// entry point that puts a real conversation behind the model.

const spinnerPeriod = 90 * time.Millisecond

// spinnerFrames is the braille cycle. It is ASCII-safe to fall back from: a
// terminal that cannot draw braille shows a box, which is ugly rather than
// wrong, and the word beside it carries the meaning.
var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

func spinnerFrame(at int) string {
	return spinnerFrames[at%len(spinnerFrames)]
}

// tuiDirective answers a `/` line: whether to leave, what to print, and the
// trace setting afterwards.
//
// It returns lines rather than printing, because in this surface everything
// finished goes to the scrollback through one queue. `runDirective` in chat.go
// writes to a Writer and is the scripted path's; the two share the set of
// directives through `interactiveDirectives` rather than through their
// rendering.
func tuiDirective(line string, trace bool, info tuiInfo) (leave bool, lines []string, showTrace bool) {
	switch strings.ToLower(strings.Fields(line)[0]) {
	case "/tools":
		return false, info.tools(), trace
	case "/session":
		return false, info.session(), trace
	case "/exit", "/quit":
		return true, nil, trace
	case "/help":
		return false, strings.Split(interactiveHelp, "\n"), trace
	case "/trace":
		trace = !trace
		return false, []string{fmt.Sprintf("  tool trace %s", shownWord[trace])}, trace
	}
	return false, []string{
		fmt.Sprintf("  %q is not a command. Try /help, or ask without the slash.", line),
	}, trace
}

// tuiInfo answers the two directives that read state the model does not hold.
// Both are closures rather than values because a tool list is fetched from the
// server when it is asked for, not cached from startup.
type tuiInfo struct {
	tools   func() []string
	session func() []string
}

// tuiReport is what a finished run leaves in the scrollback: the trace when it
// is on, then the provenance footer, in the order the scripted surface prints
// them.
func tuiReport(result *assistloop.Result, runErr error, show traceSetting) []string {
	if result == nil {
		return []string{fmt.Sprintf("  %s", runErr)}
	}

	var lines []string
	for _, record := range show.records(result.Tools) {
		lines = append(lines,
			fmt.Sprintf("  %s %s", outcomeWord[record.Failed], record.Name),
			fmt.Sprintf("      %s", firstResultLine(record.Result)),
		)
	}
	if runErr != nil {
		lines = append(lines, fmt.Sprintf("  %s", runErr))
	}
	lines = append(lines, fmt.Sprintf("-- %s via %s, %d turn(s), %d tool call(s), %s",
		result.Model, result.Provider, result.Turns, len(result.Tools), result.StopReason))
	if !result.UsedTools() {
		return append(lines,
			"-- No Ptah tool answered, so nothing above was checked against this project.")
	}
	return append(lines, fmt.Sprintf(
		"-- %d bytes of project content reached %s, from %d tool answer(s).",
		result.ToolBytes(), result.Provider, len(result.Tools)))
}

// loopSession runs one request through the model loop and records it, which is
// what `answerOne` does for the scripted surface.
type loopSession struct {
	provider     aiprovider.Provider
	tools        toolSession
	talk         *conversation
	maxToolCalls int
}

func (s *loopSession) ask(
	ctx context.Context,
	request string,
	onText func(string),
) (*assistloop.Result, error) {
	loop, err := newLoop(s.provider, s.tools, s.talk, s.maxToolCalls, nil, onText)
	if err != nil {
		return nil, err
	}
	s.talk.begin(request)
	result, runErr := loop.Run(ctx, request)
	if result != nil {
		s.talk.finish(request, result, runErr)
	}
	return result, runErr
}

// runTUI holds the conversation at a terminal.
//
// The order here is load-bearing. The model exists before the tool session,
// because the session's approval handler is the model's method; the program
// exists after both and is stored on the model before Run, because sending to
// a program that has not started blocks.
func runTUI(
	cmd *cobra.Command,
	opts *chatOptions,
	provider aiprovider.Provider,
	talk *conversation,
	connect func(approvalHandler) (toolSession, func(), error),
) error {
	model := newTUIModel(nil, opts.trace)

	tools, closeTools, err := connect(model.approve)
	if err != nil {
		return err
	}
	defer closeTools()

	model.info = tuiInfo{
		tools: func() []string { return toolLines(cmd.Context(), tools) },
		session: func() []string {
			var out strings.Builder
			writeSessionState(&out, talk)
			return splitLines(out.String())
		},
	}

	model.session = &loopSession{
		provider:     provider,
		tools:        tools,
		talk:         talk,
		maxToolCalls: opts.maxToolCalls,
	}

	// The same three lines the scripted surface opens with. They go to the
	// scrollback before the program starts, so they scroll away with the rest
	// of the conversation rather than sitting in a pane.
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Ptah Assist. %s via %s.\n", provider.Model(), provider.Profile())
	if opts.agent.Workspace == "" {
		fmt.Fprintln(out, "No workspace: the model can read declared schemas and databases you "+
			"name, and cannot change a file. Pass --workspace to add the artifact tools.")
	}
	if opts.session.resume != "" {
		fmt.Fprintf(out, "Continuing %s: %d earlier message(s).\n",
			opts.session.resume, len(talk.history))
	}
	talk.announce(out)
	fmt.Fprintf(out, "Ask a question, or /help. Ctrl-J for a new line.\n\n")

	program := tea.NewProgram(model,
		tea.WithContext(cmd.Context()),
		tea.WithInput(cmd.InOrStdin()),
		tea.WithOutput(cmd.OutOrStdout()),
	)
	model.program.Store(program)

	_, runErr := program.Run()
	// Every exit path releases the worker's context. A goroutine blocked on an
	// approval nobody will answer is the one shape that leaks for good.
	if model.cancel != nil {
		model.cancel()
	}
	return runErr
}

// splitLines turns a writer's block into the lines the scrollback queue takes,
// dropping the trailing empty one a block always ends with.
func splitLines(block string) []string {
	return strings.Split(strings.TrimRight(block, "\n"), "\n")
}

// toolLines renders the tool list the same way the scripted surface does, from
// the server rather than from a list in this file.
func toolLines(ctx context.Context, tools toolSession) []string {
	var out strings.Builder
	writeToolCatalog(ctx, &out, tools)
	return splitLines(out.String())
}
