//go:build !js

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
	"ptah.run/internal/cli/banner"
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
		return false, dim(info.tools()), trace
	case "/session":
		return false, dim(info.session()), trace
	case "/exit", "/quit":
		return true, nil, trace
	case "/help":
		return false, strings.Split(interactiveHelp, "\n"), trace
	case "/trace":
		trace = !trace
		// What it is, not only that it changed. `tool trace on` says nothing to
		// a reader who has not met the term, and the trace is the one part of
		// the surface that answers "did the model check this, or say it".
		lines := []string{noticeStyle.Render(fmt.Sprintf("  tool trace %s", shownWord[trace]))}
		if trace {
			lines = append(lines, hintStyle.Render(
				"  every Ptah tool the model calls, and what Ptah answered"))
		}
		return false, lines, trace
	}
	return false, []string{noticeStyle.Render(
		fmt.Sprintf("  %q is not a command. Try /help, or ask without the slash.", line),
	)}, trace
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
		return []string{noticeStyle.Render(fmt.Sprintf("  %s", runErr))}
	}

	var lines []string
	for _, record := range show.records(result.Tools) {
		style := traceOKStyle
		if record.Failed {
			style = traceFailStyle
		}
		lines = append(lines,
			style.Render(fmt.Sprintf("  %s %s", outcomeWord[record.Failed], record.Name)),
			footerStyle.Render(fmt.Sprintf("      %s", traceResultLine(record.Result, 92))),
		)
	}
	if runErr != nil {
		lines = append(lines, noticeStyle.Render(fmt.Sprintf("  %s", runErr)))
	}
	lines = append(lines, footerStyle.Render(fmt.Sprintf(
		"-- %s via %s, %d turn(s), %d tool call(s), %s",
		result.Model, result.Provider, result.Turns, len(result.Tools), result.StopReason)))
	if !result.UsedTools() {
		// The one footer line that is a warning rather than a fact.
		return append(lines, unverifiedStyle.Render(
			"-- No Ptah tool answered, so nothing above was checked against this project."))
	}
	return append(lines, footerStyle.Render(fmt.Sprintf(
		"-- %d bytes of project content reached %s, from %d tool answer(s).",
		result.ToolBytes(), result.Provider, len(result.Tools))))
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

	// What the session opens on. It goes to the scrollback before the program
	// starts, so it scrolls away with the rest of the conversation rather than
	// sitting in a pane.
	//
	// The wordmark and the model are what a reader needs here. Everything under
	// them is a path or a housekeeping notice: true, worth having, and not what
	// anyone came to read, so it is drawn in the muted color the footers use.
	// Undifferentiated, the block was six lines of full-strength text and the
	// screen read as noise.
	out := cmd.OutOrStdout()
	heading := strings.SplitN(banner.Heading("Interactive Assistant"), "\n\n", 2)
	fmt.Fprint(out, logoStyle.Render(heading[0]), "\n\n")
	if len(heading) > 1 {
		fmt.Fprint(out, subtitleStyle.Render(strings.TrimRight(heading[1], "\n")), "\n\n")
	}
	fmt.Fprintf(out, "%s\n\n", footerStyle.Render(
		fmt.Sprintf("%s via %s", provider.Model(), provider.Profile())))

	var aside strings.Builder
	if opts.agent.Workspace == "" {
		fmt.Fprintln(&aside, "No workspace: the model can read declared schemas and databases you "+
			"name, and cannot change a file. Pass --workspace to add the artifact tools.")
	}
	if opts.session.resume != "" {
		fmt.Fprintf(&aside, "Continuing %s: %d earlier message(s).\n",
			opts.session.resume, len(talk.history))
	}
	talk.announce(&aside)
	fmt.Fprintln(&aside, "Ask a question, or /help. Ctrl-J for a new line.")
	fmt.Fprintf(out, "%s\n\n", hintStyle.Render(strings.TrimRight(aside.String(), "\n")))

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
