package assist

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/internal/agentaudit"
	"ptah.run/internal/aiprovider"
	"ptah.run/internal/assistloop"
	"ptah.run/internal/cli/internal/agentflags"
	"ptah.run/internal/cli/internal/cmdutil"
)

// chatOptions is what the interactive surface takes.
type chatOptions struct {
	agent        *agentflags.Options
	session      sessionOptions
	profile      string
	model        string
	trace        bool
	maxToolCalls int
}

// interactiveHelp is what `/help` prints.
//
// Slash-prefixed, so a question that begins with one of these words is still a
// question. A bare `tools` would be ambiguous between asking about tools and
// listing them, and the ambiguity would be resolved differently by every
// person who met it.
const interactiveHelp = `  /tools     the Ptah tools this session can reach
  /session   where this conversation is being saved
  /trace     show or hide the tool trace
  /help      this list
  /exit      leave (Ctrl-D does the same)

At a terminal the line is edited: Up and Down walk the questions you have
already asked, Left and Right move the cursor, Alt with them moves by word,
Home and End jump, Ctrl-W deletes a word and Ctrl-U the line, and Tab
completes a directive. A pasted block arrives as one question.`

// registerChatFlags adds the interactive surface's flags to the shared agent
// ones.
func registerChatFlags(cmd *cobra.Command, opts *chatOptions) {
	opts.agent = agentflags.Register(cmd)
	flags := cmd.Flags()
	flags.StringVar(&opts.profile, profileFlag, "",
		"Provider profile to use; the default profile when omitted")
	flags.StringVar(&opts.model, modelFlag, "",
		"Model identifier, overriding the profile's")
	flags.BoolVar(&opts.trace, traceFlag, false,
		"Show every tool call and what Ptah answered")
	flags.IntVar(&opts.maxToolCalls, maxToolCallsFlag, assistloop.DefaultMaxToolCalls,
		"Most tool calls one request may make")
	registerSessionFlags(cmd, &opts.session)
}

// runChat holds a conversation until the person leaves.
func runChat(cmd *cobra.Command, opts *chatOptions) error {
	provider, err := resolveProvider(cmd, opts.profile, opts.model)
	if err != nil {
		return err
	}
	session, cleanup, err := agentflags.Build(cmd, opts.agent, agentaudit.SurfaceAssist)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer cleanup()

	talk, err := openConversation(opts.agent, &opts.session, provider, nil)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer talk.recorder.Close() //nolint:errcheck // each record is written as it happens

	// At a terminal the conversation runs as an inline Bubble Tea program; a
	// pipe, a file or a test keeps the read-ask-print loop below, byte for
	// byte. The decision is `atTerminal`, asked once, so the two surfaces
	// never both hold stdin. The tool session is opened inside runTUI because
	// its approval handler is a method on the program's model.
	if atTerminal(cmd.InOrStdin(), cmd.OutOrStdout()) {
		return runTUI(cmd, opts, provider, talk, func(approve approvalHandler) (toolSession, func(), error) {
			connected, connectErr := connectTools(cmd, session, approve)
			if connectErr != nil {
				return nil, nil, cmdutil.Fail(cmd, connectErr)
			}
			return connected, func() { _ = connected.Close() }, nil
		})
	}

	// One prompter for the whole surface: the approval prompt and the question
	// prompt read the same stdin, and two readers would each hold a partial
	// line, so the second would consume what the first was waiting for. See
	// prompt.go.
	input := newPrompter(cmd.InOrStdin(), cmd.OutOrStdout())
	defer input.close() //nolint:errcheck // restoring a terminal that was never raw cannot fail
	tools, err := connectTools(cmd, session, terminalApprover(cmd, input))
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer tools.Close() //nolint:errcheck // the in-memory transport has nothing to fail at

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
	fmt.Fprintf(out, "Ask a question, or /help.\n\n")

	return converse(cmd, opts, provider, tools, talk, input)
}

// converse is the read-ask-print loop.
func converse(
	cmd *cobra.Command,
	opts *chatOptions,
	provider aiprovider.Provider,
	tools toolSession,
	talk *conversation,
	input prompter,
) error {
	out := cmd.OutOrStdout()
	trace := opts.trace

	for {
		// The prompt is the prompter's, not a Fprint before it: the line
		// editor has to redraw it whenever the line is re-wrapped, so it has
		// to own it.
		line, readErr := input.ask("> ")
		request := strings.TrimSpace(line)

		if request == "" {
			if readErr != nil {
				fmt.Fprintln(out, "")
				return nil
			}
			continue
		}
		if strings.HasPrefix(request, "/") {
			leave, showTrace := runDirective(cmd, request, trace, tools, talk)
			trace = showTrace
			if leave {
				return nil
			}
			if readErr != nil {
				return nil
			}
			continue
		}

		l, loopErr := newLoop(provider, tools, talk, opts.maxToolCalls, nil, streamTo(out))
		if loopErr != nil {
			return cmdutil.Fail(cmd, loopErr)
		}
		answerOne(cmd, l, talk, request, traced(trace))
		if readErr != nil {
			return nil
		}
	}
}

// answerOne runs a single request and prints what happened.
//
// A failure ends the request rather than the conversation: a rate limit, a
// stale digest or a run that hit its limit is something the person can respond
// to by asking again, and dropping them back to a shell would lose the session
// they are in the middle of.
func answerOne(
	cmd *cobra.Command,
	loop *assistloop.Loop,
	talk *conversation,
	request string,
	show traceSetting,
) {
	out := cmd.OutOrStdout()
	talk.begin(request)
	result, runErr := loop.Run(cmd.Context(), request)
	if result == nil {
		fmt.Fprintf(out, "  %s\n\n", runErr)
		return
	}
	talk.finish(request, result, runErr)
	if recordErr := talk.saved(); recordErr != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "ptah: the session was not saved: %v\n", recordErr)
	}

	writeTrace(out, show.records(result.Tools))
	// The answer was written as it arrived, so what is left is the newline
	// that ends it. Printing it again here is the duplication a streamed
	// surface has to avoid.
	if strings.TrimSpace(result.Answer) != "" {
		fmt.Fprintln(out, "")
	}
	if runErr != nil {
		fmt.Fprintf(out, "  %s\n", runErr)
	}
	fmt.Fprintln(out, "")
	writeProvenance(out, result)
	fmt.Fprintln(out, "")
}

// traceSetting is whether the tool trace is shown, as a value the printer
// consults rather than a flag the printer branches on.
type traceSetting bool

// records is the trace to print: the run's records, or none.
func (t traceSetting) records(all []assistloop.ToolRecord) []assistloop.ToolRecord {
	if !t {
		return nil
	}
	return all
}

// traced names the setting at the call site.
func traced(on bool) traceSetting { return traceSetting(on) }

// runDirective handles a `/` line, and reports whether to leave and what the
// trace setting is now.
func runDirective(
	cmd *cobra.Command,
	line string,
	trace bool,
	tools toolSession,
	talk *conversation,
) (leave, showTrace bool) {
	out := cmd.OutOrStdout()
	switch strings.ToLower(strings.Fields(line)[0]) {
	case "/exit", "/quit":
		return true, trace
	case "/help":
		fmt.Fprintf(out, "%s\n\n", interactiveHelp)
	case "/tools":
		writeToolList(cmd, tools)
	case "/session":
		writeSessionState(out, talk)
	case "/trace":
		trace = !trace
		fmt.Fprintf(out, "  tool trace %s\n\n", shownWord[trace])
	default:
		fmt.Fprintf(out, "  %q is not a command. Try /help, or ask without the slash.\n\n", line)
	}
	return false, trace
}

// shownWord renders the trace toggle.
var shownWord = map[bool]string{true: "on", false: "off"}

// writeToolList prints what this session can reach, from the server rather than
// from a list in this file.
func writeToolList(cmd *cobra.Command, tools toolSession) {
	writeToolCatalog(cmd.Context(), cmd.OutOrStdout(), tools)
	fmt.Fprintln(cmd.OutOrStdout(), "")
}

// writeToolCatalog renders the list itself, from the server rather than from a
// list in this file. It takes a writer and a context so both surfaces can use
// it: the scripted one prints it, and the interactive one queues it for the
// scrollback.
func writeToolCatalog(ctx context.Context, out writer, tools toolSession) {
	listed, err := tools.ListTools(ctx, nil)
	if err != nil {
		fmt.Fprintf(out, "  the tool list could not be read: %v\n", err)
		return
	}
	for _, tool := range listed.Tools {
		fmt.Fprintf(out, "  %-22s %s\n", tool.Name, firstResultLine(tool.Description))
	}
}

// writeSessionState says where the conversation is being kept.
func writeSessionState(out writer, talk *conversation) {
	if talk.recorder.ID() == "" {
		fmt.Fprintln(out, "  This conversation is ephemeral: nothing is being written.")
		fmt.Fprintln(out, "")
		return
	}
	fmt.Fprintf(out, "  Session %s\n", talk.recorder.ID())
	fmt.Fprintf(out, "  %s\n", talk.recorder.Path())
	fmt.Fprintf(out, "  Continue it later with: ptah assist --resume %s\n\n", talk.recorder.ID())
}
