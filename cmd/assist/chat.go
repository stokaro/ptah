package assist

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/agentflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/internal/agentaudit"
	"go.5x5.cz/ptah/internal/aiprovider"
	"go.5x5.cz/ptah/internal/assistloop"
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
  /exit      leave (Ctrl-D does the same)`

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

	// One reader for the whole surface: the approval prompt and the question
	// prompt read the same stdin, and two buffered readers would each hold a
	// partial line, so the second would consume what the first was waiting for.
	reader := bufio.NewReader(cmd.InOrStdin())
	tools, err := connectTools(cmd, session, terminalApprover(cmd, reader))
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer tools.Close() //nolint:errcheck // the in-memory transport has nothing to fail at

	talk, err := openConversation(opts.agent, &opts.session, provider, nil)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer talk.recorder.Close() //nolint:errcheck // each record is written as it happens

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

	return converse(cmd, opts, provider, tools, talk, reader)
}

// converse is the read-ask-print loop.
func converse(
	cmd *cobra.Command,
	opts *chatOptions,
	provider aiprovider.Provider,
	tools toolSession,
	talk *conversation,
	reader *bufio.Reader,
) error {
	out := cmd.OutOrStdout()
	trace := opts.trace

	for {
		fmt.Fprint(out, "> ")
		line, readErr := reader.ReadString('\n')
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

		l, loopErr := newLoop(provider, tools, talk, opts.maxToolCalls, nil)
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
	if answer := strings.TrimSpace(result.Answer); answer != "" {
		fmt.Fprintln(out, answer)
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
	out := cmd.OutOrStdout()
	listed, err := tools.ListTools(cmd.Context(), nil)
	if err != nil {
		fmt.Fprintf(out, "  the tool list could not be read: %v\n\n", err)
		return
	}
	for _, tool := range listed.Tools {
		fmt.Fprintf(out, "  %-22s %s\n", tool.Name, firstResultLine(tool.Description))
	}
	fmt.Fprintln(out, "")
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
