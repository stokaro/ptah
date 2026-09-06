package assist

import (
	"bufio"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"ptah.run/cmd/internal/agentflags"
	"ptah.run/cmd/internal/cmdutil"
	"ptah.run/cmd/internal/exitcode"
	"ptah.run/internal/agentaudit"
	"ptah.run/internal/aiprovider"
	"ptah.run/internal/assistloop"
	"ptah.run/internal/assistsession"
)

// Flags this command adds to the shared agent ones.
const (
	traceFlag          = "trace"
	maxToolCallsFlag   = "max-tool-calls"
	nonInteractiveFlag = "non-interactive"
)

// explainOptions is what the command takes.
type explainOptions struct {
	agent          *agentflags.Options
	session        sessionOptions
	profile        string
	model          string
	format         string
	trace          bool
	maxToolCalls   int
	nonInteractive bool
}

// newExplainCommand returns "assist explain".
func newExplainCommand() *cobra.Command {
	opts := &explainOptions{}
	cmd := &cobra.Command{
		Use:   "explain <question>",
		Short: "Ask a model about this project, with Ptah's tools answering",
		Long: `Ask a question about this project and let the model answer it using Ptah's own
tools.

One request, one answer: this is the non-interactive shape. Run "ptah assist"
with no arguments for a conversation.

The model may call tools as many times as the limits allow, and every call goes
through the same surface an external AI client reaches over the Model Context
Protocol -- the same tools, the same capability broker, the same verification
gates, and the same audit record. Ptah Assist gets nothing an external client
does not.

Without --workspace the model reaches the reading tools. With one it also
reaches the artifact tools, under the same rules ptah mcp applies: writing stays
refused until --allow-write names an artifact class, and an approval is asked
for per patch unless --auto-approve says otherwise.

The answer is the model's words. What Ptah actually did is the tool trace, which
--trace prints and --format json always carries. The two are kept apart on
purpose: a summary is not evidence, and a model that reports a check nobody ran
should be contradicted by the record rather than believed.

The conversation is saved under .ptah/sessions unless --ephemeral says not to,
and --resume continues an earlier one.

Choose the model with ptah assist provider list and test it with ptah assist
provider test.`,
		Args:          cobra.ExactArgs(1),
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExplain(cmd, opts, args[0])
		},
	}
	opts.agent = agentflags.Register(cmd)
	flags := cmd.Flags()
	flags.StringVar(&opts.profile, profileFlag, "",
		"Provider profile to use; the default profile when omitted")
	flags.StringVar(&opts.model, modelFlag, "",
		"Model identifier, overriding the profile's")
	flags.StringVar(&opts.format, formatFlag, formatText,
		"Output format: text, json, or jsonl for the record stream as it happens")
	flags.BoolVar(&opts.trace, traceFlag, false,
		"Print every tool call and what Ptah answered")
	flags.IntVar(&opts.maxToolCalls, maxToolCallsFlag, assistloop.DefaultMaxToolCalls,
		"Most tool calls one run may make")
	flags.BoolVar(&opts.nonInteractive, nonInteractiveFlag, false,
		"Never ask for approval; an operation that needs one is refused")
	registerSessionFlags(cmd, &opts.session)
	cmdutil.ConfigureCommandArgs(cmd, cobra.ExactArgs(1))
	return cmd
}

// explainReport is the machine-readable answer.
type explainReport struct {
	// Answer is the model's words.
	Answer string `json:"answer"`
	// Tools is what Ptah did. A reader comparing the two is the point.
	Tools      []assistloop.ToolRecord `json:"tools"`
	Provider   string                  `json:"provider"`
	Model      string                  `json:"model"`
	Turns      int                     `json:"turns"`
	StopReason string                  `json:"stop_reason"`
	Usage      aiprovider.Usage        `json:"usage"`
	// Verified reports whether any Ptah tool answered. An answer with no tool
	// behind it is the model talking about databases in general.
	Verified bool `json:"verified"`
	// Session is where the conversation was saved, empty when it was not.
	Session string `json:"session,omitempty"`
}

// runExplain answers one question.
func runExplain(cmd *cobra.Command, opts *explainOptions, question string) error {
	if err := validateConversationFormat(cmd, opts.format); err != nil {
		return err
	}
	provider, err := resolveProvider(cmd, opts.profile, opts.model)
	if err != nil {
		return err
	}

	session, cleanup, err := agentflags.Build(cmd, opts.agent, agentaudit.SurfaceAssist)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer cleanup()

	var approve approvalHandler
	if !opts.nonInteractive {
		approve = terminalApprover(cmd, bufio.NewReader(cmd.InOrStdin()))
	}
	tools, err := connectTools(cmd, session, approve)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer tools.Close() //nolint:errcheck // the in-memory transport has nothing to fail at

	// The record stream is a mirror of what the session file gets, so the two
	// cannot drift: one set of records, written once, to both.
	var mirror assistsession.Recorder
	if opts.format == formatJSONL {
		mirror = assistsession.NewStream(cmd.OutOrStdout(), nil)
	}
	talk, err := openConversation(opts.agent, &opts.session, provider, mirror)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer talk.recorder.Close() //nolint:errcheck // each record is written as it happens

	// The text surface streams; json and jsonl render a document and have
	// nowhere to put a fragment.
	loop, err := newLoop(provider, tools, talk, opts.maxToolCalls,
		emitter(cmd, traced(opts.trace)), answerWriter(cmd.OutOrStdout(), opts.format))
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	talk.begin(question)
	result, runErr := loop.Run(cmd.Context(), question)
	if result == nil {
		return cmdutil.Fail(cmd, runErr)
	}
	talk.finish(question, result, runErr)
	if recordErr := talk.saved(); recordErr != nil {
		// A session that could not be written is worth saying out loud and is
		// not worth losing the answer over.
		fmt.Fprintf(cmd.ErrOrStderr(), "ptah: the session was not saved: %v\n", recordErr)
	}
	if err := writeExplain(cmd, opts, result, talk); err != nil {
		return err
	}
	if runErr != nil {
		// A run that hit a limit still produced a record worth printing, so the
		// document goes out first and the outcome is the exit code.
		//
		// The reason goes to stderr as well. Without it a lost endpoint prints
		// an empty answer and an empty stop reason and exits 1 with nothing to
		// read, which is indistinguishable from a model that said nothing --
		// measured against a real endpoint that timed out mid-run.
		fmt.Fprintf(cmd.ErrOrStderr(), "ptah: %v\n", runErr)
		return exitcode.New(1, runErr)
	}
	return nil
}

// emitter prints progress to stderr, so a machine-readable answer on stdout
// stays parseable.
func emitter(cmd *cobra.Command, show traceSetting) func(assistloop.Event) {
	if !show {
		return nil
	}
	return func(event assistloop.Event) {
		fmt.Fprintf(cmd.ErrOrStderr(), "  %s: %s\n", event.Kind, event.Text)
	}
}

// writeExplain prints the answer and the record.
func writeExplain(
	cmd *cobra.Command,
	opts *explainOptions,
	result *assistloop.Result,
	talk *conversation,
) error {
	report := explainReport{
		Answer:     result.Answer,
		Tools:      result.Tools,
		Provider:   result.Provider,
		Model:      result.Model,
		Turns:      result.Turns,
		StopReason: string(result.StopReason),
		Usage:      result.Usage,
		Verified:   result.UsedTools(),
		Session:    talk.recorder.ID(),
	}
	if opts.format == formatJSON {
		return writeJSON(cmd.OutOrStdout(), report)
	}
	if opts.format == formatJSONL {
		// Every line is already on stdout, written as it happened. The summary
		// goes to stderr so that stdout stays one record per line and nothing
		// else, which is the only reason to choose this format.
		writeSessionLine(cmd.ErrOrStderr(), talk)
		return nil
	}

	out := cmd.OutOrStdout()
	writeTrace(out, traced(opts.trace).records(result.Tools))
	// The answer reached stdout as it arrived, so only the blank line that
	// separates it from the provenance is still owed.
	fmt.Fprintln(out, "")
	writeProvenance(out, result)
	writeSessionLine(out, talk)
	return nil
}

// writeSessionLine says where the conversation was kept, and how to continue it.
func writeSessionLine(out io.Writer, talk *conversation) {
	if talk.recorder.ID() == "" {
		return
	}
	fmt.Fprintf(out, "-- Session %s. Continue it with: ptah assist --resume %s\n",
		talk.recorder.ID(), talk.recorder.ID())
}
