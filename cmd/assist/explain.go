package assist

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/agentflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/agentaudit"
	"go.5x5.cz/ptah/internal/aiprovider"
	"go.5x5.cz/ptah/internal/assistloop"
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
		"Output format: text or json")
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
	if err := validateFormat(cmd, opts.format); err != nil {
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

	talk, err := openConversation(opts.agent, &opts.session, provider)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer talk.recorder.Close() //nolint:errcheck // each record is written as it happens

	loop, err := newLoop(provider, tools, talk.history, opts.maxToolCalls, emitter(cmd, traced(opts.trace)))
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	result, runErr := loop.Run(cmd.Context(), question)
	if result == nil {
		return cmdutil.Fail(cmd, runErr)
	}
	if recordErr := talk.record(question, result); recordErr != nil {
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

	out := cmd.OutOrStdout()
	writeTrace(out, traced(opts.trace).records(result.Tools))
	fmt.Fprintln(out, strings.TrimSpace(result.Answer))
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
