package assist

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/agentflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/agentapi"
	"go.5x5.cz/ptah/internal/agentaudit"
	"go.5x5.cz/ptah/internal/aiprovider"
	"go.5x5.cz/ptah/internal/assistconfig"
	"go.5x5.cz/ptah/internal/assistloop"
	"go.5x5.cz/ptah/internal/buildinfo"
	"go.5x5.cz/ptah/internal/mcpserver"
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

One request, one answer: this is the non-interactive shape. The model may call
tools as many times as the limits allow, and every call goes through the same
surface an external AI client reaches over the Model Context Protocol -- the
same tools, the same capability broker, the same verification gates, and the
same audit record. Ptah Assist gets nothing an external client does not.

Without --workspace the model reaches the reading tools. With one it also
reaches the artifact tools, under the same rules ptah mcp applies: writing stays
refused until --allow-write names an artifact class, and an approval is asked
for per patch unless --auto-approve says otherwise.

The answer is the model's words. What Ptah actually did is the tool trace, which
--trace prints and --format json always carries. The two are kept apart on
purpose: a summary is not evidence, and a model that reports a check nobody ran
should be contradicted by the record rather than believed.

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
}

// runExplain answers one question.
func runExplain(cmd *cobra.Command, opts *explainOptions, question string) error {
	if err := validateFormat(cmd, opts.format); err != nil {
		return err
	}
	provider, err := resolveProvider(cmd, opts)
	if err != nil {
		return err
	}

	session, cleanup, err := agentflags.Build(cmd, opts.agent, agentaudit.SurfaceAssist)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer cleanup()

	tools, err := connectTools(cmd, opts, session)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer tools.Close() //nolint:errcheck // the in-memory transport has nothing to fail at

	loop, err := assistloop.New(assistloop.Options{
		Provider:     provider,
		Tools:        tools,
		MaxToolCalls: opts.maxToolCalls,
		Emit:         emitter(cmd, opts),
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	result, runErr := loop.Run(cmd.Context(), question)
	if result == nil {
		return cmdutil.Fail(cmd, runErr)
	}
	if err := writeExplain(cmd, opts, result); err != nil {
		return err
	}
	if runErr != nil {
		// A run that hit a limit still produced a record worth printing, so the
		// document goes out first and the outcome is the exit code.
		return exitcode.New(1, runErr)
	}
	return nil
}

// resolveProvider selects and builds the model this run talks to.
func resolveProvider(cmd *cobra.Command, opts *explainOptions) (aiprovider.Provider, error) {
	loadOpts := assistconfig.Options{}
	config, err := assistconfig.Load(loadOpts)
	if err != nil {
		return nil, cmdutil.Fail(cmd, err)
	}
	profile, err := config.Select(opts.profile)
	if err != nil {
		return nil, cmdutil.Fail(cmd, err)
	}
	if opts.model != "" {
		profile.Model = opts.model
	}
	provider, err := config.Provider(profile, loadOpts)
	if err != nil {
		return nil, cmdutil.Fail(cmd, err)
	}
	return provider, nil
}

// connectTools wires this process to its own MCP server over an in-memory
// transport.
//
// Assist is a client of the same surface an external agent connects to, rather
// than a second caller of the operations underneath. That is what makes #1483's
// invariant structural: anything Assist can do is something `ptah mcp` serves,
// because it is literally the same server.
func connectTools(
	cmd *cobra.Command,
	opts *explainOptions,
	session *agentapi.Session,
) (*mcp.ClientSession, error) {
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	server := mcpserver.New(mcpserver.Config{
		Version: buildinfo.Resolve().Version,
		Session: session,
	})
	if _, err := server.Connect(cmd.Context(), serverTransport, nil); err != nil {
		return nil, fmt.Errorf("start the Ptah tool surface: %w", err)
	}

	client := mcp.NewClient(&mcp.Implementation{
		Name:    "ptah-assist",
		Version: buildinfo.Resolve().Version,
	}, &mcp.ClientOptions{
		ElicitationHandler: approvalPrompt(cmd, opts),
	})
	connected, err := client.Connect(cmd.Context(), clientTransport, nil)
	if err != nil {
		return nil, fmt.Errorf("connect to the Ptah tool surface: %w", err)
	}
	return connected, nil
}

// approvalPrompt answers the server's approval requests from the terminal.
//
// The server asks the same way it asks an external client, and this is Ptah
// Assist's answer to it. In a non-interactive run there is no handler at all:
// the capability broker then refuses rather than proceeding, which is what
// `--non-interactive` has to mean.
func approvalPrompt(
	cmd *cobra.Command,
	opts *explainOptions,
) func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
	if opts.nonInteractive {
		return nil
	}
	reader := bufio.NewReader(cmd.InOrStdin())
	return func(_ context.Context, request *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
		out := cmd.ErrOrStderr()
		fmt.Fprintf(out, "\n%s\n\n", request.Params.Message)
		fmt.Fprint(out, "Allow? [n]o / [o]nce / [s]ession: ")

		answer, err := reader.ReadString('\n')
		if err != nil && answer == "" {
			return &mcp.ElicitResult{Action: "cancel"}, nil
		}
		switch strings.ToLower(strings.TrimSpace(answer)) {
		case "o", "once", "y", "yes":
			return accepted("allow once"), nil
		case "s", "session":
			return accepted("allow for this session"), nil
		}
		return &mcp.ElicitResult{Action: "decline"}, nil
	}
}

// accepted builds the answer the server's schema expects.
func accepted(decision string) *mcp.ElicitResult {
	return &mcp.ElicitResult{
		Action:  "accept",
		Content: map[string]any{"decision": decision},
	}
}

// emitter prints progress to stderr, so a machine-readable answer on stdout
// stays parseable.
func emitter(cmd *cobra.Command, opts *explainOptions) func(assistloop.Event) {
	if !opts.trace {
		return nil
	}
	return func(event assistloop.Event) {
		fmt.Fprintf(cmd.ErrOrStderr(), "  %s: %s\n", event.Kind, event.Text)
	}
}

// writeExplain prints the answer and the record.
func writeExplain(cmd *cobra.Command, opts *explainOptions, result *assistloop.Result) error {
	report := explainReport{
		Answer:     result.Answer,
		Tools:      result.Tools,
		Provider:   result.Provider,
		Model:      result.Model,
		Turns:      result.Turns,
		StopReason: string(result.StopReason),
		Usage:      result.Usage,
		Verified:   result.UsedTools(),
	}
	if opts.format == formatJSON {
		return writeJSON(cmd.OutOrStdout(), report)
	}
	traced := make([]assistloop.ToolRecord, 0)
	if opts.trace {
		traced = report.Tools
	}
	writeExplainText(cmd.OutOrStdout(), report, traced)
	return nil
}

// writeExplainText prints the human form.
//
// The provenance line is not decoration. An answer that came from a model with
// no tool behind it looks exactly like one Ptah checked, and the difference is
// the whole question a reader has.
func writeExplainText(out io.Writer, report explainReport, traced []assistloop.ToolRecord) {
	for _, record := range traced {
		fmt.Fprintf(out, "  %s %s\n", outcomeWord[record.Failed], record.Name)
		fmt.Fprintf(out, "      %s\n", firstLine(record.Result))
	}
	if len(traced) > 0 {
		fmt.Fprintln(out, "")
	}
	fmt.Fprintln(out, strings.TrimSpace(report.Answer))
	fmt.Fprintln(out, "")
	fmt.Fprintf(out, "-- %s via %s, %d turn(s), %d tool call(s), %s\n",
		report.Model, report.Provider, report.Turns, len(report.Tools), report.StopReason)
	if !report.Verified {
		fmt.Fprintln(out,
			"-- No Ptah tool answered, so nothing above was checked against this project.")
	}
}

// outcomeWord marks a tool record in the trace.
var outcomeWord = map[bool]string{true: "refused", false: "ok      "}

// firstLine renders one line of a tool result for the trace.
func firstLine(result string) string {
	const width = 100
	line, _, _ := strings.Cut(strings.TrimSpace(result), "\n")
	if len(line) <= width {
		return line
	}
	return line[:width] + "..."
}
