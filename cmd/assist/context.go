package assist

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/agentflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/internal/aiprovider"
	"go.5x5.cz/ptah/internal/assistsession"
)

// contextOptions is what "assist context" takes.
type contextOptions struct {
	agent   *agentflags.Options
	profile string
	model   string
	format  string
	resume  string
}

// newContextCommand returns "assist context".
func newContextCommand() *cobra.Command {
	opts := &contextOptions{}
	cmd := &cobra.Command{
		Use:   "context <question>",
		Short: "Print what a question would send to the model provider, and send nothing",
		Long: `Print exactly what asking this question would send to the model provider, without
sending it.

This is the boundary made checkable. The report is built by the same code that
builds the real request, so it cannot describe one thing while another leaves
the machine.

On the first request Ptah sends its own instructions, the tool schemas, and your
question. None of those carries anything about your project: the schemas are
names and argument shapes, and the instructions are the same for every project.

Project content reaches the provider when a tool answers -- migration text,
schema files, database object names -- because that is what the model asked to
see. "ptah assist explain --trace" shows each one as it happens, and every run
reports how many bytes of it were sent.

With --resume the earlier conversation is part of what would be sent, and it is
printed here too.`,
		Args:          cobra.ExactArgs(1),
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runContext(cmd, opts, args[0])
		},
	}
	opts.agent = agentflags.Register(cmd)
	flags := cmd.Flags()
	flags.StringVar(&opts.profile, profileFlag, "",
		"Provider profile to report for; the default profile when omitted")
	flags.StringVar(&opts.model, modelFlag, "",
		"Model identifier, overriding the profile's")
	flags.StringVar(&opts.format, formatFlag, formatText, "Output format: text or json")
	flags.StringVar(&opts.resume, resumeFlag, "",
		"Include the history of a saved session, by id or a unique prefix of one")
	cmdutil.ConfigureCommandArgs(cmd, cobra.ExactArgs(1))
	return cmd
}

// contextReport is the machine-readable boundary report.
type contextReport struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
	// Request is the payload itself, so a reader can check the summary against
	// the thing it summarizes rather than take it on trust.
	Request aiprovider.Request `json:"request"`
	Sizes   contextSizes       `json:"sizes"`
	// ProjectContent reports whether anything in this request describes the
	// project. It is false for a first request and true only once a resumed
	// conversation carries earlier answers.
	ProjectContent bool `json:"project_content"`
}

// contextSizes is the size of each part, in bytes of content.
//
// Content rather than wire payload: the encoding each provider wraps it in is
// the adapter's, and a number that changed with the adapter would not answer
// the question a person is asking.
type contextSizes struct {
	System    int `json:"system"`
	Tools     int `json:"tools"`
	ToolCount int `json:"tool_count"`
	Messages  int `json:"messages"`
	Total     int `json:"total"`
}

// runContext prints the boundary report.
func runContext(cmd *cobra.Command, opts *contextOptions, question string) error {
	if err := validateFormat(cmd, opts.format); err != nil {
		return err
	}
	provider, err := resolveProvider(cmd, opts.profile, opts.model)
	if err != nil {
		return err
	}

	session, cleanup, err := agentflags.BuildInert(cmd, opts.agent)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer cleanup()

	tools, err := connectTools(cmd, session, nil)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer tools.Close() //nolint:errcheck // the in-memory transport has nothing to fail at

	history, err := resumeHistory(opts.agent, opts.resume)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	loop, err := newLoop(provider, tools, &conversation{history: history}, 0, nil, nil)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	request, err := loop.Preview(cmd.Context(), question)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	return writeContext(cmd, opts, provider, request, len(history) > 0)
}

// resumeHistory reads a saved conversation without starting a new one.
//
// "assist context" sends nothing and must leave nothing: a command that created
// a session file to report what a question would send would have made the
// report untrue by writing it.
func resumeHistory(agent *agentflags.Options, resume string) ([]aiprovider.Message, error) {
	if resume == "" {
		return nil, nil
	}
	root := agent.Workspace
	if root == "" {
		working, err := workingDirectory()
		if err != nil {
			return nil, err
		}
		root = working
	}
	store, err := assistsession.Open(assistsession.Options{Root: root})
	if err != nil {
		return nil, err
	}
	records, err := store.Read(resume)
	if err != nil {
		return nil, err
	}
	return assistsession.Messages(records), nil
}

// measure sizes each part of a request.
func measure(request aiprovider.Request) contextSizes {
	sizes := contextSizes{System: len(request.System), ToolCount: len(request.Tools)}
	for _, tool := range request.Tools {
		sizes.Tools += len(tool.Name) + len(tool.Description) + len(tool.Schema)
	}
	for _, message := range request.Messages {
		sizes.Messages += len(message.Content)
	}
	sizes.Total = sizes.System + sizes.Tools + sizes.Messages
	return sizes
}

// writeContext prints the report.
func writeContext(
	cmd *cobra.Command,
	opts *contextOptions,
	provider aiprovider.Provider,
	request aiprovider.Request,
	resumed bool,
) error {
	sizes := measure(request)
	if opts.format == formatJSON {
		return writeJSON(cmd.OutOrStdout(), contextReport{
			Provider:       provider.Profile(),
			Model:          provider.Model(),
			Request:        request,
			Sizes:          sizes,
			ProjectContent: resumed,
		})
	}

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Nothing below has been sent.\n\n")
	fmt.Fprintf(out, "Asking this question would send the following to %s via %s.\n\n",
		provider.Model(), provider.Profile())
	fmt.Fprintf(out, "  %-22s %7d bytes  %s\n", "Ptah's instructions", sizes.System,
		"the same for every project")
	fmt.Fprintf(out, "  %-22s %7d bytes  %d tools: names and argument shapes\n",
		"Tool schemas", sizes.Tools, sizes.ToolCount)
	fmt.Fprintf(out, "  %-22s %7d bytes  %d message(s)\n", "Conversation", sizes.Messages,
		len(request.Messages))
	fmt.Fprintf(out, "  %-22s %7d bytes\n\n", "Total", sizes.Total)

	writeMessages(out, request.Messages)
	writeToolNames(out, request.Tools)
	writeBoundaryNote(out, resumed)
	return nil
}

// writeMessages prints the conversation that would be sent.
func writeMessages(out io.Writer, messages []aiprovider.Message) {
	fmt.Fprintf(out, "Conversation:\n")
	for _, message := range messages {
		fmt.Fprintf(out, "  %-10s %s\n", message.Role, firstResultLine(message.Content))
	}
	fmt.Fprintln(out, "")
}

// writeToolNames prints what the model would be offered, read from the server.
func writeToolNames(out io.Writer, tools []aiprovider.ToolDefinition) {
	fmt.Fprintf(out, "Tools offered:\n")
	for _, tool := range tools {
		fmt.Fprintf(out, "  %s\n", tool.Name)
	}
	fmt.Fprintln(out, "")
}

// writeBoundaryNote says what this request does and does not carry.
func writeBoundaryNote(out io.Writer, resumed bool) {
	fmt.Fprintln(out, boundaryNote[resumed])
}

// boundaryNote is what to say about project content, by whether a resumed
// conversation put any in.
var boundaryNote = map[bool]string{
	false: "Nothing above describes this project. Ptah's instructions and the tool schemas\n" +
		"are the same whatever the project is, and the only other thing here is your\n" +
		"question. Project content reaches the provider when a tool answers -- migration\n" +
		"text, schema files, database object names -- because that is what the model asked\n" +
		"to see. Every run reports how many bytes of it were sent, and --trace shows each.",
	true: "The resumed conversation above carries earlier answers, which may describe this\n" +
		"project. Tool results are deliberately not replayed, so what is here is what was\n" +
		"said rather than what was read. Further project content reaches the provider when\n" +
		"a tool answers during the run.",
}
