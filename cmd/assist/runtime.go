package assist

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/internal/agentapi"
	"go.5x5.cz/ptah/internal/aiprovider"
	"go.5x5.cz/ptah/internal/assistconfig"
	"go.5x5.cz/ptah/internal/assistloop"
	"go.5x5.cz/ptah/internal/buildinfo"
	"go.5x5.cz/ptah/internal/mcpserver"
)

// toolSession is the half of the protocol client the surfaces use: list what
// is available, and call one. Narrowed to those two so a test can supply
// something other than a live session, and so the surfaces cannot reach past
// the tool surface into the client.
type toolSession interface {
	ListTools(ctx context.Context, params *mcp.ListToolsParams) (*mcp.ListToolsResult, error)
	CallTool(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error)
}

// writer is io.Writer under a shorter name for the printing helpers.
type writer = io.Writer

// approvalHandler is the shape the protocol client takes for an approval
// request. A nil one means this session cannot ask, and the capability broker
// then refuses rather than proceeding.
type approvalHandler func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error)

// resolveProvider selects and builds the model a run talks to.
func resolveProvider(cmd *cobra.Command, profileName, model string) (aiprovider.Provider, error) {
	loadOpts := assistconfig.Options{}
	config, err := assistconfig.Load(loadOpts)
	if err != nil {
		return nil, cmdutil.Fail(cmd, err)
	}
	profile, err := config.Select(profileName)
	if err != nil {
		return nil, cmdutil.Fail(cmd, err)
	}
	if model != "" {
		profile.Model = model
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
	session *agentapi.Session,
	approve approvalHandler,
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
	}, &mcp.ClientOptions{ElicitationHandler: approve})
	connected, err := client.Connect(cmd.Context(), clientTransport, nil)
	if err != nil {
		return nil, fmt.Errorf("connect to the Ptah tool surface: %w", err)
	}
	return connected, nil
}

// terminalApprover answers the server's approval requests from the terminal.
//
// The server asks the way it asks any client, and this is Ptah Assist's answer
// to it. The prompt goes to stderr so a machine-readable run's document stays
// parseable, and the answer is read from the same reader the interactive loop
// uses -- one reader, because two would each buffer and the second would eat
// the first's line.
func terminalApprover(cmd *cobra.Command, reader *bufio.Reader) approvalHandler {
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

// newLoop builds the model loop for one surface.
func newLoop(
	provider aiprovider.Provider,
	tools toolSession,
	talk *conversation,
	maxToolCalls int,
	emit func(assistloop.Event),
) (*assistloop.Loop, error) {
	return assistloop.New(assistloop.Options{
		Provider:     provider,
		Tools:        tools,
		History:      talk.history,
		MaxToolCalls: maxToolCalls,
		Emit:         emit,
		OnTool:       talk.tool,
	})
}

// writeTrace prints what Ptah did, one line per tool call.
func writeTrace(out io.Writer, records []assistloop.ToolRecord) {
	for _, record := range records {
		fmt.Fprintf(out, "  %s %s\n", outcomeWord[record.Failed], record.Name)
		fmt.Fprintf(out, "      %s\n", firstResultLine(record.Result))
	}
	if len(records) > 0 {
		fmt.Fprintln(out, "")
	}
}

// outcomeWord marks a tool record in the trace.
var outcomeWord = map[bool]string{true: "refused", false: "ok      "}

// firstResultLine renders one line of a tool result for the trace.
func firstResultLine(result string) string {
	const width = 100
	line, _, _ := strings.Cut(strings.TrimSpace(result), "\n")
	if len(line) <= width {
		return line
	}
	return line[:width] + "..."
}

// writeProvenance prints who answered and whether Ptah checked anything.
//
// The second line is the one that matters. An answer with no tool behind it
// looks exactly like a verified one, and the difference is the whole question a
// reader has.
func writeProvenance(out io.Writer, result *assistloop.Result) {
	fmt.Fprintf(out, "-- %s via %s, %d turn(s), %d tool call(s), %s\n",
		result.Model, result.Provider, result.Turns, len(result.Tools), result.StopReason)
	if !result.UsedTools() {
		fmt.Fprintln(out,
			"-- No Ptah tool answered, so nothing above was checked against this project.")
		return
	}
	// What a tool returned is what left the machine about this project, so the
	// size of it is the one number a person can act on. "ptah assist context"
	// reports the other half: what a request carries before any tool answers.
	fmt.Fprintf(out, "-- %d bytes of project content reached %s, from %d tool answer(s).\n",
		result.ToolBytes(), result.Provider, len(result.Tools))
}
