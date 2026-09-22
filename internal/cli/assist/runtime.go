package assist

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"

	"ptah.run/internal/agentapi"
	"ptah.run/internal/aiprovider"
	"ptah.run/internal/assistconfig"
	"ptah.run/internal/assistloop"
	"ptah.run/internal/buildinfo"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/mcpserver"
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
	server, err := mcpserver.New(mcpserver.Config{
		Version: buildinfo.Resolve().Version,
		Session: session,
	})
	if err != nil {
		return nil, err
	}
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
func terminalApprover(cmd *cobra.Command, input prompter) approvalHandler {
	return func(_ context.Context, request *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
		out := cmd.ErrOrStderr()
		fmt.Fprintf(out, "\n%s\n\n", request.Params.Message)

		// Not remembered: walking Up through "o", "s" and "n" to reach the
		// last real question is not a history worth having.
		answer, err := input.confirm("Allow? [n]o / [o]nce / [s]ession: ")
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
	onText func(string),
) (*assistloop.Loop, error) {
	return assistloop.New(assistloop.Options{
		Provider:     provider,
		Tools:        tools,
		History:      talk.history,
		MaxToolCalls: maxToolCalls,
		Emit:         emit,
		OnText:       onText,
		OnTool:       talk.tool,
	})
}

// streamTo writes the model's answer to out as it arrives.
func streamTo(out io.Writer) func(string) {
	return func(fragment string) {
		fmt.Fprint(out, fragment)
	}
}

// answerWriter is where a format wants the answer delivered as it arrives, and
// nil where the format renders a document instead.
//
// A JSON report has nowhere to put a fragment, and printing one beside it would
// break the only reason to ask for that format.
func answerWriter(out io.Writer, format string) func(string) {
	if !streamed[format] {
		return nil
	}
	return streamTo(out)
}

// streamed reports whether a format wants the answer as it arrives.
var streamed = map[string]bool{formatText: true, formatJSON: false, formatJSONL: false}

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
	return clip(firstLine(result), 100)
}

// firstLine is the first line of a value, with the surrounding space gone.
func firstLine(text string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(text), "\n")
	return line
}

// clip shortens a line to fit, at a word boundary.
//
// Cutting at the character the budget lands on leaves a word in halves --
// `Text ins` / `ide it` in a trace, `read one file inside it, with content
// digests.` broken across two rows in the tool list -- and a reader spends a
// moment reassembling it before deciding the line was not worth reading. The
// ellipsis says the rest is there; the half word says the program is broken.
//
// Runes rather than bytes, so a clip never lands inside one.
func clip(line string, width int) string {
	runes := []rune(line)
	if len(runes) <= width {
		return line
	}
	cut := string(runes[:width])
	if space := strings.LastIndexAny(cut, " \t"); space > width/2 {
		cut = cut[:space]
	}
	return strings.TrimRight(cut, " \t,;:") + "..."
}

// traceResultLine is what one tool call shows under its name in the trace.
//
// A tool answers with JSON, and every answer carries the same `notice` field:
// a paragraph telling the model that what follows is repository data rather
// than instructions. It is load-bearing for the model and pure noise for a
// reader, who sees the same sentence under every call and none of the answer.
// So it goes, and what is left is the part that differs.
func traceResultLine(result string, width int) string {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(result), &fields); err != nil {
		return clip(firstLine(result), width)
	}
	delete(fields, "notice")
	keys := slices.Sorted(maps.Keys(fields))
	var out strings.Builder
	for _, key := range keys {
		if out.Len() > 0 {
			out.WriteString(" ")
		}
		fmt.Fprintf(&out, "%s=%s", key, clip(firstLine(string(fields[key])), 40))
	}
	return clip(out.String(), width)
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
