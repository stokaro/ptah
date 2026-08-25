package mcpserver_test

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"sort"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentpolicy"
)

// contractSnapshotPath is the checked-in shape of the agent contract.
const contractSnapshotPath = "testdata/agent-contract.json"

// updateContractSnapshot rewrites the snapshot instead of comparing against it.
//
// The failure message named this flag before the flag existed, which is its own
// small version of the failure this file is about: an instruction nobody could
// follow, in the one place a reader goes when something breaks.
var updateContractSnapshot = flag.Bool("update-contract-snapshot", false,
	"rewrite testdata/agent-contract.json from the served surface")

// TestServer_TheContractSurfaceMatchesItsSnapshot makes the rule on
// [agentapi.Version] enforceable.
//
// That rule is written and was not measured: "a change to the shape of any
// request or response changes it, and so does adding an operation". Prose in a
// doc comment is read by whoever is already thinking about the contract, which
// is never the person who adds a field in a hurry. A caller that branches on
// contract_version relies on the version moving when the shape does, and
// nothing made that true.
//
// The snapshot is the tool surface as a client sees it: every tool served with
// and without a workspace, its description, and the full JSON Schema of its
// input and output. A change to any of them reddens here with the one
// instruction that matters -- decide whether it is compatible, and bump the
// version if it is not.
//
// It deliberately does NOT decide compatibility itself. A renamed field and an
// added optional one look the same to a differ and are not the same to a
// client, and a gate that guessed would either block harmless changes or wave
// breaking ones through. It stops at "this changed; say so on purpose".
func TestServer_TheContractSurfaceMatchesItsSnapshot(t *testing.T) {
	c := qt.New(t)

	current := contractSurface(c)
	refreshContractSnapshot(c, current)
	recorded, err := os.ReadFile(contractSnapshotPath)

	c.Assert(err, qt.IsNil, qt.Commentf(
		"regenerate with: go test ./internal/mcpserver/ -run TheContractSurface -update-contract-snapshot"))
	c.Assert(string(current), qt.Equals, string(recorded),
		qt.Commentf(
			"the agent contract surface changed.\n"+
				"If the change is incompatible for a client, bump agentapi.Version.\n"+
				"Either way, refresh %s -- deliberately, not to make this pass.",
			contractSnapshotPath))
}

// TestServer_TheSnapshotDescribesTheServedTools is the control.
//
// A snapshot compared against a snapshot passes whether it describes anything
// or not. This asserts the recorded surface names every tool the server serves
// and carries a schema for each, so an empty or truncated file fails as itself.
func TestServer_TheSnapshotDescribesTheServedTools(t *testing.T) {
	c := qt.New(t)

	recorded, err := os.ReadFile(contractSnapshotPath)
	c.Assert(err, qt.IsNil)
	var surface []contractTool
	c.Assert(json.Unmarshal(recorded, &surface), qt.IsNil)

	names := make(map[string]bool, len(surface))
	for _, tool := range surface {
		names[tool.Name] = true
		c.Assert(tool.InputSchema, qt.Not(qt.HasLen), 0,
			qt.Commentf("%s has no input schema in the snapshot", tool.Name))
	}
	for _, served := range servedToolNames(c) {
		c.Assert(names[served], qt.IsTrue,
			qt.Commentf("%s is served and absent from the snapshot", served))
	}
	c.Assert(names, qt.HasLen, len(servedToolNames(c)))
}

// contractTool is one tool as a client sees it.
type contractTool struct {
	Name         string          `json:"name"`
	Description  string          `json:"description"`
	InputSchema  json.RawMessage `json:"input_schema"`
	OutputSchema json.RawMessage `json:"output_schema,omitempty"`
}

// contractSurface renders the served tools in a stable order.
// refreshContractSnapshot rewrites the snapshot when the flag asks for it, and
// does nothing otherwise. It runs before the comparison rather than instead of
// it, so a regenerating run still reads the file back and still fails if the
// write did not take.
func refreshContractSnapshot(c *qt.C, current []byte) {
	c.Helper()
	if !*updateContractSnapshot {
		return
	}
	c.Assert(os.WriteFile(contractSnapshotPath, current, 0o600), qt.IsNil)
}

func contractSurface(c *qt.C) []byte {
	c.Helper()
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	session := connect(c, fixture.config, nil)
	listed, err := session.ListTools(context.Background(), nil)
	c.Assert(err, qt.IsNil)

	tools := make([]contractTool, 0, len(listed.Tools))
	for _, tool := range listed.Tools {
		tools = append(tools, contractTool{
			Name:         tool.Name,
			Description:  tool.Description,
			InputSchema:  marshalSchema(c, tool.InputSchema),
			OutputSchema: marshalSchema(c, tool.OutputSchema),
		})
	}
	sort.Slice(tools, func(i, j int) bool { return tools[i].Name < tools[j].Name })

	encoded, err := json.MarshalIndent(tools, "", "  ")
	c.Assert(err, qt.IsNil)
	return append(encoded, '\n')
}

// marshalSchema renders one JSON Schema, or nothing when the tool has none.
func marshalSchema(c *qt.C, schema any) json.RawMessage {
	c.Helper()
	if schema == nil {
		return nil
	}
	encoded, err := json.Marshal(schema)
	c.Assert(err, qt.IsNil)
	return encoded
}

// servedToolNames lists what a workspace server offers.
func servedToolNames(c *qt.C) []string {
	c.Helper()
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	session := connect(c, fixture.config, nil)
	listed, err := session.ListTools(context.Background(), nil)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(listed.Tools))
	for _, tool := range listed.Tools {
		names = append(names, tool.Name)
	}
	return names
}
