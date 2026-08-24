package mcpserver_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.5x5.cz/ptah/internal/agentapi"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agenttarget"
	"go.5x5.cz/ptah/internal/mcpserver"
)

// diagnosticKey is where a failed call carries its structured diagnostic.
//
// The literal is repeated here rather than exported from the package under
// test, because it is a wire contract: a test that read the constant would keep
// passing through a rename that broke every client.
const diagnosticKey = "ptah.5x5.cz/diagnostic"

// diagnosticOf returns the structured diagnostic a failed call carries.
func diagnosticOf(c *qt.C, result *mcp.CallToolResult) map[string]any {
	c.Helper()
	c.Assert(result.IsError, qt.IsTrue, qt.Commentf("%s", textOf(c, result)))
	carried, present := result.Meta[diagnosticKey]
	c.Assert(present, qt.IsTrue, qt.Commentf("no %s in _meta: %v", diagnosticKey, result.Meta))
	diagnostic, isObject := carried.(map[string]any)
	c.Assert(isObject, qt.IsTrue, qt.Commentf("unexpected diagnostic shape %T", carried))
	return diagnostic
}

// TestServer_CodesAFailedCallForAProgramAndForAModel pins both halves of the
// contract at once, because they are read by different consumers and either one
// can be lost without the other noticing.
func TestServer_CodesAFailedCallForAProgramAndForAModel(t *testing.T) {
	c := qt.New(t)
	// The source is inside a configured root, so the call reaches the operation
	// and fails on the dialect rather than on the scope.
	session := connect(c, readOnlyConfig(c, "."), nil)

	result := call(c, session, "render_schema", map[string]any{
		"dialect": "orackle",
		"source":  map[string]any{"root_dirs": []string{"."}},
	})

	diagnostic := diagnosticOf(c, result)
	c.Assert(diagnostic["code"], qt.Equals, "invalid_request")
	c.Assert(diagnostic["actor"], qt.Equals, "caller")
	c.Assert(diagnostic["retryable"], qt.Equals, false)
	c.Assert(diagnostic["message"], qt.Equals, `unknown dialect "orackle"`)
	c.Assert(textOf(c, result), qt.Equals, `invalid_request: unknown dialect "orackle"`)
}

// TestServer_CodesADatabaseItCannotReachAsRetryable pins the one axis an agent
// is allowed to act on by calling again.
//
// The target is an ephemeral one -- the class the default policy allows an
// inspect on without asking -- pointed at a port nothing listens on, so the
// failure is the connection rather than the policy or the schema.
func TestServer_CodesADatabaseItCannotReachAsRetryable(t *testing.T) {
	c := qt.New(t)
	session := connect(c, unreachableTargetConfig(c, agentpolicy.ClassEphemeral), nil)

	result := call(c, session, "read_database", make(map[string]any))

	diagnostic := diagnosticOf(c, result)
	c.Assert(diagnostic["code"], qt.Equals, "database_unreachable")
	c.Assert(diagnostic["actor"], qt.Equals, "environment")
	c.Assert(diagnostic["retryable"], qt.Equals, true)
}

// TestServer_CodesAReadThePolicyRefusesAsTheOperatorsToClear is the same call
// against a production target, where the answer is a refusal rather than a
// connection: the two must not look alike to a caller, because retrying is
// right for one and useless for the other.
func TestServer_CodesAReadThePolicyRefusesAsTheOperatorsToClear(t *testing.T) {
	c := qt.New(t)
	session := connect(c, unreachableTargetConfig(c, agentpolicy.ClassProduction), nil)

	result := call(c, session, "read_database", make(map[string]any))

	diagnostic := diagnosticOf(c, result)
	c.Assert(diagnostic["code"], qt.Equals, "capability_denied")
	c.Assert(diagnostic["actor"], qt.Equals, "operator")
	c.Assert(diagnostic["retryable"], qt.Equals, false)
}

// unreachableTargetConfig serves one database the operator named, at an address
// nothing answers on.
func unreachableTargetConfig(c *qt.C, class agentpolicy.DatabaseClass) mcpserver.Config {
	c.Helper()
	policy, err := agentpolicy.Assemble()
	c.Assert(err, qt.IsNil)
	target, err := agenttarget.New(agenttarget.Config{
		Name: "app", URL: "postgres://nobody@127.0.0.1:1/none?sslmode=disable", Class: class,
	})
	c.Assert(err, qt.IsNil)
	set, err := agenttarget.NewSet(target)
	c.Assert(err, qt.IsNil)
	session, err := agentapi.NewSession(agentapi.SessionConfig{
		Broker:  agentpolicy.NewBroker(policy),
		Targets: set,
	})
	c.Assert(err, qt.IsNil)
	return mcpserver.Config{Version: "test", Session: session}
}

// TestServer_CodesTheSDKsOwnArgumentRefusal pins the one failure Ptah's code
// never sees.
//
// A call whose arguments do not match the input schema is refused by the SDK
// before the operation runs, and it reports that as plain text with no type to
// match on. Left alone it answered `internal`, telling an agent it had found a
// defect in Ptah when it had sent the wrong arguments.
func TestServer_CodesTheSDKsOwnArgumentRefusal(t *testing.T) {
	c := qt.New(t)
	session := connect(c, readOnlyConfig(c), nil)

	result := call(c, session, "render_schema", map[string]any{"dialect": "postgres"})

	diagnostic := diagnosticOf(c, result)
	c.Assert(diagnostic["code"], qt.Equals, "invalid_request")
	c.Assert(diagnostic["actor"], qt.Equals, "caller")
	c.Assert(textOf(c, result), qt.Contains, `missing properties: ["source"]`)
}

// TestServer_CodesADeniedCapabilityAndNamesTheRemedy pins the refusal an agent
// is most likely to retry, and the sentence that tells the person watching what
// would clear it.
func TestServer_CodesADeniedCapabilityAndNamesTheRemedy(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictDeny, nil)
	session := connect(c, fixture.config, nil)
	digest := migrationsDigest(c, session)
	preview := previewPatch(c, session, digest)

	result := call(c, session, "apply_patch", map[string]any{
		"patch_id":      preview["patch_id"],
		"preview_token": preview["preview_token"],
	})

	diagnostic := diagnosticOf(c, result)
	c.Assert(diagnostic["code"], qt.Equals, "capability_denied")
	c.Assert(diagnostic["actor"], qt.Equals, "operator")
	c.Assert(diagnostic["retryable"], qt.Equals, false)
	c.Assert(diagnostic["hint"], qt.Contains, "describe_session reports what this session may do")
}

// TestServer_AnswersAGateFailureWithBothTheErrorAndTheAnswer pins the one call
// that carries a failure and a structured response together.
//
// The patch was written, verification found what it introduced, and it was
// undone. An agent needs the diagnostics to fix the file and the digests to
// know what the directory holds now, and neither survives in prose.
func TestServer_AnswersAGateFailureWithBothTheErrorAndTheAnswer(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	session := connect(c, fixture.config, nil)
	digest := migrationsDigest(c, session)
	preview := structured(c, call(c, session, "preview_patch", map[string]any{
		"artifact":        "migrations",
		"expected_digest": digest,
		"summary":         "a migration whose SQL does not parse",
		"changes": []any{map[string]any{
			"path":      "1700000200_broken.up.sql",
			"operation": "create",
			"content":   "CREATE TABLE ((( ;\n",
		}},
	}))

	result := call(c, session, "apply_patch", map[string]any{
		"patch_id":      preview["patch_id"],
		"preview_token": preview["preview_token"],
	})

	c.Assert(diagnosticOf(c, result)["code"], qt.Equals, "gate_failed")
	c.Assert(result.StructuredContent, qt.IsNotNil,
		qt.Commentf("a rolled-back apply must still report what it found"))
	answer := errorAnswer(c, result)
	c.Assert(answer["rolled_back"], qt.Equals, true)
	introduced, _ := answer["introduced"].([]any)
	c.Assert(introduced, qt.HasLen, 1)
	first, _ := introduced[0].(map[string]any)
	c.Assert(first["gate"], qt.Equals, "migration-sql")
	c.Assert(first["path"], qt.Equals, "1700000200_broken.up.sql")
}

// TestServer_CarriesNoDiagnosticOnASuccessfulCall pins the other direction: the
// key is present when something failed and absent when nothing did, so a client
// can use its presence as the test.
func TestServer_CarriesNoDiagnosticOnASuccessfulCall(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	session := connect(c, fixture.config, nil)

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "describe_session",
		Arguments: make(map[string]any),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.IsError, qt.IsFalse, qt.Commentf("%s", textOf(c, result)))
	_, present := result.Meta[diagnosticKey]
	c.Assert(present, qt.IsFalse)
}

// errorAnswer decodes the structured content of a result that also failed.
//
// It is separate from structured because that helper asserts the call
// succeeded, which is exactly what this case does not.
func errorAnswer(c *qt.C, result *mcp.CallToolResult) map[string]any {
	c.Helper()
	raw, err := json.Marshal(result.StructuredContent)
	c.Assert(err, qt.IsNil)
	decoded := make(map[string]any)
	c.Assert(json.Unmarshal(raw, &decoded), qt.IsNil)
	return decoded
}
