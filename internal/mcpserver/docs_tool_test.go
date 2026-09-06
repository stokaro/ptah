package mcpserver_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"ptah.run/internal/agentapi"
	"ptah.run/internal/agentgate"
	"ptah.run/internal/agentpolicy"
	"ptah.run/internal/mcpserver"
)

// search_docs answers a question about Ptah from Ptah, over the served
// surface -- stokaro/ptah#2123.
//
// Driven through a real client session rather than by calling the operation,
// because the thing being measured is what a model reaches, and every other
// gap on this surface was between the operation and what was exposed.
func TestSearchDocs_AnswersFromPtahsOwnDocumentation(t *testing.T) {
	c := qt.New(t)

	result := callTool(c, docsSession(c, agentpolicy.VerdictAllow), "search_docs", map[string]any{
		"query": "difference between migrations checkpoint and migrations baseline",
	})

	passages := result["passages"].([]any)
	c.Assert(len(passages) > 0, qt.IsTrue)
	first := passages[0].(map[string]any)
	c.Assert(first["path"], qt.Matches, `docs/.*\.mdx?`)
	c.Assert(first["heading"], qt.Not(qt.Equals), "")
	c.Assert(first["text"], qt.Not(qt.Equals), "")
}

// A question the documentation does not answer comes back empty, and empty is
// a result rather than an error. Returning the nearest paragraph would be a
// confident wrong answer, which is the failure this tool exists to remove.
func TestSearchDocs_AnUnansweredQuestionReturnsNoPassages(t *testing.T) {
	c := qt.New(t)

	result := callTool(c, docsSession(c, agentpolicy.VerdictAllow), "search_docs", map[string]any{
		"query": "what is the price of a subscription refund",
	})

	c.Assert(result["passages"], qt.HasLen, 0)
	c.Assert(result["documents"].(float64) > 0, qt.IsTrue)
}

// The capability is consulted, not merely published. A verdict that
// describe_session reports and no operation reads is the bypass ADR 0006 was
// written for.
func TestSearchDocs_IsRefusedWhenTheCapabilityIsDenied(t *testing.T) {
	c := qt.New(t)

	refusal := callToolError(c, docsSession(c, agentpolicy.VerdictDeny), "search_docs", map[string]any{
		"query": "difference between migrations checkpoint and migrations baseline",
	})

	c.Assert(refusal, qt.Contains, "docs.read")
}

func TestSearchDocs_HonorsTheLimit(t *testing.T) {
	c := qt.New(t)

	result := callTool(c, docsSession(c, agentpolicy.VerdictAllow), "search_docs", map[string]any{
		"query": "migration",
		"limit": 2,
	})

	c.Assert(len(result["passages"].([]any)) <= 2, qt.IsTrue)
}

// describe_session reports the documentation as reachability, beside the
// databases and the source roots. `docs.read allow` beside a count of zero
// would be a tool offered with nothing behind it.
func TestDescribeSession_ReportsWhatTheDocumentationToolCanReach(t *testing.T) {
	c := qt.New(t)

	result := callTool(c, docsSession(c, agentpolicy.VerdictAllow), "describe_session", make(map[string]any))

	documentation := result["documentation"].(map[string]any)
	c.Assert(documentation["documents"].(float64) > 0, qt.IsTrue)
	c.Assert(documentation["passages"].(float64) > documentation["documents"].(float64), qt.IsTrue)
}

// docsSession builds a session whose only policy statement is about docs.read.
// It carries no workspace, because reading Ptah's documentation needs none --
// and a fixture that supplied one would not notice if the operation started
// depending on it.
func docsSession(c *qt.C, docs agentpolicy.Verdict) *mcp.ClientSession {
	c.Helper()
	policy, err := agentpolicy.Assemble(agentpolicy.LayerRules{
		Layer:  agentpolicy.LayerInvocation,
		Source: "ptah mcp flags",
		Rules:  []agentpolicy.Rule{{Capability: agentpolicy.DocsRead, Verdict: docs}},
	})
	c.Assert(err, qt.IsNil)
	gates, err := agentgate.New(agentgate.Options{Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	session, err := agentapi.NewSession(agentapi.SessionConfig{
		Broker: agentpolicy.NewBroker(policy),
		Gates:  gates,
	})
	c.Assert(err, qt.IsNil)
	return connect(c, mcpserver.Config{Version: "test", Session: session}, nil)
}

// Every answer names the documentation it came from, and describe_session
// reports the same one.
//
// An answer from documentation that does not describe the binary being driven
// is worse than no answer, because it is indistinguishable from a right one.
// Reporting the build in both places is what lets a reader tell that the
// passage and the session describe one thing (stokaro/ptah#2123).
func TestSearchDocs_NamesTheSameDocumentationDescribeSessionReports(t *testing.T) {
	c := qt.New(t)
	session := docsSession(c, agentpolicy.VerdictAllow)

	answer := callTool(c, session, "search_docs", map[string]any{
		"query": "difference between migrations checkpoint and migrations baseline",
	})["documentation"].(map[string]any)
	reported := callTool(c, session, "describe_session", make(map[string]any))["documentation"].(map[string]any)

	c.Assert(answer["version"], qt.Not(qt.Equals), "")
	c.Assert(answer["commit"], qt.Not(qt.Equals), "")
	c.Assert(answer["version"], qt.Equals, reported["version"])
	c.Assert(answer["commit"], qt.Equals, reported["commit"])
}

// An empty answer names it too. A question the documentation does not answer is
// still an answer about a particular documentation, and a reader deciding
// whether to trust the emptiness needs to know which.
func TestSearchDocs_AnEmptyAnswerStillNamesItsDocumentation(t *testing.T) {
	c := qt.New(t)

	result := callTool(c, docsSession(c, agentpolicy.VerdictAllow), "search_docs", map[string]any{
		"query": "what is the price of a subscription refund",
	})

	c.Assert(result["passages"], qt.HasLen, 0)
	c.Assert(result["documentation"].(map[string]any)["version"], qt.Not(qt.Equals), "")
}
