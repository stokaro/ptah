package agentpolicy_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/agentpolicy"
)

// Reading Ptah's own documentation is its own capability, and it is authorized
// like every other operation on this surface -- stokaro/ptah#2123.
func TestDocsRead_IsAllowedByDefault(t *testing.T) {
	c := qt.New(t)

	decision := decide(c, defaultsOnly(c), agentpolicy.Request{
		Capability: agentpolicy.DocsRead,
		Reason:     "answer a question about a flag",
	})

	c.Assert(decision.Verdict, qt.Equals, agentpolicy.VerdictAllow)
	c.Assert(decision.Layer, qt.Equals, agentpolicy.LayerBuiltin)
}

// A builtin allow is a default, not a guarantee. An operator who refuses it
// gets a refusal, and the refusal is what the policy report already said.
func TestDocsRead_AnOperatorCanRefuseIt(t *testing.T) {
	c := qt.New(t)

	policy, err := agentpolicy.Assemble(agentpolicy.LayerRules{
		Layer:  agentpolicy.LayerInvocation,
		Source: "--deny=docs.read",
		Rules: []agentpolicy.Rule{{
			Capability: agentpolicy.DocsRead,
			Verdict:    agentpolicy.VerdictDeny,
		}},
	})
	c.Assert(err, qt.IsNil)

	decision := decide(c, policy, agentpolicy.Request{
		Capability: agentpolicy.DocsRead,
		Reason:     "answer a question about a flag",
	})

	c.Assert(decision.Verdict, qt.Equals, agentpolicy.VerdictDeny)
	c.Assert(decision.Layer, qt.Equals, agentpolicy.LayerInvocation)
}

// The reason it is not project.read, as a measurement rather than a comment.
//
// Ptah's documentation is not the operator's content. An operator who refuses
// project reading is refusing to have their workspace described; folding the
// two together would also take away the answers that would have told them what
// they were refusing.
func TestDocsRead_IsNotProjectRead(t *testing.T) {
	c := qt.New(t)

	policy, err := agentpolicy.Assemble(agentpolicy.LayerRules{
		Layer:  agentpolicy.LayerInvocation,
		Source: "--deny=project.read",
		Rules: []agentpolicy.Rule{{
			Capability: agentpolicy.ProjectRead,
			Verdict:    agentpolicy.VerdictDeny,
		}},
	})
	c.Assert(err, qt.IsNil)

	c.Assert(decide(c, policy, agentpolicy.Request{
		Capability: agentpolicy.ProjectRead,
		Reason:     "describe the workspace",
	}).Verdict, qt.Equals, agentpolicy.VerdictDeny)
	c.Assert(decide(c, policy, agentpolicy.Request{
		Capability: agentpolicy.DocsRead,
		Reason:     "answer a question about a flag",
	}).Verdict, qt.Equals, agentpolicy.VerdictAllow)
}

// It carries no scope: there is one documentation set, not one per database or
// per artifact class, so the resolved table has exactly one cell for it. A
// capability that grew a scope by accident would report several, and an
// operator reading the table would be asked to grant something that does not
// exist.
func TestDocsRead_HasExactlyOneCell(t *testing.T) {
	c := qt.New(t)

	c.Assert(entriesFor(defaultsOnly(c), agentpolicy.DocsRead), qt.HasLen, 1)
}

func TestDocsRead_IsNamedInThePolicyVocabulary(t *testing.T) {
	c := qt.New(t)

	parsed, err := agentpolicy.ParseCapability("docs.read")

	c.Assert(err, qt.IsNil)
	c.Assert(parsed, qt.Equals, agentpolicy.DocsRead)
	c.Assert(agentpolicy.Capabilities(), qt.Contains, agentpolicy.DocsRead)
}

func defaultsOnly(c *qt.C) *agentpolicy.Policy {
	policy, err := agentpolicy.Assemble()
	c.Assert(err, qt.IsNil)
	return policy
}

func decide(c *qt.C, policy *agentpolicy.Policy, request agentpolicy.Request) agentpolicy.Decision {
	decision, err := policy.Decide(request)
	c.Assert(err, qt.IsNil)
	return decision
}

func entriesFor(policy *agentpolicy.Policy, capability agentpolicy.Capability) []agentpolicy.Entry {
	var found []agentpolicy.Entry
	for _, entry := range policy.Entries() {
		found = appendMatching(found, entry, capability)
	}
	return found
}

func appendMatching(found []agentpolicy.Entry, entry agentpolicy.Entry, capability agentpolicy.Capability) []agentpolicy.Entry {
	if entry.Capability != capability {
		return found
	}
	return append(found, entry)
}
