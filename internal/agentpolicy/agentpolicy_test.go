package agentpolicy_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentpolicy"
)

// verdictOf resolves one request against a policy and returns the spelling, so
// a row can carry the expected verdict as data rather than as a checker.
func verdictOf(c *qt.C, policy *agentpolicy.Policy, req agentpolicy.Request) string {
	decision, err := policy.Decide(req)
	c.Assert(err, qt.IsNil)
	return decision.Verdict.String()
}

func TestDefaults_DecideEveryCapabilityAndScope(t *testing.T) {
	// The builtin table has to state a verdict for every capability and every
	// scope of it. A missing cell resolves to the zero Verdict -- deny -- and
	// is indistinguishable from a deliberate refusal, so the completeness is
	// asserted rather than trusted.
	//
	// The expected count is derived from the exported enumeration rather than
	// written down: a literal here would have to be edited every time a
	// capability is added, and an edit that makes a red test green is not a
	// check on anything.
	c := qt.New(t)

	policy, err := agentpolicy.Assemble()
	c.Assert(err, qt.IsNil)

	cells := 0
	for _, capability := range agentpolicy.Capabilities() {
		for _, req := range agentpolicy.Scopes(capability) {
			cells++
			decision, decideErr := policy.Decide(req)
			c.Assert(decideErr, qt.IsNil)
			c.Assert(decision.Layer.String(), qt.Equals, "builtin")
		}
	}

	c.Assert(policy.Entries(), qt.HasLen, cells)
	c.Assert(policy.Ignored(), qt.HasLen, 0)
}

func TestDefaults_Verdicts(t *testing.T) {
	tests := []struct {
		name string
		req  agentpolicy.Request
		want string
	}{
		{
			name: "reading a declared schema is allowed",
			req:  agentpolicy.Request{Capability: agentpolicy.SchemaValidate},
			want: "allow",
		},
		{
			name: "reading an artifact in scope is allowed",
			req: agentpolicy.Request{
				Capability: agentpolicy.ArtifactRead,
				Artifact:   agentpolicy.ClassMigrations,
			},
			want: "allow",
		},
		{
			name: "writing an artifact asks",
			req: agentpolicy.Request{
				Capability: agentpolicy.ArtifactWrite,
				Artifact:   agentpolicy.ClassMigrations,
			},
			want: "ask",
		},
		{
			name: "deleting an artifact is denied",
			req: agentpolicy.Request{
				Capability: agentpolicy.ArtifactDelete,
				Artifact:   agentpolicy.ClassMigrations,
			},
			want: "deny",
		},
		{
			name: "inspecting a configured dev database asks",
			req: agentpolicy.Request{
				Capability: agentpolicy.DatabaseInspect,
				Database:   agentpolicy.ClassDev,
			},
			want: "ask",
		},
		{
			name: "inspecting a production database is denied",
			req: agentpolicy.Request{
				Capability: agentpolicy.DatabaseInspect,
				Database:   agentpolicy.ClassProduction,
			},
			want: "deny",
		},
		{
			// A URL that arrived as a tool argument is unclassified, and an
			// unrecognized address is not evidence of harmlessness.
			name: "inspecting an unclassified database is denied",
			req: agentpolicy.Request{
				Capability: agentpolicy.DatabaseInspect,
				Database:   agentpolicy.ClassUnclassified,
			},
			want: "deny",
		},
		{
			name: "reading rows is denied",
			req: agentpolicy.Request{
				Capability: agentpolicy.DatabaseReadRows,
				Database:   agentpolicy.ClassEphemeral,
			},
			want: "deny",
		},
		{
			name: "applying a migration is denied even to an ephemeral database",
			req: agentpolicy.Request{
				Capability: agentpolicy.MigrationApply,
				Database:   agentpolicy.ClassEphemeral,
			},
			want: "deny",
		},
		{
			name: "resolving a schema that runs repository code is denied",
			req:  agentpolicy.Request{Capability: agentpolicy.SchemaExternalExecute},
			want: "deny",
		},
		{
			name: "shell execution is denied",
			req:  agentpolicy.Request{Capability: agentpolicy.ShellExecute},
			want: "deny",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			policy, err := agentpolicy.Assemble()
			c.Assert(err, qt.IsNil)

			c.Assert(verdictOf(c, policy, test.req), qt.Equals, test.want)
		})
	}
}

func TestAssemble_OperatorLayersWiden(t *testing.T) {
	tests := []struct {
		name  string
		layer agentpolicy.Layer
		rule  agentpolicy.Rule
		req   agentpolicy.Request
		want  string
	}{
		{
			name:  "invocation grants a scoped write",
			layer: agentpolicy.LayerInvocation,
			rule: agentpolicy.Rule{
				Capability: agentpolicy.ArtifactWrite,
				Artifact:   agentpolicy.ClassMigrations,
				Verdict:    agentpolicy.VerdictAllow,
			},
			req: agentpolicy.Request{
				Capability: agentpolicy.ArtifactWrite,
				Artifact:   agentpolicy.ClassMigrations,
			},
			want: "allow",
		},
		{
			name:  "user configuration grants an inspection",
			layer: agentpolicy.LayerUser,
			rule: agentpolicy.Rule{
				Capability: agentpolicy.DatabaseInspect,
				Database:   agentpolicy.ClassDev,
				Verdict:    agentpolicy.VerdictAllow,
			},
			req: agentpolicy.Request{
				Capability: agentpolicy.DatabaseInspect,
				Database:   agentpolicy.ClassDev,
			},
			want: "allow",
		},
		{
			name:  "an operator layer narrows too",
			layer: agentpolicy.LayerInvocation,
			rule: agentpolicy.Rule{
				Capability: agentpolicy.SchemaRender,
				Verdict:    agentpolicy.VerdictDeny,
			},
			req:  agentpolicy.Request{Capability: agentpolicy.SchemaRender},
			want: "deny",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			policy, err := agentpolicy.Assemble(agentpolicy.LayerRules{
				Layer:  test.layer,
				Source: "test",
				Rules:  []agentpolicy.Rule{test.rule},
			})
			c.Assert(err, qt.IsNil)

			c.Assert(verdictOf(c, policy, test.req), qt.Equals, test.want)
			c.Assert(policy.Ignored(), qt.HasLen, 0)
		})
	}
}

func TestAssemble_ScopedRuleBeatsUnscopedRegardlessOfOrder(t *testing.T) {
	// Both orderings are exercised because the alternative -- applying rules in
	// file order -- passes whichever ordering the test happened to write, and
	// the failure it hides is a policy whose meaning depends on how the lines
	// were sorted in an editor.
	tests := []struct {
		name  string
		rules []agentpolicy.Rule
	}{
		{
			name: "unscoped first",
			rules: []agentpolicy.Rule{
				{Capability: agentpolicy.ArtifactWrite, Verdict: agentpolicy.VerdictDeny},
				{
					Capability: agentpolicy.ArtifactWrite,
					Artifact:   agentpolicy.ClassMigrations,
					Verdict:    agentpolicy.VerdictAllow,
				},
			},
		},
		{
			name: "scoped first",
			rules: []agentpolicy.Rule{
				{
					Capability: agentpolicy.ArtifactWrite,
					Artifact:   agentpolicy.ClassMigrations,
					Verdict:    agentpolicy.VerdictAllow,
				},
				{Capability: agentpolicy.ArtifactWrite, Verdict: agentpolicy.VerdictDeny},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			policy, err := agentpolicy.Assemble(agentpolicy.LayerRules{
				Layer:  agentpolicy.LayerInvocation,
				Source: "test",
				Rules:  test.rules,
			})
			c.Assert(err, qt.IsNil)

			c.Assert(verdictOf(c, policy, agentpolicy.Request{
				Capability: agentpolicy.ArtifactWrite,
				Artifact:   agentpolicy.ClassMigrations,
			}), qt.Equals, "allow")
			c.Assert(verdictOf(c, policy, agentpolicy.Request{
				Capability: agentpolicy.ArtifactWrite,
				Artifact:   agentpolicy.ClassSchema,
			}), qt.Equals, "deny")
		})
	}
}

func TestAssemble_ProjectLayerMayOnlyNarrow(t *testing.T) {
	// The refusal side: a repository-controlled file cannot grant itself a
	// write the operator did not grant.
	c := qt.New(t)

	policy, err := agentpolicy.Assemble(agentpolicy.LayerRules{
		Layer:  agentpolicy.LayerProject,
		Source: ".ptah/agent-policy",
		Rules: []agentpolicy.Rule{{
			Capability: agentpolicy.ArtifactWrite,
			Artifact:   agentpolicy.ClassMigrations,
			Verdict:    agentpolicy.VerdictAllow,
		}},
	})
	c.Assert(err, qt.IsNil)

	c.Assert(verdictOf(c, policy, agentpolicy.Request{
		Capability: agentpolicy.ArtifactWrite,
		Artifact:   agentpolicy.ClassMigrations,
	}), qt.Equals, "ask")

	ignored := policy.Ignored()
	c.Assert(ignored, qt.HasLen, 1)
	c.Assert(ignored[0].Layer, qt.Equals, "project")
	c.Assert(ignored[0].Source, qt.Equals, ".ptah/agent-policy")
	c.Assert(ignored[0].Rule, qt.Equals, "artifact.write:migrations allow")
	c.Assert(ignored[0].Applied, qt.Equals, "ask")
	c.Assert(ignored[0].Reason, qt.Equals,
		"a project policy may only narrow, and ask is already in effect")
}

func TestAssemble_ProjectLayerNarrows(t *testing.T) {
	// The control for the test above. Without it, deleting the project layer
	// entirely would keep the refusal green: a layer that is never read cannot
	// widen anything either.
	c := qt.New(t)

	policy, err := agentpolicy.Assemble(agentpolicy.LayerRules{
		Layer:  agentpolicy.LayerProject,
		Source: ".ptah/agent-policy",
		Rules: []agentpolicy.Rule{{
			Capability: agentpolicy.ArtifactWrite,
			Artifact:   agentpolicy.ClassSchema,
			Verdict:    agentpolicy.VerdictDeny,
		}},
	})
	c.Assert(err, qt.IsNil)

	c.Assert(verdictOf(c, policy, agentpolicy.Request{
		Capability: agentpolicy.ArtifactWrite,
		Artifact:   agentpolicy.ClassSchema,
	}), qt.Equals, "deny")
	c.Assert(policy.Ignored(), qt.HasLen, 0)
}

func TestAssemble_ProjectCannotWidenWhatAnOperatorGranted(t *testing.T) {
	// An operator grant plus a project attempt to widen a different scope: the
	// grant stands and the project's line is reported, which is the shape a
	// real session has.
	c := qt.New(t)

	policy, err := agentpolicy.Assemble(
		agentpolicy.LayerRules{
			Layer:  agentpolicy.LayerInvocation,
			Source: "--allow",
			Rules: []agentpolicy.Rule{{
				Capability: agentpolicy.ArtifactWrite,
				Artifact:   agentpolicy.ClassMigrations,
				Verdict:    agentpolicy.VerdictAllow,
			}},
		},
		agentpolicy.LayerRules{
			Layer:  agentpolicy.LayerProject,
			Source: ".ptah/agent-policy",
			Rules: []agentpolicy.Rule{{
				Capability: agentpolicy.ArtifactDelete,
				Artifact:   agentpolicy.ClassMigrations,
				Verdict:    agentpolicy.VerdictAllow,
			}},
		},
	)
	c.Assert(err, qt.IsNil)

	c.Assert(verdictOf(c, policy, agentpolicy.Request{
		Capability: agentpolicy.ArtifactWrite,
		Artifact:   agentpolicy.ClassMigrations,
	}), qt.Equals, "allow")
	c.Assert(verdictOf(c, policy, agentpolicy.Request{
		Capability: agentpolicy.ArtifactDelete,
		Artifact:   agentpolicy.ClassMigrations,
	}), qt.Equals, "deny")
	c.Assert(policy.Ignored(), qt.HasLen, 1)
}

func TestAssemble_LayerOrderDoesNotDependOnArgumentOrder(t *testing.T) {
	// Passing the project layer first must not let it decide before the
	// invocation layer it is not allowed to widen past.
	c := qt.New(t)

	policy, err := agentpolicy.Assemble(
		agentpolicy.LayerRules{
			Layer:  agentpolicy.LayerProject,
			Source: ".ptah/agent-policy",
			Rules: []agentpolicy.Rule{{
				Capability: agentpolicy.ArtifactWrite,
				Artifact:   agentpolicy.ClassTests,
				Verdict:    agentpolicy.VerdictAllow,
			}},
		},
		agentpolicy.LayerRules{
			Layer:  agentpolicy.LayerInvocation,
			Source: "--allow",
			Rules: []agentpolicy.Rule{{
				Capability: agentpolicy.ArtifactWrite,
				Artifact:   agentpolicy.ClassTests,
				Verdict:    agentpolicy.VerdictDeny,
			}},
		},
	)
	c.Assert(err, qt.IsNil)

	c.Assert(verdictOf(c, policy, agentpolicy.Request{
		Capability: agentpolicy.ArtifactWrite,
		Artifact:   agentpolicy.ClassTests,
	}), qt.Equals, "deny")
}

func TestAssemble_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		layer   agentpolicy.LayerRules
		wantErr string
	}{
		{
			name: "granting shell execution",
			layer: agentpolicy.LayerRules{
				Layer: agentpolicy.LayerInvocation,
				Rules: []agentpolicy.Rule{{
					Capability: agentpolicy.ShellExecute,
					Verdict:    agentpolicy.VerdictAllow,
				}},
			},
			wantErr: `invocation policy: "shell.execute allow": capability cannot be granted`,
		},
		{
			name: "asking about arbitrary filesystem writes",
			layer: agentpolicy.LayerRules{
				Layer: agentpolicy.LayerUser,
				Rules: []agentpolicy.Rule{{
					Capability: agentpolicy.FilesystemArbitraryWrite,
					Verdict:    agentpolicy.VerdictAsk,
				}},
			},
			wantErr: `user policy: "filesystem.arbitrary_write ask": capability cannot be granted`,
		},
		{
			name: "an unknown capability",
			layer: agentpolicy.LayerRules{
				Layer: agentpolicy.LayerProject,
				Rules: []agentpolicy.Rule{{
					Capability: agentpolicy.Capability("schema.apply"),
					Verdict:    agentpolicy.VerdictDeny,
				}},
			},
			wantErr: `project policy: unknown capability "schema.apply"`,
		},
		{
			name: "a database class on an artifact capability",
			layer: agentpolicy.LayerRules{
				Layer: agentpolicy.LayerInvocation,
				Rules: []agentpolicy.Rule{{
					Capability: agentpolicy.ArtifactWrite,
					Database:   agentpolicy.ClassProduction,
					Verdict:    agentpolicy.VerdictAllow,
				}},
			},
			wantErr: `invocation policy: capability "artifact.write" is scoped by artifact class, not by database class`,
		},
		{
			name: "an artifact class on a database capability",
			layer: agentpolicy.LayerRules{
				Layer: agentpolicy.LayerInvocation,
				Rules: []agentpolicy.Rule{{
					Capability: agentpolicy.DatabaseInspect,
					Artifact:   agentpolicy.ClassMigrations,
					Verdict:    agentpolicy.VerdictAllow,
				}},
			},
			wantErr: `invocation policy: capability "database.inspect" is scoped by database class, not by artifact class`,
		},
		{
			name: "a scope on an unscoped capability",
			layer: agentpolicy.LayerRules{
				Layer: agentpolicy.LayerInvocation,
				Rules: []agentpolicy.Rule{{
					Capability: agentpolicy.SchemaRender,
					Artifact:   agentpolicy.ClassSchema,
					Verdict:    agentpolicy.VerdictAllow,
				}},
			},
			wantErr: `invocation policy: capability "schema.render" takes no scope`,
		},
		{
			name: "an unknown artifact class",
			layer: agentpolicy.LayerRules{
				Layer: agentpolicy.LayerInvocation,
				Rules: []agentpolicy.Rule{{
					Capability: agentpolicy.ArtifactWrite,
					Artifact:   agentpolicy.ArtifactClass("workflows"),
					Verdict:    agentpolicy.VerdictAllow,
				}},
			},
			wantErr: `invocation policy: unknown artifact class "workflows"`,
		},
		{
			name: "the builtin layer supplied from outside",
			layer: agentpolicy.LayerRules{
				Layer: agentpolicy.LayerBuiltin,
				Rules: []agentpolicy.Rule{{
					Capability: agentpolicy.ArtifactWrite,
					Artifact:   agentpolicy.ClassMigrations,
					Verdict:    agentpolicy.VerdictAllow,
				}},
			},
			wantErr: `layer "builtin" is this package's own; it cannot be supplied`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			policy, err := agentpolicy.Assemble(test.layer)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(policy, qt.IsNil)
		})
	}
}

func TestAssemble_HardDenialIsSentinel(t *testing.T) {
	c := qt.New(t)

	_, err := agentpolicy.Assemble(agentpolicy.LayerRules{
		Layer: agentpolicy.LayerInvocation,
		Rules: []agentpolicy.Rule{{
			Capability: agentpolicy.NetworkArbitrary,
			Verdict:    agentpolicy.VerdictAllow,
		}},
	})

	c.Assert(err, qt.ErrorIs, agentpolicy.ErrHardDenied)
}

func TestPolicyEntries_CarryTheDecidingLayer(t *testing.T) {
	c := qt.New(t)

	policy, err := agentpolicy.Assemble(agentpolicy.LayerRules{
		Layer:  agentpolicy.LayerInvocation,
		Source: "--allow-write=migrations",
		Rules: []agentpolicy.Rule{{
			Capability: agentpolicy.ArtifactWrite,
			Artifact:   agentpolicy.ClassMigrations,
			Verdict:    agentpolicy.VerdictAllow,
		}},
	})
	c.Assert(err, qt.IsNil)

	entries := policy.Entries()
	granted := agentpolicy.Entry{}
	untouched := agentpolicy.Entry{}
	for _, entry := range entries {
		granted = pick(granted, entry, agentpolicy.ArtifactWrite, agentpolicy.ClassMigrations)
		untouched = pick(untouched, entry, agentpolicy.ArtifactWrite, agentpolicy.ClassSchema)
	}

	c.Assert(granted.Verdict, qt.Equals, "allow")
	c.Assert(granted.DecidedBy, qt.Equals, "invocation")
	c.Assert(granted.Source, qt.Equals, "--allow-write=migrations")
	c.Assert(untouched.Verdict, qt.Equals, "ask")
	c.Assert(untouched.DecidedBy, qt.Equals, "builtin")
	c.Assert(untouched.Source, qt.Equals, "builtin")
}

// pick returns candidate when it is the entry for the named cell, and keeps the
// accumulated value otherwise. It is written without a conditional so the test
// body stays declarative.
func pick(
	kept, candidate agentpolicy.Entry,
	capability agentpolicy.Capability,
	class agentpolicy.ArtifactClass,
) agentpolicy.Entry {
	matches := map[bool]agentpolicy.Entry{true: candidate, false: kept}
	return matches[candidate.Capability == capability && candidate.Artifact == class]
}

func TestParseRule_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		line string
		want string
	}{
		{name: "unscoped", line: "schema.render allow", want: "schema.render allow"},
		{
			name: "artifact scoped",
			line: "artifact.write:migrations allow",
			want: "artifact.write:migrations allow",
		},
		{
			name: "database scoped",
			line: "database.inspect:production deny",
			want: "database.inspect:production deny",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rule, err := agentpolicy.ParseRule(test.line)

			c.Assert(err, qt.IsNil)
			c.Assert(rule.String(), qt.Equals, test.want)
		})
	}
}

func TestParseRule_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		wantErr string
	}{
		{
			name:    "no verdict",
			line:    "schema.render",
			wantErr: `invalid rule "schema.render": want "capability\[:scope\] verdict"`,
		},
		{
			name:    "unknown verdict",
			line:    "schema.render maybe",
			wantErr: `invalid rule "schema.render maybe": invalid verdict "maybe": want allow, ask or deny`,
		},
		{
			// Nothing is trimmed, for the reason internal/envbool states about
			// boolean environment values: a line that looks aligned in an
			// editor must not become a rule about something else.
			name:    "padded verdict",
			line:    "schema.render  allow",
			wantErr: `invalid rule "schema.render  allow": invalid verdict " allow": want allow, ask or deny`,
		},
		{
			name:    "unknown capability",
			line:    "schema.apply allow",
			wantErr: `invalid rule "schema.apply allow": unknown capability "schema.apply"`,
		},
		{
			name:    "scope on an unscoped capability",
			line:    "schema.render:schema allow",
			wantErr: `invalid rule "schema.render:schema allow": capability "schema.render" takes no scope`,
		},
		{
			name:    "unknown artifact class",
			line:    "artifact.write:workflows allow",
			wantErr: `invalid rule "artifact.write:workflows allow": unknown artifact class "workflows"`,
		},
		{
			name:    "unknown database class",
			line:    "database.inspect:staging allow",
			wantErr: `invalid rule "database.inspect:staging allow": unknown database class "staging"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rule, err := agentpolicy.ParseRule(test.line)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(rule, qt.Equals, agentpolicy.Rule{})
		})
	}
}

func TestDecide_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		req     agentpolicy.Request
		wantErr string
	}{
		{
			name:    "an artifact capability with no class",
			req:     agentpolicy.Request{Capability: agentpolicy.ArtifactWrite},
			wantErr: `capability "artifact.write" requires an artifact class`,
		},
		{
			name:    "a database capability with no class",
			req:     agentpolicy.Request{Capability: agentpolicy.DatabaseInspect},
			wantErr: `capability "database.inspect" requires a database class`,
		},
		{
			name: "an unknown capability",
			req: agentpolicy.Request{
				Capability: agentpolicy.Capability("migration.rewrite"),
			},
			wantErr: `unknown capability "migration.rewrite"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			policy, err := agentpolicy.Assemble()
			c.Assert(err, qt.IsNil)

			decision, err := policy.Decide(test.req)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(decision.Verdict.String(), qt.Equals, "deny")
		})
	}
}
