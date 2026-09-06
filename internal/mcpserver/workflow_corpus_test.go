package mcpserver_test

import (
	"maps"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"ptah.run/internal/agentpolicy"
	"ptah.run/migration/migrator"
)

// This file is #1490's workflow corpus: one case per task shape a person
// actually brings to an agent, driven through the served tools and asserted on
// the outcome rather than on the prose.
//
// The dialect corpus beside it varies the target and holds the task still. This
// varies the task. Both matter and neither substitutes: a dialect that stopped
// answering and a task shape that started succeeding when it should refuse are
// different failures, and only one of them is visible in a matrix of one task.
//
// Every expectation was read off the running server rather than predicted, and
// where a shape's outcome was not what it should be it is recorded as what
// happens, with the gap named.

// TestServer_ATaskTheServerAnswersInOneCall covers the shapes that are one
// question: a target the schema cannot be rendered for, a class the operator
// never configured, and a declaration that does not load.
func TestServer_ATaskTheServerAnswersInOneCall(t *testing.T) {
	tests := []struct {
		name string
		// schema is written into the project root before the call, empty when
		// the shape does not need one.
		schema string
		tool   string
		args   map[string]any
		// wantFailure is whether the task must be refused, which is the
		// decision each row carries.
		wantFailure bool
		// contains is what the answer must name: a refusal by its reason, an
		// answer by its content.
		contains string
	}{
		{
			name:        "a dialect this schema cannot be rendered for",
			schema:      corpusSchema,
			tool:        "render_schema",
			args:        map[string]any{"dialect": "clickhouse"},
			wantFailure: true,
			contains:    "clickhouse",
		},
		{
			name:        "an artifact class the operator did not configure",
			tool:        "read_artifact",
			args:        map[string]any{"artifact": "tests"},
			wantFailure: true,
			contains:    "artifact_class_not_configured",
		},
		{
			// Not a refusal, and deliberately so: "it does not parse for this
			// target" answers the question the caller asked, so it comes back
			// as a problem in the answer rather than as a failed call.
			name:        "a declaration that does not load",
			schema:      "package models\nthis is not go\n",
			tool:        "validate_schema",
			args:        map[string]any{"dialect": "postgres"},
			wantFailure: false,
			contains:    "source",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
			writeCorpusSchema(c, fixture.root, test.schema)
			session := connect(c, fixture.config, nil)
			// The source is added for the shapes that read a declaration and
			// withheld from the ones that do not: read_artifact declares no
			// such argument and refuses it as an unexpected property, which
			// would answer a different question than the row asks.
			args := sourceArgs(fixture.root, test.schema)
			maps.Copy(args, test.args)

			result := call(c, session, test.tool, args)

			c.Assert(result.IsError, qt.Equals, test.wantFailure,
				qt.Commentf("%s", textOf(c, result)))
			c.Assert(answerText(c, result), qt.Contains, test.contains)
		})
	}
}

// TestServer_AddingAMigrationPair is the ordinary task, end to end: read the
// digest, propose a pair against it, apply, and find it on disk.
func TestServer_AddingAMigrationPair(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	session := connect(c, fixture.config, nil)

	preview := previewPatch(c, session, migrationsDigest(c, session))
	result := call(c, session, "apply_patch", map[string]any{
		"preview_token": preview["preview_token"],
		"patch_id":      preview["patch_id"],
	})

	c.Assert(result.IsError, qt.IsFalse, qt.Commentf("%s", textOf(c, result)))
	_, err := os.Stat(fixture.dir + "/1700000100_add_status.up.sql")
	c.Assert(err, qt.IsNil)
}

// TestServer_AMigrationWithNoDownIsAcceptedAndThenUnrunnable records a gap
// rather than an endorsement.
//
// A model asked to add a column writes the up migration; stopping there is the
// ordinary way to get half a pair. The agent surface accepts it — both gates
// report ok and the patch applies — and the product then refuses the directory:
// migration_provider answers `incomplete migrations found (missing up or down
// files)` when it loads one. So the write succeeds and `ptah migrations up`
// meets it later, which is exactly the shape a gate exists to prevent.
//
// Both halves are asserted, so this is evidence rather than an opinion. When a
// gate learns this, the first half flips and this test fails: that is the
// intended signal, and the fix is to expect the refusal here
// (stokaro/ptah#1490).
func TestServer_AMigrationWithNoDownIsAcceptedAndThenUnrunnable(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	session := connect(c, fixture.config, nil)

	preview := call(c, session, "preview_patch", map[string]any{
		"artifact":        "migrations",
		"expected_digest": migrationsDigest(c, session),
		"summary":         "half a pair",
		"changes": []any{map[string]any{
			"path":      "1700000200_lonely.up.sql",
			"operation": "create",
			"content":   "ALTER TABLE users ADD COLUMN nickname TEXT;\n",
		}},
	})
	c.Assert(preview.IsError, qt.IsFalse, qt.Commentf("%s", textOf(c, preview)))
	proposal, isMap := preview.StructuredContent.(map[string]any)
	c.Assert(isMap, qt.IsTrue)

	applied := call(c, session, "apply_patch", map[string]any{
		"preview_token": proposal["preview_token"],
		"patch_id":      proposal["patch_id"],
	})

	c.Assert(applied.IsError, qt.IsFalse,
		qt.Commentf("the gate now refuses half a pair; flip this test to expect it"))
	_, loadErr := migrator.NewFSMigrationProvider(os.DirFS(fixture.dir))
	c.Assert(loadErr, qt.IsNotNil,
		qt.Commentf("the product accepted a directory with no down file; this test's premise is gone"))
	c.Assert(loadErr.Error(), qt.Contains, "incomplete migrations found")
}

// sourceArgs carries the schema source only for a shape that declares one.
func sourceArgs(root, schema string) map[string]any {
	if schema == "" {
		return make(map[string]any)
	}
	return map[string]any{"source": map[string]any{"root_dirs": []string{root}}}
}

// writeCorpusSchema puts a declaration in the project root, and nothing when a
// shape does not need one.
func writeCorpusSchema(c *qt.C, root, schema string) {
	c.Helper()
	if schema == "" {
		return
	}
	c.Assert(writeFile(root, "models.go", schema), qt.IsNil)
}

// answerText renders whichever half of a result carries the answer, so a row
// can assert over a refusal and over a structured answer with one expression.
func answerText(c *qt.C, result *mcp.CallToolResult) string {
	c.Helper()
	if structured, isMap := result.StructuredContent.(map[string]any); isMap {
		return asJSONText(c, structured)
	}
	return textOf(c, result)
}
