//go:build integration

package agentapi_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/agentapi"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/embedconnect"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/ydbrealm"
)

// The agent surface's inference tools against YDB, on both certified lines:
// they answer through the YDB backend, and what they answer carries no source
// content and no vector, for the reason the PostgreSQL tests above give.
func TestInferenceTools_AnswerWithoutTheSourceContentOnYDBLive(t *testing.T) {
	for _, engine := range []dbtarget.Engine{dbtarget.YDB, dbtarget.YDB251} {
		t.Run(engine.String(), func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			realmURL, release, err := ydbrealm.Enter(ctx, dbtarget.URL(c, engine))
			c.Assert(err, qt.IsNil)
			c.Cleanup(release)
			seedSecretYDBSource(c, ctx, realmURL)
			specPath, root := writeYDBInferenceSpec(c)
			runID := seedYDBRun(c, ctx, realmURL)
			session := liveInferenceSession(c, realmURL, root)

			plan, err := session.InferencePlan(ctx, agentapi.InferencePlanRequest{Spec: specPath})
			c.Assert(err, qt.IsNil)
			c.Assert(plan.Plan.Desired, qt.Not(qt.Equals), "")
			c.Assert(plan.Plan.Steps, qt.Not(qt.HasLen), 0)
			c.Assert(plan.Plan.Disclosure.Fields, qt.DeepEquals, []string{"title", "body"})
			c.Assert(plan.Plan.Disclosure.RowsInScope, qt.Equals, int64(1))
			assertCarriesNoContent(c, plan)

			status, err := session.InferenceStatus(ctx, agentapi.InferenceStatusRequest{RunID: runID})
			c.Assert(err, qt.IsNil)
			c.Assert(status.Status.RunID, qt.Equals, runID)
			c.Assert(status.Status.Progress.RowsEmbedded, qt.Equals, int64(41))
			assertCarriesNoContent(c, status)
		})
	}
}

// seedSecretYDBSource creates a YDB table holding one row of the distinctive
// text and one stored vector whose first component is the value the absence
// assertion looks for.
func seedSecretYDBSource(c *qt.C, ctx context.Context, dbURL string) {
	c.Helper()
	db, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(db)
	for _, statement := range []string{
		"CREATE TABLE articles (id Utf8 NOT NULL, title Utf8, body Utf8, updated_at Timestamp, embedding String, " +
			"PRIMARY KEY (id))",
		fmt.Sprintf("UPSERT INTO articles (id, title, body, updated_at, embedding) VALUES ('row-key-7', %q, %q, "+
			"CurrentUtcTimestamp(), Untag(Knn::ToBinaryStringFloat([0.125f, 0.25f, 0.375f, 0.5f]), \"FloatVector\"))",
			theSecret, theSecret),
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// seedYDBRun writes a run whose cursor holds a source key value, through the
// YDB backend's own store.
func seedYDBRun(c *qt.C, ctx context.Context, dbURL string) string {
	c.Helper()
	backend, err := embedconnect.Open(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = backend.Close() }()
	c.Assert(backend.EnsureSchema(ctx, must.Must(builtin.New())), qt.IsNil)
	runID := fmt.Sprintf("agent-run-%d", time.Now().UnixNano())
	c.Assert(backend.Store().CreateRun(ctx, embedrun.Run{
		ID: runID, SpecDigest: "spec-1", GenerationIdentity: "gen-1",
		Environment: "test", Source: "articles", Target: "articles.embedding",
		Phase: embedrun.PhaseBackfilling, Status: embedrun.StatusRunning,
		Cursor:    []string{"row-key-7"},
		Progress:  embedrun.Progress{RowsScanned: 41, RowsEmbedded: 41},
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}), qt.IsNil)
	return runID
}

// writeYDBInferenceSpec writes a YDB specification for that table.
func writeYDBInferenceSpec(c *qt.C) (specPath, dir string) {
	c.Helper()
	root := c.TempDir()
	document := `
version: 1
name: agent surface on YDB
source:
  table: articles
  key_fields: [id]
  input_fields: [title, body]
  version_strategy: updated_at
  version_field: updated_at
  mutable: false
preprocessing:
  separator: "\n"
  null_policy: empty
  empty_policy: skip
  unicode_normalization: none
  truncate: refuse
model:
  provider: openai-compatible
  endpoint_class: local
  endpoint: http://127.0.0.1:9/v1
  identifier: test-embed
  revision: "1"
  reported_dimension: 4
  normalization: none
target:
  table: articles
  column: embedding_v2
  representation: float
  metric: cosine
consistency:
  mode: immutable
policy:
  require_exact_approval: true
  require_consistency_mode: true
`
	path := filepath.Join(root, "spec.yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path, root
}
