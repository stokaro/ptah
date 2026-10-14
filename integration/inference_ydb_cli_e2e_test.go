//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/ydbrealm"
)

// inferenceYDBLine is one certified YDB line the lifecycle runs against.
type inferenceYDBLine struct {
	// name is the release line.
	name string
	// engine is where the server's address comes from.
	engine dbtarget.Engine
	// maintainedOnWrite is what the line's capabilities say about a vector
	// index taking in rows written after its build.
	maintainedOnWrite bool
}

// inferenceYDBLines are the lines the integration workflow starts: 26.2, the
// current release, and 25.1, the line with a published support date, started
// with EnableVectorIndex on. The two differ where it matters here: 25.1's
// vector index answers as it stood at its build.
var inferenceYDBLines = []inferenceYDBLine{
	{name: "26.2", engine: dbtarget.YDB, maintainedOnWrite: true},
	{name: "25.1", engine: dbtarget.YDB251, maintainedOnWrite: false},
}

// TestInferenceYDBCLIE2E drives an embedding migration on YDB through the
// command line an operator runs: plan, prepare, backfill, index, verify,
// evaluate and cutover for one generation, then a second generation cut over
// with a stabilization window, a rollback to the first, and the retirement of
// the second. Each line runs in a dev realm of its own, so the run state, the
// source and the vectors are all inside one directory the test removes.
//
// It fails where YDB is refused before it connects, which is how inference on
// YDB stood before this backend (stokaro/ptah#4181).
func TestInferenceYDBCLIE2E(t *testing.T) {
	for _, line := range inferenceYDBLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			dbURL := enterYDBRealm(c, ctx, line)
			db := openYDBRealm(c, ctx, dbURL)
			seedYDBArticles(c, ctx, db)

			endpoint := httptest.NewServer(http.HandlerFunc(topicEmbeddingsHandler(c)))
			defer endpoint.Close()
			first := writeYDBSpec(c, ydbSpec{endpoint: endpoint.URL, column: "embedding_a", revision: "1"})
			second := writeYDBSpec(c, ydbSpec{endpoint: endpoint.URL, column: "embedding_b", revision: "2"})

			assertYDBPlanSaysWhatItMeasured(c, ctx, first, dbURL)
			assertYDBPrepareIsIdempotent(c, ctx, first, dbURL)
			buildYDBGeneration(c, ctx, first, dbURL, "ydb-first")
			assertYDBEvaluationFindsTheCorpus(c, ctx, first, dbURL)
			firstGeneration := activeGenerationOfRun(c, ctx, first, dbURL, "ydb-first")
			assertYDBCutoverBindsToItsPlan(c, ctx, first, dbURL, "ydb-first", "")

			buildYDBGeneration(c, ctx, second, dbURL, "ydb-second")
			secondGeneration := activeGenerationOfRun(c, ctx, second, dbURL, "ydb-second")
			assertYDBCutoverBindsToItsPlan(c, ctx, second, dbURL, "ydb-second", "1h")
			assertYDBRollbackReturnsToTheFirst(c, ctx, second, dbURL, firstGeneration)
			assertYDBRetirementDropsTheSecond(c, ctx, db, second, dbURL, secondGeneration)
			assertYDBWritesAfterTheIndexFollowTheLine(c, ctx, db, first, dbURL, line)
		})
	}
}

// TestInferenceYDBRefusalsE2E is what a YDB database cannot run, each refused
// by name before anything is created: the outbox mode, which needs row
// triggers, and a pgvector representation.
func TestInferenceYDBRefusalsE2E(t *testing.T) {
	for _, line := range inferenceYDBLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			dbURL := enterYDBRealm(c, ctx, line)
			db := openYDBRealm(c, ctx, dbURL)
			seedYDBArticles(c, ctx, db)
			endpoint := httptest.NewServer(http.HandlerFunc(topicEmbeddingsHandler(c)))
			defer endpoint.Close()

			outbox := writeYDBSpec(c, ydbSpec{endpoint: endpoint.URL, column: "embedding", revision: "1", mode: "outbox"})
			plan, err := runInferenceExpectingFailure(c, ctx, "plan", "--spec", outbox, "--db-url", dbURL)
			c.Assert(err, qt.IsNotNil)
			c.Assert(plan, qt.Contains, "has no triggers")
			c.Assert(plan, qt.Contains, "use consistency mode immutable")

			prepare, err := runInferenceExpectingFailure(c, ctx, "prepare",
				"--spec", outbox, "--db-url", dbURL, "--run-id", "ydb-outbox")
			c.Assert(err, qt.ErrorMatches, `.*has no triggers, and the outbox consistency mode .*`)
			c.Assert(prepare, qt.Not(qt.Contains), "prepared run")

			pgvector := writeYDBSpec(c, ydbSpec{
				endpoint: endpoint.URL, column: "embedding", revision: "1", representation: "vector",
			})
			_, err = runInferenceExpectingFailure(c, ctx, "prepare",
				"--spec", pgvector, "--db-url", dbURL, "--run-id", "ydb-pgvector")
			c.Assert(err, qt.ErrorMatches, `.*representation "vector" is not one a YDB generation stores.*`)

			// Neither refusal created anything: the source is the only table.
			names, err := tableNamesInRealm(ctx, db)
			c.Assert(err, qt.IsNil)
			c.Assert(names, qt.DeepEquals, []string{"articles"})
		})
	}
}

// enterYDBRealm gives the test a dev realm of its own on the line's server and
// removes it, with everything in it, when the test ends.
func enterYDBRealm(c *qt.C, ctx context.Context, line inferenceYDBLine) string {
	c.Helper()
	realmURL, release, err := ydbrealm.Enter(ctx, dbtarget.URL(c, line.engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(release)
	return realmURL
}

// openYDBRealm connects to the realm, and asserts the line the connection
// reached is the one the test names: a second local-ydb behind a remapped
// port answers for the first, and every assertion would then measure 26.2
// under 25.1's name.
func openYDBRealm(c *qt.C, ctx context.Context, dbURL string) *dbschema.DatabaseConnection {
	c.Helper()
	db, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(db) })
	return db
}

// seedYDBArticles creates the source table and five documents, each a distinct
// mixture of the topics the fake provider embeds along. The key is the table's
// primary key, which is what a YDB generation reads and writes by.
func seedYDBArticles(c *qt.C, ctx context.Context, db *dbschema.DatabaseConnection) {
	c.Helper()
	statements := []string{
		"CREATE TABLE articles (id Int64 NOT NULL, title Utf8, body Utf8, updated_at Timestamp, PRIMARY KEY (id))",
		`UPSERT INTO articles (id, title, body, updated_at) VALUES
			(1, 'Pricing', 'pricing pricing pricing support', Timestamp('2026-10-01T00:00:00Z')),
			(2, 'Support', 'support support support pricing', Timestamp('2026-10-01T00:00:00Z')),
			(3, 'Billing', 'billing billing billing support', Timestamp('2026-10-01T00:00:00Z')),
			(4, 'Renewals', 'renewal renewal renewal billing', Timestamp('2026-10-01T00:00:00Z')),
			(5, 'Costs', 'pricing pricing support support', Timestamp('2026-10-01T00:00:00Z'))`,
	}
	for _, statement := range statements {
		c.Assert(execAfterSchemaSettles(ctx, db, statement), qt.IsNil, qt.Commentf("%s", statement))
	}
}

// execAfterSchemaSettles runs a statement, again for a short while when the
// server answers that its scheme is not ready: local-ydb refuses DDL for a
// moment after it starts answering queries, and a realm that was just created
// is such a moment.
func execAfterSchemaSettles(ctx context.Context, db *dbschema.DatabaseConnection, statement string) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := db.ExecContext(ctx, statement)
		if err == nil || time.Now().After(deadline) || !strings.Contains(err.Error(), "UNAVAILABLE") {
			return err
		}
		time.Sleep(time.Second)
	}
}

// tableNamesInRealm lists the row tables at the realm's root.
func tableNamesInRealm(ctx context.Context, db *dbschema.DatabaseConnection) ([]string, error) {
	read, err := db.Reader().ReadSchemaContext(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(read.Tables))
	for _, table := range read.Tables {
		names = append(names, table.Name)
	}
	return names, nil
}

// columnsOfArticles lists the realm's articles table's columns.
func columnsOfArticles(ctx context.Context, db *dbschema.DatabaseConnection) ([]string, error) {
	read, err := db.Reader().ReadSchemaContext(ctx)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, table := range read.Tables {
		for _, column := range table.Columns {
			names = append(names, table.Name+"."+column.Name)
		}
	}
	return names, nil
}

// ydbSpec is what a YDB fixture specification varies.
type ydbSpec struct {
	endpoint       string
	column         string
	revision       string
	mode           string
	representation string
}

// writeYDBSpec writes a specification for the realm's articles table.
func writeYDBSpec(c *qt.C, spec ydbSpec) string {
	c.Helper()
	mode := spec.mode
	if mode == "" {
		mode = "immutable"
	}
	representation := spec.representation
	if representation == "" {
		representation = "float"
	}
	document := fmt.Sprintf(`
version: 1
name: ydb articles
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
  endpoint: %s/v1
  identifier: test-embed
  revision: "%s"
  reported_dimension: 4
  normalization: none
target:
  table: articles
  column: %s
  representation: %s
  metric: cosine
  index_method: vector_kmeans_tree
  index_options:
    levels: "1"
    clusters: "2"
consistency:
  mode: %s
policy:
  require_exact_approval: true
  require_consistency_mode: true
`, spec.endpoint, spec.revision, spec.column, representation, mode)
	path := filepath.Join(c.TempDir(), "spec-"+spec.column+"-"+mode+".yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// assertYDBPlanSaysWhatItMeasured is the read-only verb against YDB: the rows
// are counted, the vector type and index are the line's capabilities, and the
// column is not there yet.
func assertYDBPlanSaysWhatItMeasured(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()
	output := runInference(c, ctx, "plan", "--spec", specPath, "--db-url", dbURL)

	c.Assert(output, qt.Contains, "source.estimated_rows = 5 (measured)")
	c.Assert(output, qt.Contains, "target.capability.vector_type = true (measured)")
	c.Assert(output, qt.Contains, "target.capability.vector_index = true (measured)")
	c.Assert(output, qt.Contains, "target.exists = false (measured)")
	c.Assert(output, qt.Contains, "Consistency mode: immutable")
}

// assertYDBPrepareIsIdempotent prepares the first run twice: the second finds
// it prepared and leaves it.
func assertYDBPrepareIsIdempotent(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()
	first := runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbURL, "--run-id", "ydb-first")
	c.Assert(first, qt.Contains, "prepared run ydb-first for generation ")
	again := runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbURL, "--run-id", "ydb-first")
	c.Assert(again, qt.Contains, "run ydb-first already exists; leaving it as it is")
}

// buildYDBGeneration takes a run from prepare to a passing verification: the
// backfill embeds every row, the index is built over them, and every
// deterministic layer passes against what YDB holds.
func buildYDBGeneration(c *qt.C, ctx context.Context, specPath, dbURL, runID string) {
	c.Helper()
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	backfill := runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID, "--batch-rows", "2")
	c.Assert(backfill, qt.Contains, "backfill finished: 5 scanned, 5 embedded, 0 skipped")

	index := runInference(c, ctx, "index", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	c.Assert(index, qt.Contains, "has a valid index")
	again := runInference(c, ctx, "index", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	c.Assert(again, qt.Contains, "already has a valid index")

	verify := runInference(c, ctx, "verify", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	c.Assert(verify, qt.Contains, "every deterministic layer passed")
	c.Assert(verify, qt.Contains, "5 source rows, 5 target rows")

	status := runInference(c, ctx, "status", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	c.Assert(status, qt.Contains, "run "+runID+": verified, running")
	c.Assert(status, qt.Contains, "verified: true, cutover ready: true")
}

// assertYDBEvaluationFindsTheCorpus searches the generation through its
// vector_kmeans_tree index and exhaustively, and both find what the corpus
// says they should.
func assertYDBEvaluationFindsTheCorpus(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()
	corpus := writeCorpus(c, `
version: 1
name: topics
default_k: 2
cases:
  - id: pricing
    query: pricing
    required: ["1"]
    relevant: {"1": 3}
  - id: support
    query: support
    required: ["2"]
    relevant: {"2": 3}
  - id: billing
    query: billing
    required: ["3"]
    relevant: {"3": 3}
`)
	output := runInference(c, ctx, "evaluate",
		"--spec", specPath, "--db-url", dbURL, "--corpus", corpus,
		"--min-recall", "1.0", "--min-exact-agreement", "1.0")

	c.Assert(output, qt.Contains, "recall 1.000, MRR 1.000, NDCG 1.000 over 3 cases")
	c.Assert(output, qt.Contains, "exhaustive search on 1.000 of results, over 3 cases")
	c.Assert(output, qt.Contains, "measured under ydb.KMeansTreeSearchTopSize=")
}

// assertYDBCutoverBindsToItsPlan is the approval flow: a cutover without an
// approval is refused and publishes the plan digest, and the same cutover
// approved for that digest moves the pointer.
func assertYDBCutoverBindsToItsPlan(c *qt.C, ctx context.Context, specPath, dbURL, runID, window string) {
	c.Helper()
	unapproved, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	c.Assert(err, qt.IsNotNil)
	c.Assert(unapproved, qt.Contains, "this policy requires an approval and none was given")

	digest := planDigestFrom(c, unapproved)
	args := []string{"cutover", "--spec", specPath, "--db-url", dbURL, "--run-id", runID,
		"--approve", digest, "--approver", "an operator"}
	if window != "" {
		args = append(args, "--stabilize-for", window)
	}
	approved := runInference(c, ctx, args...)
	c.Assert(approved, qt.Contains, "queries now read generation ")
	c.Assert(approved, qt.Contains, "(plan "+digest+")")
}

// assertYDBRollbackReturnsToTheFirst goes back through the stabilization
// window the second cutover opened over the first generation.
func assertYDBRollbackReturnsToTheFirst(c *qt.C, ctx context.Context, specPath, dbURL, first string) {
	c.Helper()
	output := runInference(c, ctx, "rollback", "--spec", specPath, "--db-url", dbURL, "--to", first)
	c.Assert(output, qt.Contains, "queries now read "+first)
}

// assertYDBRetirementDropsTheSecond retires the generation the rollback
// displaced: approved for its digest, it drops the index and the columns, and
// the run that built it is complete.
func assertYDBRetirementDropsTheSecond(
	c *qt.C, ctx context.Context, db *dbschema.DatabaseConnection, specPath, dbURL, generation string,
) {
	c.Helper()
	refused, err := runInferenceExpectingFailure(c, ctx, "retire",
		"--spec", specPath, "--db-url", dbURL, "--generation", generation, "--drop-column")
	c.Assert(err, qt.IsNotNil)
	digest := planDigestFrom(c, refused)

	output := runInference(c, ctx, "retire",
		"--spec", specPath, "--db-url", dbURL, "--generation", generation, "--drop-column",
		"--approve", digest, "--approver", "an operator")
	c.Assert(output, qt.Contains, "generation "+generation+" is gone, with 5 vectors")

	columns, err := columnsOfArticles(ctx, db)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(columns, ","), qt.Not(qt.Contains), "articles.embedding_b")
	c.Assert(strings.Join(columns, ","), qt.Contains, "articles.embedding_a")

	status := runInference(c, ctx, "status", "--spec", specPath, "--db-url", dbURL, "--run-id", "ydb-second")
	c.Assert(status, qt.Contains, "run ydb-second: retired, complete")
}

// assertYDBWritesAfterTheIndexFollowTheLine is the behavior of a write into a
// generation whose index exists, which differs by line.
//
// A source row changes and a new run of the same generation embeds it again.
// On a line whose vector index takes in later writes the write lands and the
// index answers with it. On 25.1, whose index answers as it stood at its build,
// the write is refused, naming the capability and the line, and nothing is
// written: a corpus the index no longer describes would answer queries
// confidently and wrongly.
func assertYDBWritesAfterTheIndexFollowTheLine(
	c *qt.C, ctx context.Context, db *dbschema.DatabaseConnection, specPath, dbURL string, line inferenceYDBLine,
) {
	c.Helper()
	_, err := db.ExecContext(ctx, `UPDATE articles SET body = 'renewal renewal renewal renewal',
		updated_at = Timestamp('2026-10-02T00:00:00Z') WHERE id = 1`)
	c.Assert(err, qt.IsNil)
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbURL, "--run-id", "ydb-rewrite")
	output, err := runInferenceExpectingFailure(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbURL, "--run-id", "ydb-rewrite", "--batch-rows", "10")

	c.Assert(db.Info().Capabilities.Has(capability.VectorIndexMaintainedOnWrite), qt.Equals, line.maintainedOnWrite)
	c.Assert(err == nil, qt.Equals, line.maintainedOnWrite, qt.Commentf("%s", output))
	c.Assert(strings.Contains(output, "backfill finished: 5 scanned, 5 embedded"), qt.Equals, line.maintainedOnWrite,
		qt.Commentf("%s", output))
	c.Assert(strings.Contains(fmt.Sprint(err), "has no vector_index_maintained_on_write"), qt.Equals,
		!line.maintainedOnWrite, qt.Commentf("%v", err))
}
