//go:build integration

package providercli_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerhost"
)

// inferenceFixture is a database the fixture's embedding store serves, the
// manifest that selects the fixture, and an embedding endpoint.
type inferenceFixture struct {
	cli      providerCLI
	endpoint string
	// spec is the fixture build the manifest selects, for a test that writes
	// a manifest of its own for it.
	spec providerhost.Spec
	// refuse makes the endpoint refuse any input that mentions billing, so a
	// backfill stops at the batch that holds one.
	refuse *atomic.Bool
}

// newInferenceFixture builds the fixture that serves the embedding store and
// seeds five articles, each a distinct mixture of the topics the endpoint
// embeds along.
func newInferenceFixture(c *qt.C) inferenceFixture {
	c.Helper()
	return newInferenceFixtureOf(c, "embeddings")
}

// newInferenceFixtureOf is newInferenceFixture with another program of the
// fixture serving the database.
func newInferenceFixtureOf(c *qt.C, program string) inferenceFixture {
	c.Helper()
	spec := fixtureSpec(c, program)
	f := inferenceFixture{cli: newProviderCLI(c, spec), spec: spec, refuse: &atomic.Bool{}}
	db, err := sql.Open("sqlite", f.cli.databasePath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	for _, statement := range []string{
		`CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, body TEXT, updated_at TEXT)`,
		`INSERT INTO articles VALUES
			(1, 'Pricing', 'pricing pricing pricing support', '2026-10-01T00:00:00Z'),
			(2, 'Support', 'support support support pricing', '2026-10-01T00:00:00Z'),
			(3, 'Billing', 'billing billing billing support', '2026-10-01T00:00:00Z'),
			(4, 'Renewals', 'renewal renewal renewal billing', '2026-10-01T00:00:00Z'),
			(5, 'Costs', 'pricing pricing support support', '2026-10-01T00:00:00Z')`,
	} {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	endpoint := httptest.NewServer(topicEndpoint(c, f.refuse))
	c.Cleanup(endpoint.Close)
	f.endpoint = endpoint.URL
	return f
}

// inferenceTopics are the axes the endpoint embeds along.
var inferenceTopics = []string{"pricing", "support", "billing", "renewal"}

// topicEndpoint answers an OpenAI-compatible embedding request with a vector
// that counts each topic word in the input, so cosine distance separates the
// articles and a query about a topic lands beside the article about it.
func topicEndpoint(c *qt.C, refuse *atomic.Bool) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		c.Assert(json.NewDecoder(request.Body).Decode(&body), qt.IsNil)
		type entry struct {
			Index     int       `json:"index"`
			Embedding []float32 `json:"embedding"`
		}
		answer := struct {
			Data  []entry `json:"data"`
			Model string  `json:"model"`
		}{Model: body.Model}
		for index, input := range body.Input {
			if refuse.Load() && strings.Contains(input, "billing") {
				http.Error(writer, "refused for the test", http.StatusBadRequest)
				return
			}
			vector := make([]float32, len(inferenceTopics))
			for axis, topic := range inferenceTopics {
				vector[axis] = float32(strings.Count(input, topic))
			}
			answer.Data = append(answer.Data, entry{Index: index, Embedding: vector})
		}
		writer.Header().Set("Content-Type", "application/json")
		c.Assert(json.NewEncoder(writer).Encode(answer), qt.IsNil)
	}
}

// inferenceSpec is what a fixture specification varies.
type inferenceSpec struct {
	column, revision, mode string
	// index declares an index method, which the fixture refuses.
	index bool
	// ownTable names a table of the generation's own, and the source rows are
	// split into chunks.
	ownTable string
}

// writeSpec writes a specification for the articles table.
func (f inferenceFixture) writeSpec(c *qt.C, spec inferenceSpec) string {
	c.Helper()
	mode := spec.mode
	if mode == "" {
		mode = "immutable"
	}
	target, layout, truncate, chunking, index := "articles", "", "refuse", "", ""
	if spec.index {
		index = "  index_method: hnsw\n"
	}
	if spec.ownTable != "" {
		target, layout, truncate = spec.ownTable, "  layout: own_table\n", "chunk"
		chunking = "  max_input_bytes: 16\n  overlap_bytes: 4\n"
	}
	document := fmt.Sprintf(`
version: 1
name: fixture articles
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
  truncate: %s
%smodel:
  provider: openai-compatible
  endpoint_class: local
  endpoint: %s/v1
  identifier: test-embed
  revision: "%s"
  reported_dimension: 4
  normalization: none
target:
  table: %s
  column: %s
  representation: float
  metric: cosine
%s%sconsistency:
  mode: %s
policy:
  require_exact_approval: true
  require_consistency_mode: true
`, truncate, chunking, f.endpoint, spec.revision, target, spec.column, index, layout, mode)
	path := filepath.Join(c.TempDir(), "spec-"+spec.column+".yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// run runs one inference verb against the fixture's database, through the
// provider the manifest selects, and checks the provider process is gone
// when the verb returns. It answers what the verb printed and its exit code.
func (f inferenceFixture) run(c *qt.C, verb, spec string, args ...string) (string, int) {
	c.Helper()
	arguments := append([]string{"inference", verb, "--provider-config", f.cli.manifestPath, "--spec", spec}, args...)
	if verb != "describe" {
		arguments = append(arguments, "--db-url", f.cli.address)
	}
	out, diagnostics, code := executeProviderCLI(c.Context(), arguments)
	f.cli.assertStopped(c)
	return out + diagnostics, code
}

// succeed runs a verb and requires it to succeed.
func (f inferenceFixture) succeed(c *qt.C, verb, spec string, args ...string) string {
	c.Helper()
	output, code := f.run(c, verb, spec, args...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", output))
	return output
}

// planDigest reads the short plan digest a refusal printed.
func planDigest(c *qt.C, output string) string {
	c.Helper()
	for line := range strings.SplitSeq(output, "\n") {
		if after, found := strings.CutPrefix(line, "plan "); found {
			return strings.Fields(after)[0]
		}
	}
	c.Fatalf("no plan digest in:\n%s", output)
	return ""
}

// generationOf reads the generation a run built from its status.
func (f inferenceFixture) generationOf(c *qt.C, spec, runID string) string {
	c.Helper()
	for line := range strings.SplitSeq(f.succeed(c, "status", spec, "--run-id", runID), "\n") {
		if after, found := strings.CutPrefix(strings.TrimSpace(line), "- generation: "); found {
			return strings.TrimSpace(after)
		}
	}
	c.Fatalf("no generation in the status of run %s", runID)
	return ""
}

// approved runs a verb that needs an approval: once without one, which is
// refused and prints the plan digest, and again approved for that digest.
func (f inferenceFixture) approved(c *qt.C, verb, spec string, args ...string) string {
	c.Helper()
	refused, code := f.run(c, verb, spec, args...)
	c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s", refused))
	return f.succeed(c, verb, spec, append(args, "--approve", planDigest(c, refused), "--approver", "an operator")...)
}

// build takes a run from prepare to a passing verification.
func (f inferenceFixture) build(c *qt.C, spec, runID string) {
	c.Helper()
	f.succeed(c, "prepare", spec, "--run-id", runID)
	backfill := f.succeed(c, "backfill", spec, "--run-id", runID, "--batch-rows", "2")
	c.Assert(backfill, qt.Contains, "backfill finished: 5 scanned, 5 embedded, 0 skipped")
	c.Assert(f.succeed(c, "index", spec, "--run-id", runID), qt.Contains, "declares no index method")
	verify := f.succeed(c, "verify", spec, "--run-id", runID)
	c.Assert(verify, qt.Contains, "every deterministic layer passed")
	c.Assert(verify, qt.Contains, "5 source rows, 5 target rows")
}

// exec runs a statement on the database file directly, as an application
// would beside Ptah.
func (f inferenceFixture) exec(c *qt.C, statement string) {
	c.Helper()
	db, err := sql.Open("sqlite", f.cli.databasePath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	_, err = db.ExecContext(c.Context(), statement)
	c.Assert(err, qt.IsNil)
}

// columns lists a table's columns, read from the file directly.
func (f inferenceFixture) columns(c *qt.C, table string) []string {
	c.Helper()
	db, err := sql.Open("sqlite", f.cli.databasePath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), "SELECT name FROM pragma_table_info(?) ORDER BY cid", table)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// TestInferenceOnAProviderEngineE2E drives an embedding migration on an
// engine only a provider serves, through the command line an operator runs:
// describe and plan, prepare twice, a backfill that stops at a refused batch
// and resumes after its checkpoint, an index the fixture does not declare,
// verify, an exhaustive evaluation, and cutover. A second generation is cut
// over with a stabilization window, rolled back from, and retired with its
// columns. The provider keeps the run state and the vectors; every decision
// is the host's.
func TestInferenceOnAProviderEngineE2E(t *testing.T) {
	c := qt.New(t)
	f := newInferenceFixture(c)
	first := f.writeSpec(c, inferenceSpec{column: "embedding_a", revision: "1"})
	second := f.writeSpec(c, inferenceSpec{column: "embedding_b", revision: "2"})

	c.Assert(f.succeed(c, "describe", first), qt.Contains, "target: articles.embedding_a BLOB")
	// The plan holds every database to a vector index, and the fixture builds
	// none, so it measures the source and the capabilities and is blocked.
	plan, code := f.run(c, "plan", first)
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(plan, qt.Contains, "source.estimated_rows = 5 (measured)")
	c.Assert(plan, qt.Contains, "target.capability.vector_type = true (measured)")
	c.Assert(plan, qt.Contains, "target.capability.vector_index = unsupported")
	c.Assert(plan, qt.Contains, "target.exists = false (measured)")

	c.Assert(f.succeed(c, "prepare", first, "--run-id", "first"), qt.Contains, "prepared run first for generation ")
	c.Assert(f.succeed(c, "prepare", first, "--run-id", "first"), qt.Contains, "run first already exists; leaving it as it is")
	f.refuse.Store(true)
	stopped, code := f.run(c, "backfill", first, "--run-id", "first", "--batch-rows", "2")
	c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s", stopped))
	f.refuse.Store(false)
	resumed := f.succeed(c, "backfill", first, "--run-id", "first", "--batch-rows", "2")
	c.Assert(resumed, qt.Contains, "backfill finished: 3 scanned, 3 embedded, 0 skipped")
	c.Assert(f.succeed(c, "index", first, "--run-id", "first"), qt.Contains, "declares no index method")
	verify := f.succeed(c, "verify", first, "--run-id", "first")
	c.Assert(verify, qt.Contains, "every deterministic layer passed")
	c.Assert(verify, qt.Contains, "5 source rows, 5 target rows")

	corpus := filepath.Join(c.TempDir(), "corpus.yaml")
	c.Assert(os.WriteFile(corpus, []byte(`
version: 1
name: topics
default_k: 2
cases:
  - id: pricing
    query: pricing
    required: ["1"]
    relevant: {"1": 3, "5": 2}
  - id: billing
    query: billing
    required: ["3"]
    relevant: {"3": 3, "4": 1}
`), 0o600), qt.IsNil)
	evaluated := f.succeed(c, "evaluate", first, "--corpus", corpus, "--min-recall", "1.0", "--min-exact-agreement", "1.0")
	c.Assert(evaluated, qt.Contains, "recall 1.000")
	c.Assert(evaluated, qt.Contains, "the index agrees with an exhaustive search on 1.000 of results, over 2 cases")
	c.Assert(evaluated, qt.Contains, "measured under fixture.search=exhaustive")

	firstGeneration := f.generationOf(c, first, "first")
	c.Assert(f.approved(c, "cutover", first, "--run-id", "first"), qt.Contains, "queries now read generation ")

	f.build(c, second, "second")
	secondGeneration := f.generationOf(c, second, "second")
	c.Assert(f.approved(c, "cutover", second, "--run-id", "second", "--stabilize-for", "1h"), qt.Contains, "queries now read generation ")
	c.Assert(f.succeed(c, "rollback", second, "--to", firstGeneration), qt.Contains, "queries now read "+firstGeneration)

	retired := f.approved(c, "retire", second, "--generation", secondGeneration, "--drop-column")
	c.Assert(retired, qt.Contains, "generation "+secondGeneration+" is gone, with 5 vectors")
	c.Assert(strings.Join(f.columns(c, "articles"), ","), qt.Not(qt.Contains), "embedding_b")
	c.Assert(strings.Join(f.columns(c, "articles"), ","), qt.Contains, "embedding_a")
	c.Assert(f.succeed(c, "status", second, "--run-id", "second"), qt.Contains, "run second: retired, complete")
}

// TestInferenceOnAProviderEngineOwnTableE2E is a generation in a table of its
// own, with each source row split into a set of chunks: prepare creates the
// table and marks it as the generation's, verification walks both tables a
// page at a time, and retirement drops the table because the mark names this
// generation.
func TestInferenceOnAProviderEngineOwnTableE2E(t *testing.T) {
	c := qt.New(t)
	f := newInferenceFixture(c)
	spec := f.writeSpec(c, inferenceSpec{column: "embedding", revision: "1", ownTable: "article_chunks"})

	f.succeed(c, "prepare", spec, "--run-id", "chunks")
	c.Assert(f.columns(c, "article_chunks"), qt.DeepEquals,
		[]string{"id", "embedding_chunk_ordinal", "embedding", "embedding_generation", "embedding_input_hash", "embedding_source_version", "embedding_state"})
	c.Assert(f.succeed(c, "backfill", spec, "--run-id", "chunks", "--batch-rows", "2"), qt.Contains, "backfill finished: ")
	f.succeed(c, "index", spec, "--run-id", "chunks")
	verify := f.succeed(c, "verify", spec, "--run-id", "chunks")
	c.Assert(verify, qt.Contains, "every deterministic layer passed")
	c.Assert(verify, qt.Contains, "5 source rows, ")

	generation := f.generationOf(c, spec, "chunks")
	retired := f.approved(c, "retire", spec, "--generation", generation)
	c.Assert(retired, qt.Contains, "and the table article_chunks they were in")
	c.Assert(f.columns(c, "article_chunks"), qt.HasLen, 0)
	c.Assert(f.columns(c, "articles"), qt.DeepEquals, []string{"id", "title", "body", "updated_at"})
}

// TestInferenceOnAProviderEngineRefusalsE2E is what the fixture's engine
// cannot run, each refused by name before anything is created: the outbox
// mode, which the engine does not declare, and an index method, which the
// fixture does not build.
func TestInferenceOnAProviderEngineRefusalsE2E(t *testing.T) {
	c := qt.New(t)
	f := newInferenceFixture(c)
	outbox := f.writeSpec(c, inferenceSpec{column: "embedding", revision: "1", mode: "outbox"})
	indexed := f.writeSpec(c, inferenceSpec{column: "embedding_i", revision: "1", index: true})

	refusedMode, code := f.run(c, "prepare", outbox, "--run-id", "outbox")
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(refusedMode, qt.Contains, `engine "fixture" builds generations in consistency mode immutable, and the specification selects "outbox"`)
	refusedIndex, code := f.run(c, "prepare", indexed, "--run-id", "indexed")
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(refusedIndex, qt.Contains, "the fixture builds no vector index")
	described, code := f.run(c, "describe", indexed)
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(described, qt.Contains, "the fixture builds no vector index")

	// Neither refusal created anything: the source is the only table.
	db, err := sql.Open("sqlite", f.cli.databasePath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var tables int
	c.Assert(db.QueryRowContext(c.Context(), "SELECT count(*) FROM sqlite_schema WHERE type = 'table'").Scan(&tables), qt.IsNil)
	c.Assert(tables, qt.Equals, 1)
	c.Assert(f.columns(c, "articles"), qt.DeepEquals, []string{"id", "title", "body", "updated_at"})
}

// TestInferenceOnAProviderEngineVerifiesAPreparedTargetE2E is a prepare run
// again after the storage it created lost a column: the run is recorded, so
// the second prepare is a retry, and a retry is accepted only when the
// provider finds the storage complete.
func TestInferenceOnAProviderEngineVerifiesAPreparedTargetE2E(t *testing.T) {
	c := qt.New(t)
	f := newInferenceFixture(c)
	spec := f.writeSpec(c, inferenceSpec{column: "embedding", revision: "1"})
	f.succeed(c, "prepare", spec, "--run-id", "first")
	f.exec(c, "ALTER TABLE articles DROP COLUMN embedding_state")

	output, code := f.run(c, "prepare", spec, "--run-id", "first")

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(output, qt.Contains, "validate prepared run first: ")
	c.Assert(output, qt.Contains, "prepared target column articles.embedding_state is absent")
}

// TestInferenceOnAProviderWithoutTheStoreE2E is a provider whose engine keeps
// no embedding store: every other command can use it, and inference refuses it
// by name before it reads or writes anything.
func TestInferenceOnAProviderWithoutTheStoreE2E(t *testing.T) {
	c := qt.New(t)
	f := newInferenceFixtureOf(c, "provider")
	spec := f.writeSpec(c, inferenceSpec{column: "embedding", revision: "1"})

	output, code := f.run(c, "prepare", spec, "--run-id", "first")

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(output, qt.Contains, `the example.org/fixture provider keeps no embedding store for engine "fixture" (embeddings.store)`)
	c.Assert(f.columns(c, "articles"), qt.DeepEquals, []string{"id", "title", "body", "updated_at"})
}
