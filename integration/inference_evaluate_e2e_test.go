//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestInferenceEvaluateE2E runs an evaluation corpus against a live generation
// through the command line.
//
// The deterministic layers cannot tell whether a corpus WORKS: perfectly fresh
// vectors from a worse model pass every one of them. This is the verb that can,
// and it can only be tested against a real index -- the exact-search comparison
// exists precisely to catch a difference between what the index returns and what
// the vectors contain, and no fake has both (stokaro/ptah#2068).
func TestInferenceEvaluateE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_eval_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedEvaluationArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(topicEmbeddingsHandler(c)))
	defer endpoint.Close()
	specPath := writeCLISpec(c, endpoint.URL)

	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")

	assertACorpusTheGenerationAnswers(c, ctx, specPath, dbName)
	assertACorpusItDoesNotAnswerIsRefused(c, ctx, specPath, dbName)
	assertAFloorNobodySetGatesNothing(c, ctx, specPath, dbName)
	assertAnEmptyCorpusIsRefusedBeforeAnythingRuns(c, ctx, specPath, dbName)
	assertACasesOwnDepthBeatsTheCorpusDefault(c, ctx, specPath, dbName)
	assertAnotherGenerationsRowsAreNotAnswers(c, ctx, db, specPath, dbName)
	assertTheMetricDecidesTheOrder(c, ctx, adminDB, dbURL, endpoint.URL)
	assertAnApproximateIndexIsComparedAgainstExactSearch(c, ctx, adminDB, dbURL, endpoint.URL)
	assertABaselineIsMeasuredAndGates(c, ctx, db, specPath, dbName, endpoint.URL)
	assertAHardExpectationAloneStillScores(c, ctx, specPath, dbName)
}

// assertABaselineIsMeasuredAndGates is stokaro/ptah#2640.
//
// `--baseline` was bound to a field nothing read: `evaluateCorpus` handed
// `Evaluate` a literal empty baseline, the report short-circuited on
// `Baseline.Cases == 0`, and both regression gates were unreachable. Measured on
// the shipped binary, `--max-ndcg-regression 0` — the strictest allowance there
// is — refused nothing, and the string `not-a-generation-at-all` was accepted at
// exit 0.
//
// The comparison is now a second measurement of the same corpus against the
// previous generation, driven by that generation's OWN specification: scoring a
// generation embeds each query with its model and searches its column, and a
// generation identity carries neither.
func assertABaselineIsMeasuredAndGates(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL, endpoint string,
) {
	c.Helper()
	// A second generation over the same rows, differing in its column so the
	// two can coexist, and backfilled through the same fake provider — so the
	// two generations answer the corpus identically and the regression is zero.
	previous := writeCLISpecWithMetric(c, endpoint, "cosine", "embedding_prev")
	runInference(c, ctx, "prepare",
		"--spec", previous, "--db-url", dbURL, "--run-id", "eval-baseline")
	runInference(c, ctx, "backfill",
		"--spec", previous, "--db-url", dbURL, "--run-id", "eval-baseline", "--batch-rows", "10")

	corpus := writeCorpus(c, `
version: 1
name: baseline
default_k: 3
cases:
  - id: pricing
    query: pricing
    required: ["1"]
    relevant: {"1": 3}
`)
	identity := generationIdentityOf(c, ctx, previous, dbURL)

	output := runInference(c, ctx, "evaluate",
		"--spec", specPath, "--db-url", dbURL, "--corpus", corpus,
		"--baseline", identity, "--baseline-spec", previous,
		"--max-ndcg-regression", "0", "--max-mrr-regression", "0")

	// The report no longer says the comparison was not measured, which is the
	// sentence the defect printed for every run.
	c.Assert(output, qt.Not(qt.Contains), "no baseline was measured for it")
	// The generations are equivalent here, so the strictest allowance passes.
	c.Assert(output, qt.Not(qt.Contains), "regression")

	assertABaselineThatIsNotTheGenerationNamedIsRefused(c, ctx, specPath, dbURL, corpus, previous)
	assertHalfAComparisonIsRefused(c, ctx, specPath, dbURL, corpus, identity)
}

// assertABaselineThatIsNotTheGenerationNamedIsRefused is the validation the
// flag never had: a nonsense identity was accepted at exit 0.
func assertABaselineThatIsNotTheGenerationNamedIsRefused(
	c *qt.C, ctx context.Context, specPath, dbURL, corpus, previous string,
) {
	c.Helper()
	_, err := runInferenceExpectingFailure(c, ctx, "evaluate",
		"--spec", specPath, "--db-url", dbURL, "--corpus", corpus,
		"--baseline", "not-a-generation-at-all", "--baseline-spec", previous)

	c.Assert(err, qt.ErrorMatches, `(?s).*--baseline names generation .*`)
}

// assertHalfAComparisonIsRefused keeps a request nobody can answer from being
// answered with silence.
func assertHalfAComparisonIsRefused(
	c *qt.C, ctx context.Context, specPath, dbURL, corpus, identity string,
) {
	c.Helper()
	_, err := runInferenceExpectingFailure(c, ctx, "evaluate",
		"--spec", specPath, "--db-url", dbURL, "--corpus", corpus,
		"--baseline", identity, "--max-ndcg-regression", "0")

	c.Assert(err, qt.ErrorMatches, `(?s).*--baseline-spec is how it gets measured.*`)
}

// generationIdentityOf reads a specification's generation identity through the
// verb that prints it, rather than recomputing it here.
func generationIdentityOf(c *qt.C, ctx context.Context, specPath, dbURL string) string {
	c.Helper()
	output := runInference(c, ctx, "describe", "--spec", specPath, "--format", "json")
	var described struct {
		Generation string `json:"generation"`
	}
	c.Assert(json.Unmarshal([]byte(output), &described), qt.IsNil, qt.Commentf("%s", output))
	c.Assert(described.Generation, qt.HasLen, 64)
	return described.Generation
}

// assertAHardExpectationAloneStillScores is stokaro/ptah#2634.
//
// A reader's first corpus states the answer it wants and nothing else. Every
// ranked measure divides by the number of GRADED keys, so a case with `required`
// and no `relevant` divided by zero: recall, MRR and NDCG all came back NaN, at
// exit 0. A comparison against NaN is always false, so `--max-recall-drop` and
// `--max-ndcg-drop` could never fire on such a corpus -- the gate was present,
// accepted, and unable to refuse anything.
//
// The numbers are asserted rather than the exit code. NaN exits 0, so a run
// that only checked the command succeeded would have passed against the defect;
// and a fix answering a constant zero would exit 0 too, while reporting a
// generation that answers every query perfectly as answering none.
func assertAHardExpectationAloneStillScores(
	c *qt.C, ctx context.Context, specPath, dbURL string,
) {
	c.Helper()
	corpus := writeCorpus(c, `
version: 1
name: required only
default_k: 3
cases:
  - id: pricing
    query: "pricing pricing pricing"
    required: ["1"]
  - id: shipping
    query: "shipping shipping shipping"
    required: ["2"]
`)

	output := runInference(c, ctx, "evaluate",
		"--spec", specPath, "--db-url", dbURL, "--corpus", corpus)

	// Every query is about a topic one document holds, so a corpus reading its
	// hard expectations as grades scores perfectly.
	c.Assert(output, qt.Contains, "recall 1.000")
	c.Assert(output, qt.Not(qt.Contains), "NaN")
}

// seedEvaluationArticles writes documents whose topics a search can separate.
//
// The bodies are what the fake provider reads: it embeds by topic, so a query
// about pricing is nearest the pricing document and a corpus can say so. A
// fixture of interchangeable text would score the same however the search
// behaved.
func seedEvaluationArticles(c *qt.C, ctx context.Context, db *sql.DB) {
	c.Helper()
	seedArticlesTable(c, ctx, db,
		// Every document is a distinct MIXTURE, which is what makes cosine rank
		// them strictly. Documents mentioning one topic and nothing else all
		// point the same way, so a cosine search over them is a tie the planner
		// breaks -- and a fixture built on that measures the planner.
		`INSERT INTO articles (id, title, body, updated_at) VALUES
			(1, 'Pricing',  'pricing pricing pricing support', '1'),
			(2, 'Support',  'support support support pricing', '1'),
			(3, 'Billing',  'billing billing billing support', '1'),
			(4, 'Renewals', 'renewal renewal renewal billing', '1'),
			(5, 'Costs',    'pricing pricing support support', '1')`)
}

// topics are the four axes the fake provider embeds along.
var topics = []string{"pricing", "support", "billing", "renewal"}

// topicEmbeddingsHandler answers with a vector whose components count the topic
// words in the input.
//
// A document about pricing lands on the pricing axis, and a query about pricing
// lands on the same one, so cosine distance actually separates them. That is
// what makes the corpus below measure retrieval rather than measure nothing.
func topicEmbeddingsHandler(c *qt.C) http.HandlerFunc {
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
			Usage struct {
				PromptTokens int `json:"prompt_tokens"`
				TotalTokens  int `json:"total_tokens"`
			} `json:"usage"`
		}{Model: body.Model}
		for index, input := range body.Input {
			answer.Data = append(answer.Data, entry{Index: index, Embedding: topicVector(input)})
		}
		answer.Usage.PromptTokens = len(body.Input)
		answer.Usage.TotalTokens = len(body.Input)
		writer.Header().Set("Content-Type", "application/json")
		c.Assert(json.NewEncoder(writer).Encode(answer), qt.IsNil)
	}
}

// topicVector counts each topic word in a text.
//
// A vector of all zeroes would be rejected by cosine distance as having no
// direction, so a text mentioning nothing gets a small even spread -- which is
// equidistant from everything, and is the right answer for a query about
// nothing in the corpus.
func topicVector(text string) []float32 {
	vector := make([]float32, len(topics))
	total := float32(0)
	for index, topic := range topics {
		count := float32(countOccurrences(text, topic))
		vector[index] = count
		total += count
	}
	if total == 0 {
		for index := range vector {
			vector[index] = 1
		}
	}
	return vector
}

// countOccurrences counts how many times a word appears.
func countOccurrences(text, word string) int {
	count := 0
	for position := 0; position+len(word) <= len(text); position++ {
		if text[position:position+len(word)] == word {
			count++
		}
	}
	return count
}

// writeCorpus writes an evaluation corpus file.
func writeCorpus(c *qt.C, document string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "corpus.yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// assertACorpusTheGenerationAnswers is the control.
//
// Every query is about a topic one document holds, and the generation finds it.
// Without this, a verb that refused everything would satisfy every row below.
func assertACorpusTheGenerationAnswers(c *qt.C, ctx context.Context, specPath, dbURL string) {
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
	// The parameters every number was measured under. ADR 0010's whole point:
	// two numbers taken under different ones are not two numbers about the
	// same thing.
	c.Assert(output, qt.Contains, "measured under hnsw.ef_search=")
	c.Assert(output, qt.Contains, "ivfflat.probes=")
}

// assertACorpusItDoesNotAnswerIsRefused is the finding a score cannot express.
//
// The corpus asks for a document the generation ranks nowhere near the query,
// and a required hit is a hard expectation rather than an average.
func assertACorpusItDoesNotAnswerIsRefused(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()
	corpus := writeCorpus(c, `
version: 1
default_k: 1
cases:
  - id: wrong-document
    query: pricing
    required: ["4"]
    relevant: {"4": 3}
`)

	output, err := runInferenceExpectingFailure(c, ctx, "evaluate",
		"--spec", specPath, "--db-url", dbURL, "--corpus", corpus)

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "blocking: wrong-document did not return 4")
}

// assertAFloorNobodySetGatesNothing is ADR 0013's decision, reached through the
// command line.
//
// A threshold Ptah picked would be met or missed by a session setting Ptah does
// not own. So the same corpus that fails with a floor reports its number and
// exits zero without one.
func assertAFloorNobodySetGatesNothing(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()
	corpus := writeCorpus(c, `
version: 1
default_k: 1
cases:
  - id: half
    query: pricing
    relevant: {"1": 3, "4": 1}
`)

	refused, err := runInferenceExpectingFailure(c, ctx, "evaluate",
		"--spec", specPath, "--db-url", dbURL, "--corpus", corpus, "--min-recall", "1.0")
	c.Assert(err, qt.IsNotNil)
	c.Assert(refused, qt.Contains, "recall is 0.500 and this policy requires 1.000")

	reported := runInference(c, ctx, "evaluate",
		"--spec", specPath, "--db-url", dbURL, "--corpus", corpus)

	c.Assert(reported, qt.Contains, "recall 0.500")
	c.Assert(reported, qt.Not(qt.Contains), "blocking:")
}

// assertAnEmptyCorpusIsRefusedBeforeAnythingRuns keeps a gate from passing
// because it had no question.
func assertAnEmptyCorpusIsRefusedBeforeAnythingRuns(
	c *qt.C, ctx context.Context, specPath, dbURL string,
) {
	c.Helper()
	corpus := writeCorpus(c, "version: 1\n")

	output, err := runInferenceExpectingFailure(c, ctx, "evaluate",
		"--spec", specPath, "--db-url", dbURL, "--corpus", corpus,
		"--min-recall", "1.0", "--min-exact-agreement", "1.0")

	c.Assert(err, qt.IsNotNil)
	c.Assert(output+err.Error(), qt.Contains, "declares no cases, so it would measure nothing")
}

// assertACasesOwnDepthBeatsTheCorpusDefault is why a case carries a depth.
//
// One query wanting ten results and another wanting one is the normal shape of
// a corpus. The document this case requires is ranked second, so a default of
// one misses it and the case's own three finds it.
func assertACasesOwnDepthBeatsTheCorpusDefault(
	c *qt.C, ctx context.Context, specPath, dbURL string,
) {
	c.Helper()
	shallow := writeCorpus(c, `
version: 1
default_k: 1
cases:
  - id: second-place
    query: pricing
    required: ["5"]
    relevant: {"5": 3}
`)
	deep := writeCorpus(c, `
version: 1
default_k: 1
cases:
  - id: second-place
    query: pricing
    k: 3
    required: ["5"]
    relevant: {"5": 3}
`)

	output, err := runInferenceExpectingFailure(c, ctx, "evaluate",
		"--spec", specPath, "--db-url", dbURL, "--corpus", shallow)
	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "second-place did not return 5")

	c.Assert(runInference(c, ctx, "evaluate",
		"--spec", specPath, "--db-url", dbURL, "--corpus", deep),
		qt.Not(qt.Contains), "blocking:")
}

// assertAnotherGenerationsRowsAreNotAnswers is Decision 6 at query time.
//
// Two generations can share a column while a migration is in flight -- an
// interrupted backfill leaves exactly that. A search that did not filter by
// generation would answer with vectors from a different model, which are
// perfectly valid numbers pointing at the wrong documents.
func assertAnotherGenerationsRowsAreNotAnswers(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL string,
) {
	c.Helper()
	// The document this query is about now belongs to somebody else's
	// generation. It is still in the table, and its vector is still the
	// nearest one.
	_, err := db.ExecContext(ctx,
		`UPDATE articles SET embedding_generation = 'another-generation' WHERE id = 1`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, restoreErr := db.ExecContext(ctx,
			`UPDATE articles SET embedding_generation = (
				SELECT embedding_generation FROM articles WHERE id = 2) WHERE id = 1`)
		c.Assert(restoreErr, qt.IsNil)
	}()
	corpus := writeCorpus(c, `
version: 1
default_k: 1
cases:
  - id: pricing
    query: pricing
    required: ["1"]
    relevant: {"1": 3}
`)

	output, err := runInferenceExpectingFailure(c, ctx, "evaluate",
		"--spec", specPath, "--db-url", dbURL, "--corpus", corpus)

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "pricing did not return 1")
}

// assertTheMetricDecidesTheOrder is why the distance operator is chosen from a
// closed set rather than defaulted.
//
// A wrong operator answers every query with the wrong distance, which produces
// plausible results in the wrong order -- the failure that looks least like a
// failure. The two documents here are ranked one way by cosine and the other by
// Euclidean distance, so the same corpus gets opposite answers.
//
// It gets its own database and exactly two documents, because the separation is
// arithmetic: a third document sharing either one's direction ties with it under
// cosine, and the fixture would then be measuring which of a tie the planner
// happened to return.
func assertTheMetricDecidesTheOrder(
	c *qt.C, ctx context.Context, adminDB *sql.DB, dbURL, endpointURL string,
) {
	c.Helper()
	name := fmt.Sprintf("ptah_eval_metric_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	metricURL := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", metricURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedTheMetricPair(c, ctx, db)

	corpus := writeCorpus(c, `
version: 1
default_k: 1
cases:
  - id: nearest
    query: renewal
    required: ["1"]
    relevant: {"1": 3}
`)

	// Document one points exactly where the query does and is far along that
	// direction; document two points elsewhere and is closer in absolute terms.
	cosineSpec := writeCLISpecWithMetric(c, endpointURL, "cosine", "embedding")
	backfillFor(c, ctx, cosineSpec, metricURL, "cosine-run")
	c.Assert(runInference(c, ctx, "evaluate",
		"--spec", cosineSpec, "--db-url", metricURL, "--corpus", corpus),
		qt.Not(qt.Contains), "blocking:")

	// The same two documents, the same query, the other metric, the other
	// answer. The metric is part of the generation identity, so this is a
	// second generation -- and a generation writes its own column. It used to
	// name the same one, which worked only because nothing stopped it from
	// overwriting the first (stokaro/ptah#2391).
	l2Spec := writeCLISpecWithMetric(c, endpointURL, "l2", "embedding_l2")
	backfillFor(c, ctx, l2Spec, metricURL, "l2-run")

	output, err := runInferenceExpectingFailure(c, ctx, "evaluate",
		"--spec", l2Spec, "--db-url", metricURL, "--corpus", corpus)

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "nearest did not return 1")
}

// backfillFor prepares and fills a generation.
func backfillFor(c *qt.C, ctx context.Context, specPath, dbURL, runID string) {
	c.Helper()
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID, "--batch-rows", "500")
}

// seedTheMetricPair writes the two documents cosine and Euclidean distance
// disagree about.
//
// Against a one-word "renewal" query: document one is three steps along the
// renewal axis -- the same direction, distance 2 away. Document two is one step
// along renewal and one along support -- a different direction, distance 1 away.
func seedTheMetricPair(c *qt.C, ctx context.Context, db *sql.DB) {
	c.Helper()
	seedArticlesTable(c, ctx, db, `INSERT INTO articles (id, title, body, updated_at) VALUES
		(1, 'Same direction', 'renewal renewal renewal', '1'),
		(2, 'Closer overall', 'renewal support',         '1')`)
}

// assertAnApproximateIndexIsComparedAgainstExactSearch is the comparison the
// exact-search path exists for, and the only one that needs a real index.
//
// An approximate index is allowed to miss. `ivfflat` with one probe searches a
// single cluster, so over a corpus spread across many it returns a neighbour
// that is near rather than nearest -- and the vectors are perfectly fine. A
// verification that only asked the index would report that as a bad model.
//
// It needs its own database and enough rows for the planner to reach for the
// index at all: over five rows it scans, index and exact agree by construction,
// and the fixture measures nothing.
func assertAnApproximateIndexIsComparedAgainstExactSearch(
	c *qt.C, ctx context.Context, adminDB *sql.DB, dbURL, endpointURL string,
) {
	c.Helper()
	name := fmt.Sprintf("ptah_eval_index_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	indexedURL := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", indexedURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedManyEvaluationArticles(c, ctx, db)

	specPath := writeCLISpec(c, endpointURL)
	backfillFor(c, ctx, specPath, indexedURL, cliRunID)

	// The index goes on after the vectors, which is what Phase G does, and one
	// probe is what makes it approximate enough to disagree.
	for _, statement := range []string{
		// Far more lists than this many rows support, which is a real
		// misconfiguration and the one this check exists for: each probe then
		// sees a couple of rows, and one probe reads a couple of rows out of a
		// thousand. pgvector's own guidance is rows/1000.
		`CREATE INDEX ON articles USING ivfflat (embedding vector_cosine_ops) WITH (lists = 500)`,
		`ALTER DATABASE ` + name + ` SET ivfflat.probes = 1`,
		`ALTER DATABASE ` + name + ` SET enable_seqscan = off`,
	} {
		_, execErr := db.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("%s", statement))
	}

	corpus := writeCorpus(c, manyTopicCorpus())
	output, err := runInferenceExpectingFailure(c, ctx, "evaluate",
		"--spec", specPath, "--db-url", indexedURL, "--corpus", corpus,
		"--min-exact-agreement", "1.0")

	// The index and an exhaustive scan disagree, which is a fact about the
	// index rather than about the vectors -- and the run says which.
	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains, "the index agrees with an exhaustive search on ")
	c.Assert(output, qt.Contains, "and this policy requires 1.000")
	c.Assert(output, qt.Contains, "ivfflat.probes=1")
}

// seedManyEvaluationArticles writes enough documents for an approximate index
// to be worth building.
func seedManyEvaluationArticles(c *qt.C, ctx context.Context, db *sql.DB) {
	c.Helper()
	// Every combination of topic word counts, which spreads the corpus over the
	// whole space rather than onto four points.
	seedArticlesTable(c, ctx, db, `INSERT INTO articles (id, title, body, updated_at)
		SELECT n, 'Article ' || n,
			repeat('pricing ', 1 + (n % 5)) || repeat('support ', 1 + ((n / 5) % 5)) ||
			repeat('billing ', 1 + ((n / 25) % 5)) || repeat('renewal ', 1 + ((n / 125) % 5)),
			'1'
		FROM generate_series(1, 1000) AS n`)
}

// seedArticlesTable creates the articles table with a generation's columns and
// fills it.
func seedArticlesTable(c *qt.C, ctx context.Context, db *sql.DB, insert string) {
	c.Helper()
	for _, statement := range []string{
		`CREATE EXTENSION IF NOT EXISTS vector`,
		`CREATE TABLE articles (
			id BIGINT PRIMARY KEY, title TEXT, body TEXT, updated_at TEXT NOT NULL)`,
		`ALTER TABLE articles
			ADD COLUMN embedding vector(4),
			ADD COLUMN embedding_generation TEXT,
			ADD COLUMN embedding_input_hash TEXT,
			ADD COLUMN embedding_source_version TEXT,
			ADD COLUMN embedding_state TEXT`,
		insert,
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// manyTopicCorpus asks a spread of queries over that corpus.
//
// Ten cases rather than one: an approximate index misses some queries and not
// others, and one case is a coin toss.
func manyTopicCorpus() string {
	document := "version: 1\ndefault_k: 5\ncases:\n"
	queries := []string{
		"pricing", "support", "billing", "renewal",
		"pricing support", "pricing billing", "support billing", "billing renewal",
		"pricing pricing support", "support support billing",
	}
	for index, query := range queries {
		document += fmt.Sprintf("  - id: case-%d\n    query: %q\n    relevant: {\"1\": 1}\n",
			index, query)
	}
	return document
}
