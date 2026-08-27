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
}

// seedEvaluationArticles writes documents whose topics a search can separate.
//
// The bodies are what the fake provider reads: it embeds by topic, so a query
// about pricing is nearest the pricing document and a corpus can say so. A
// fixture of interchangeable text would score the same however the search
// behaved.
func seedEvaluationArticles(c *qt.C, ctx context.Context, db *sql.DB) {
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
		`INSERT INTO articles (id, title, body, updated_at) VALUES
			(1, 'Pricing',  'pricing plans and cost',      '1'),
			(2, 'Support',  'support contact and help',    '1'),
			(3, 'Billing',  'billing invoices and refund', '1'),
			(4, 'Renewals', 'renewal and subscription',    '1')`,
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
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
