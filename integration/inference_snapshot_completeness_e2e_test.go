//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
)

// TestInferenceSnapshotCompletenessIsMeasuredE2E is stokaro/ptah#2649 finding 3.
//
// "The backfill has not reached the end of its snapshot" was decided by a phase
// reading. The first spelling, `Phase != backfilling`, was true for every phase
// BEFORE the backfill as well as after it. The second, `Reached(backfilled)`,
// closed that direction and left the one a high-water mark cannot express: a run
// whose backfill once finished and was then given more to do still read as
// complete, so the entire consistency layer went quiet for a run whose status
// was `failed`.
//
// The fixture is that exact sequence -- a healthy run, a resumed pass that fails
// partway, and then a pass that finishes -- because a phase reading can produce
// the sentence only by never having reached `backfilled`, and cannot take it
// back afterwards.
func TestInferenceSnapshotCompletenessIsMeasuredE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshSnapshotDatabase(c, ctx, dbURL, "ptah_snapshot")
	seedCLIArticles(c, ctx, db)
	endpoint := httptest.NewServer(http.HandlerFunc(poisonableEmbeddingsHandler(c)))
	defer endpoint.Close()

	specPath := writeCLISpec(c, endpoint.URL)
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")

	// The control. A finished walk says nothing, and a fix that always reported
	// the finding would fail here.
	c.Assert(runInference(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID),
		qt.Not(qt.Contains), "has not reached the end of its snapshot")

	// More rows, one of which the provider refuses, so the resumed pass stops
	// partway through the walk. That is the state the phase could not describe:
	// the run has reached `backfilled` and has work left.
	_, err := db.ExecContext(ctx,
		`INSERT INTO articles (id, title, body, updated_at) VALUES
			(4, 'Fourth', 'about shipping', '8'),
			(5, 'Fifth', 'POISON about returns', '8')`)
	c.Assert(err, qt.IsNil)
	failed, err := runInferenceExpectingFailure(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID,
		"--batch-rows", "1", "--batch-inputs", "1")
	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", failed))

	output, err := runInferenceExpectingFailure(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains,
		"[consistency/blocking] the backfill has not reached the end of its snapshot")

	// And it goes away when the walk finishes, which is the direction that
	// proves the answer follows the walk rather than latching. The row the
	// provider refused is edited so it is embeddable.
	_, err = db.ExecContext(ctx, `UPDATE articles SET body = 'about returns' WHERE id = 5`)
	c.Assert(err, qt.IsNil)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	c.Assert(runInference(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID),
		qt.Not(qt.Contains), "has not reached the end of its snapshot")
}

// TestInferenceRowsAfterAFinishedWalkAreNotTheBackfillsE2E is the control that
// decides between the two ways to answer this.
//
// Asking the SOURCE -- are there in-scope rows past the run's cursor -- is
// tempting and wrong under `outbox`, where a row written after the boundary is
// catch-up's to process and the backfill owes nothing for it. Measured: it made
// the CLI lifecycle test report an incomplete snapshot for a run that had
// backfilled, caught up, and covered every row.
//
// So the fact is recorded by the walk instead, and this pins the consequence:
// rows appearing after a finished walk do not reopen it.
func TestInferenceRowsAfterAFinishedWalkAreNotTheBackfillsE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshSnapshotDatabase(c, ctx, dbURL, "ptah_snapshot_after")
	seedCLIArticles(c, ctx, db)
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	specPath := writeCLISpec(c, endpoint.URL)
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")

	_, err := db.ExecContext(ctx,
		`INSERT INTO articles (id, title, body, updated_at)
			VALUES (4, 'Fourth', 'about shipping', '8')`)
	c.Assert(err, qt.IsNil)
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")

	c.Assert(runInference(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID),
		qt.Not(qt.Contains), "has not reached the end of its snapshot")
}

// poisonableEmbeddingsHandler answers like [embeddingsHandler] except for an
// input carrying POISON, which it refuses.
//
// A provider that fails on demand is what makes a backfill stop partway with
// everything else intact: the rows before it are embedded and committed, the
// cursor is durable, and only the walk is unfinished.
func poisonableEmbeddingsHandler(c *qt.C) http.HandlerFunc {
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
			answer.Data = append(answer.Data, entry{Index: index, Embedding: embeddingOf(input)})
		}
		answer.Usage.PromptTokens = len(body.Input)
		answer.Usage.TotalTokens = len(body.Input) * 2
		writer.Header().Set("Content-Type", "application/json")
		c.Assert(refuseOrAnswer(writer, body.Input, answer), qt.IsNil)
	}
}

// embeddingOf is the deterministic vector the fixture provider answers with.
func embeddingOf(input string) []float32 {
	vector := make([]float32, 4)
	for component := range vector {
		vector[component] = float32(len(input) + component)
	}
	return vector
}

// refuseOrAnswer writes the refusal or the answer.
//
// The branch lives here rather than in the handler because a test function may
// not carry one, and a handler is production-shaped code the test supplies
// rather than an assertion.
func refuseOrAnswer(writer http.ResponseWriter, inputs []string, answer any) error {
	for _, input := range inputs {
		if strings.Contains(input, "POISON") {
			writer.WriteHeader(http.StatusInternalServerError)
			_, err := writer.Write([]byte(`{"error":"refused"}`))
			return err
		}
	}
	return json.NewEncoder(writer).Encode(answer)
}

// freshSnapshotDatabase makes a database of its own and hands back a connection
// and the URL that reaches it.
func freshSnapshotDatabase(
	c *qt.C, ctx context.Context, dbURL, prefix string,
) (*sql.DB, string) {
	c.Helper()
	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	name := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, name) })

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = db.Close() })
	return db, dbName
}
