//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestInferenceRepresentationsE2E is stokaro/ptah#2633.
//
// The one walk that feeds every verification layer appended
// `vector_dims(<column>)` whatever the target representation was. pgvector
// defines that function for `vector` and `halfvec` and not for `sparsevec`, so
// a sparsevec generation prepared, backfilled, caught up and indexed -- storing
// correct sparse vectors -- and then `verify`, `status` and `cutover` all died
// with `function vector_dims(sparsevec) does not exist`.
//
// That stranded the generation with no way out. The plan digest an approval
// binds to is published only by a cutover refused for want of one, and the
// cutover could not reach the point of refusing: it failed on the same read. So
// no approval existed that could rescue it, and `status --require-ready` -- what
// the Kubernetes rollout guide gates on -- answered with a raw SQLSTATE rather
// than the exit-1 "not ready" it promises.
//
// Every representation the specification accepts is walked here, because the
// defect was one representation taking a path the others did not. Testing the
// one that broke would leave the next one to be found by an operator.
func TestInferenceRepresentationsE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_repr_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	representations := []struct {
		name   string
		column string
	}{
		{name: "vector", column: "embedding_dense"},
		{name: "halfvec", column: "embedding_half"},
		{name: "sparsevec", column: "embedding_sparse"},
	}
	for _, representation := range representations {
		t.Run(representation.name, func(t *testing.T) {
			c := qt.New(t)
			specPath := writeRepresentationSpec(c, endpoint.URL,
				representation.name, representation.column)
			runID := "repr-" + representation.name

			runInference(c, ctx, "prepare",
				"--spec", specPath, "--db-url", dbName, "--run-id", runID)
			runInference(c, ctx, "backfill",
				"--spec", specPath, "--db-url", dbName, "--run-id", runID, "--batch-rows", "10")
			runInference(c, ctx, "catchup",
				"--spec", specPath, "--db-url", dbName, "--run-id", runID, "--batch-rows", "10")
			runInference(c, ctx, "index",
				"--spec", specPath, "--db-url", dbName, "--run-id", runID)

			// The three verbs that run the deterministic layers, and so the
			// three the missing function killed.
			runInference(c, ctx, "verify",
				"--spec", specPath, "--db-url", dbName, "--run-id", runID)
			runInference(c, ctx, "status",
				"--spec", specPath, "--db-url", dbName, "--run-id", runID, "--require-ready")

			refused, err := runInferenceExpectingFailure(c, ctx, "cutover",
				"--spec", specPath, "--db-url", dbName, "--run-id", runID)
			c.Assert(err, qt.IsNotNil)
			runInference(c, ctx, "cutover",
				"--spec", specPath, "--db-url", dbName, "--run-id", runID,
				"--approve", planDigestFrom(c, refused), "--approver", "an operator")

			// The width the verification layer measured has to be the one the
			// rows actually carry: an expression answering NULL for every row
			// would report no payload rather than a wrong one, and an
			// expression answering a constant would agree with itself.
			c.Assert(storedWidths(c, ctx, db, representation.column),
				qt.DeepEquals, []int{4, 4, 4})
		})
	}
}

// storedWidths asks the server for each row's vector width, per representation.
//
// Read through the catalog's own type rather than through the expression under
// test: a helper that reused it would agree with whatever it produced, which is
// the whole failure mode this file exists for.
func storedWidths(c *qt.C, ctx context.Context, db *sql.DB, column string) []int {
	c.Helper()
	var udt string
	c.Assert(db.QueryRowContext(ctx,
		`SELECT udt_name FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = 'articles' AND column_name = $1`,
		column).Scan(&udt), qt.IsNil)

	// A sparse vector states its width after the slash; a dense one is a list.
	widths := map[string]string{
		"sparsevec": `split_part(%s::text, '/', 2)::int`,
		"vector":    `vector_dims(%s)`,
		"halfvec":   `vector_dims(%s)`,
	}
	expression, known := widths[udt]
	c.Assert(known, qt.IsTrue, qt.Commentf("no width expression for %q", udt))

	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		`SELECT %s FROM articles WHERE %q IS NOT NULL ORDER BY id`,
		fmt.Sprintf(expression, `"`+column+`"`), column))
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var found []int
	for rows.Next() {
		var width int
		c.Assert(rows.Scan(&width), qt.IsNil)
		found = append(found, width)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// writeRepresentationSpec writes a specification at one target representation.
func writeRepresentationSpec(c *qt.C, endpoint, representation, column string) string {
	c.Helper()
	document := fmt.Sprintf(`
version: 1
name: cli articles
source:
  schema: public
  table: articles
  key_fields: [id]
  input_fields: [title, body]
  version_strategy: updated_at
  version_field: updated_at
  mutable: true
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
  revision: "1"
  reported_dimension: 4
  normalization: none
target:
  schema: public
  table: articles
  column: %s
  representation: %s
  metric: cosine
  index_method: hnsw
consistency:
  mode: outbox
policy:
  require_exact_approval: true
`, endpoint, column, representation)
	path := filepath.Join(c.TempDir(), "spec-"+representation+".yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}
