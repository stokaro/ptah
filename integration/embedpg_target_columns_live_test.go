//go:build integration

package integration_test

// Live PostgreSQL coverage for the columns a generation writes into.
//
// Nothing created them until stokaro/ptah#2390: `Spec.TargetObjects` derived
// what a generation needs and every caller read the answer to verify or to
// retire, while the only ALTER TABLE in the tree was the DROP. These tests are
// about the half that was missing, and each of them reads the catalog rather
// than what a verb said about itself.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedpg"
	"ptah.run/internal/embedspec"
	"ptah.run/internal/embedverify"
)

// TestEnsureTarget_CreatesTheColumnsAGenerationWritesLive is the happy path.
func TestEnsureTarget_CreatesTheColumnsAGenerationWritesLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)

	spec := loadTargetSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)

	c.Assert(embeddingColumns(c, ctx, db, table), qt.DeepEquals, map[string]string{
		"embedding":                "vector",
		"embedding_generation":     "text",
		"embedding_input_hash":     "text",
		"embedding_source_version": "text",
		"embedding_state":          "text",
	})
}

// TestEnsureTarget_IsIdempotentLive is what lets several workers start at once.
func TestEnsureTarget_IsIdempotentLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)

	spec := loadTargetSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)

	c.Assert(embeddingColumns(c, ctx, db, table), qt.HasLen, 5)
}

// TestEnsureTarget_LeavesVectorsAloneLive is the control the idempotency needs.
//
// "Running it twice does not error" is satisfied by a second run that drops the
// column and creates it again. This writes a vector between the two calls and
// requires it to still be there, which is the property an operator resuming an
// interrupted backfill is relying on.
func TestEnsureTarget_LeavesVectorsAloneLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)

	spec := loadTargetSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)
	// Tagged with this specification's own generation, because a column holding
	// somebody else's is refused now -- that refusal has its own test, and this
	// one is about a second EnsureTarget not destroying what the first one's
	// backfill put there.
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		"UPDATE %s SET embedding = '[1,2,3,4]', embedding_generation = $1", table),
		spec.Identity().Digest)
	c.Assert(err, qt.IsNil)

	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)

	var vector, generation string
	c.Assert(db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT embedding::text, embedding_generation FROM %s", table)).
		Scan(&vector, &generation), qt.IsNil)
	c.Assert(vector, qt.Equals, "[1,2,3,4]")
	c.Assert(generation, qt.Equals, spec.Identity().Digest)
}

// TestEnsureTarget_RefusesWithoutPgvectorLive is the failure path, and it needs
// its own database because the refusal is about one not having the extension.
//
// Ptah does not install it: CREATE EXTENSION is a database-wide privileged act,
// and a migration tool taking it on behalf of an operator who did not ask is a
// surprise this repository refuses elsewhere. So the refusal carries the
// statement to run.
func TestEnsureTarget_RefusesWithoutPgvectorLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withoutVector)

	err := embedpg.EnsureTarget(ctx, db, loadTargetSpec(c, table))

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "the target database has no pgvector")
	c.Assert(err.Error(), qt.Contains, "CREATE EXTENSION vector")
	// And it refused before touching the table, rather than after a partial
	// ALTER: the metadata columns are TEXT and would have been created happily.
	c.Assert(embeddingColumns(c, ctx, db, table), qt.HasLen, 0)
}

// What the fixture database has installed before the test runs. Statements
// rather than a flag, so the helper below has nothing to branch on: a setup
// helper that chose between two databases would be the conditional this
// repository's test style keeps out of test bodies.
var (
	withVector    = []string{"CREATE EXTENSION IF NOT EXISTS vector"}
	withoutVector []string
)

// targetColumnsDatabase makes a database of its own holding one source table.
func targetColumnsDatabase(
	c *qt.C, ctx context.Context, setup []string,
) (*sql.DB, string) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.TimescaleDB)
	admin, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer admin.Close()

	name := fmt.Sprintf("ptah_target_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, admin, name)
	c.Cleanup(func() {
		cleanup, err := sql.Open("pgx", adminURL)
		c.Assert(err, qt.IsNil)
		defer cleanup.Close()
		dropE2EDatabase(c, context.Background(), cleanup, name)
	})

	db, err := sql.Open("pgx", replaceDatabaseName(c, adminURL, name))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = db.Close() })

	table := "docs"
	_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s (
		id BIGINT PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL,
		updated_at TEXT NOT NULL)`, table))
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (id, title, body, updated_at) VALUES (1, 'a', 'b', '1')", table))
	c.Assert(err, qt.IsNil)

	for _, statement := range setup {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	// The run tables, because Target.Commit writes the run's progress in the
	// same transaction as the vectors -- which is the point of that design and
	// not something a test gets to opt out of.
	c.Assert(embedpg.NewStore(db).EnsureSchema(ctx), qt.IsNil)
	return db, table
}

// embeddingColumns reports the generation's columns as the catalog holds them.
func embeddingColumns(
	c *qt.C, ctx context.Context, db *sql.DB, table string,
) map[string]string {
	c.Helper()
	rows, err := db.QueryContext(ctx, `SELECT column_name, udt_name
		FROM information_schema.columns
		WHERE table_name = $1 AND column_name LIKE 'embedding%'`, table)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	found := map[string]string{}
	for rows.Next() {
		var name, kind string
		c.Assert(rows.Scan(&name, &kind), qt.IsNil)
		found[name] = kind
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// loadTargetSpec writes and loads a specification naming that table.
func loadTargetSpec(c *qt.C, table string) embedgen.Spec {
	c.Helper()
	document := fmt.Sprintf(`
version: 1
name: target columns
source:
  schema: public
  table: %s
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
  endpoint: http://127.0.0.1:9/v1
  identifier: test-embed
  revision: "1"
  reported_dimension: 4
  normalization: none
target:
  schema: public
  table: %s
  column: embedding
  representation: vector
  metric: cosine
consistency:
  mode: outbox
policy:
  require_exact_approval: true
  require_consistency_mode: true
`, table, table)
	path := filepath.Join(c.TempDir(), "spec.yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	loaded, err := embedspec.Load(path)
	c.Assert(err, qt.IsNil)
	return loaded.Spec
}

// TestVerificationCorpus_ReportsTheWidthAndNotTheVectorLive is what keeps a
// verification's memory proportional to the corpus rather than to the corpus
// times its dimension.
//
// The read used to answer with `make([]float32, dimension)` per row: a
// zero-filled slice carrying nothing the width does not, because pgvector
// refuses a NaN or an infinity on write and the layer that reads it asks about
// length. Over a million rows at 1536 dimensions that is six gigabytes of
// zeroes, and `ptah inference verify` ran the process out of memory on a corpus
// it could otherwise measure in a third of a second (stokaro/ptah#2068).
//
// A live test because the placeholder was built where the server's answer was
// scanned, and every assertion about it that did not go through a real read
// would be an assertion about a fixture.
func TestVerificationCorpus_ReportsTheWidthAndNotTheVectorLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)

	spec := loadTargetSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)
	// The row the helper seeded, given a vector and the metadata a written
	// generation carries.
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		`UPDATE %s SET %s = $1, %s_generation = 'gen-1', %s_input_hash = 'hash-1',
			%s_source_version = updated_at, %s_state = 'embedded' WHERE id = 1`,
		table, spec.Target.Column, spec.Target.Column, spec.Target.Column,
		spec.Target.Column, spec.Target.Column), "[1,2,3,4]")
	c.Assert(err, qt.IsNil)

	corpus, err := embedpg.VerificationCorpus(ctx, db, spec)
	c.Assert(err, qt.IsNil)

	var stored []embedverify.TargetRow
	for pair, walkErr := range corpus {
		c.Assert(walkErr, qt.IsNil)
		stored = append(stored, *pair.Target)
	}

	c.Assert(stored, qt.HasLen, 1)
	// The server's own answer to how wide the stored vector is, which is all
	// the read reports: the values themselves are not fetched, and the field
	// that used to carry them is gone (stokaro/ptah#2622).
	c.Assert(stored[0].Dimension, qt.Equals, 4)
}
