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
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/embedpg"
)

// TestInferenceRetirementKeepsAnOutboxASecondTargetNeedsE2E is the defect the
// first fix for stokaro/ptah#2649 introduced.
//
// An outbox belongs to a SOURCE table, and the ownership question was asked
// about the TARGET one. A specification whose target table differs from its
// source is accepted, so two generations can share one source -- and therefore
// one outbox -- while writing into different tables. Retiring either counted
// zero live generations over its own target and uninstalled the capture the
// other was still fed by. The survivor stopped seeing writes silently, and said
// so only at its next catch-up, as a missing relation.
//
// The fixture that shipped with that fix could not see this: every generation
// in it had target table == source table, so asking about either gave the same
// answer. This one varies exactly that axis and nothing else.
func TestInferenceRetirementKeepsAnOutboxASecondTargetNeedsE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshOutboxOwnershipDatabase(c, ctx, dbURL, "ptah_outbox_owner")
	seedCLIArticles(c, ctx, db)
	// A second relation for the second generation's vectors, carrying the same
	// key so it is a target a specification could really name.
	_, err := db.ExecContext(ctx, `CREATE TABLE article_vectors (id BIGINT PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	// Both read public.articles. One writes into it, the other into
	// article_vectors, and that is the only difference between them.
	intoArticles := writeCLISpec(c, endpoint.URL)
	intoVectors := writeCLISpecWithTargetTable(c, endpoint.URL, "article_vectors")

	runInference(c, ctx, "prepare",
		"--spec", intoArticles, "--db-url", dbName, "--run-id", "into-articles")
	runInference(c, ctx, "prepare",
		"--spec", intoVectors, "--db-url", dbName, "--run-id", "into-vectors")

	// One outbox, shared. If prepare had installed two, the assertions below
	// would be about a table the retirement was never entitled to touch.
	c.Assert(outboxTriggerCount(c, ctx, db), qt.Equals, 2)
	c.Assert(outboxObjectsExist(c, ctx, db), qt.IsTrue)

	retiring := generationOfRun(c, ctx, db, "into-vectors")
	surviving := generationOfRun(c, ctx, db, "into-articles")
	c.Assert(retiring, qt.Not(qt.Equals), surviving)

	digest := retirementDigestOf(c, ctx, intoVectors, dbName, retiring)
	output := runInference(c, ctx, "retire",
		"--spec", intoVectors, "--db-url", dbName, "--generation", retiring,
		"--approve", digest, "--approver", "an operator")

	// The sentence names the source, because that is what the outbox is on.
	c.Assert(output, qt.Contains, "the outbox stays: 1 other generation(s) still read public.articles")
	c.Assert(output, qt.Not(qt.Contains), "the outbox is gone")
	c.Assert(outboxTriggerCount(c, ctx, db), qt.Equals, 2)
	c.Assert(outboxObjectsExist(c, ctx, db), qt.IsTrue)

	// And the capture still captures, which is the property the survivor
	// depends on. A count of triggers proves they are declared; a row proves
	// they fire.
	before := outboxEventCount(c, ctx, db)
	_, err = db.ExecContext(ctx,
		`UPDATE articles SET title = 'changed after the other retirement', updated_at = '8' WHERE id = 1`)
	c.Assert(err, qt.IsNil)
	c.Assert(outboxEventCount(c, ctx, db), qt.Equals, before+1)
}

// TestInferenceRetirementAnswersForItsOwnSchemaE2E is the same question asked
// of a bare relation name.
//
// The registry was searched by table name with no schema, while every other
// part of the lifecycle carries both halves. Two ordinary setups over
// same-named tables in different schemas therefore answered for each other:
// retiring every generation over one of them was told a reader remained, so its
// triggers stayed installed on a table nothing would ever read again, growing an
// event table with no consumer.
func TestInferenceRetirementAnswersForItsOwnSchemaE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshOutboxOwnershipDatabase(c, ctx, dbURL, "ptah_outbox_schema")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	_, err := db.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS vector`)
	c.Assert(err, qt.IsNil)
	seedSchemaScopedArticles(c, ctx, db, "alpha")
	seedSchemaScopedArticles(c, ctx, db, "beta")

	alphaSpec := writeSchemaScopedSpec(c, endpoint.URL, "alpha")
	betaSpec := writeSchemaScopedSpec(c, endpoint.URL, "beta")
	runInference(c, ctx, "prepare", "--spec", alphaSpec, "--db-url", dbName, "--run-id", "alpha")
	runInference(c, ctx, "prepare", "--spec", betaSpec, "--db-url", dbName, "--run-id", "beta")

	// Two outboxes, one per source, which is what makes the count a question
	// about schemas rather than about tables.
	c.Assert(schemaTriggerCount(c, ctx, db, "alpha"), qt.Equals, 2)
	c.Assert(schemaTriggerCount(c, ctx, db, "beta"), qt.Equals, 2)

	// And two SOURCE identities, which is stokaro/ptah#2724. The run recorded
	// `spec.Source.Table`, so both of these held the string `articles` while the
	// outboxes above were keyed on the qualified pair -- and OutboxFloor, which
	// matches readers by that string, gave each run the other's floor.
	//
	// Asserted through `prepare` rather than by seeding a row, because createRun
	// is the thing under test: the floor's own live test seeds runs directly and
	// stays green with the bare name restored.
	alphaSource := sourceOfRun(c, ctx, db, "alpha")
	betaSource := sourceOfRun(c, ctx, db, "beta")
	c.Assert(alphaSource, qt.Not(qt.Equals), betaSource)
	c.Assert(alphaSource, qt.Equals, embedpg.SourceIdentity("alpha", "articles"))
	c.Assert(betaSource, qt.Equals, embedpg.SourceIdentity("beta", "articles"))

	generation := generationOfRun(c, ctx, db, "alpha")
	digest := retirementDigestOf(c, ctx, alphaSpec, dbName, generation)
	output := runInference(c, ctx, "retire",
		"--spec", alphaSpec, "--db-url", dbName, "--generation", generation,
		"--approve", digest, "--approver", "an operator")

	c.Assert(output, qt.Contains, "the outbox is gone")
	c.Assert(output, qt.Contains, "alpha.articles")
	c.Assert(schemaTriggerCount(c, ctx, db, "alpha"), qt.Equals, 0)
	// The control, and the half a fix that simply always uninstalled would
	// break: the other schema is untouched.
	c.Assert(schemaTriggerCount(c, ctx, db, "beta"), qt.Equals, 2)
}

// TestInferenceRetirementRemovesTheOutboxTheGenerationNamesE2E separates which
// outbox is removed from how many readers were counted.
//
// The outbox to uninstall was constructed from the specification handed to the
// invocation, so a retirement run with a file naming another source would have
// taken another table's triggers off -- and every fixture here passes a file
// whose source matches, which is why nothing could see it. `--generation` names
// the generation explicitly and the registry records the document it was built
// from, so that record is what decides, the way rollback's already does
// (stokaro/ptah#2630).
func TestInferenceRetirementRemovesTheOutboxTheGenerationNamesE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshOutboxOwnershipDatabase(c, ctx, dbURL, "ptah_outbox_named")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	_, err := db.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS vector`)
	c.Assert(err, qt.IsNil)
	seedSchemaScopedArticles(c, ctx, db, "alpha")
	seedSchemaScopedArticles(c, ctx, db, "beta")

	alphaSpec := writeSchemaScopedSpec(c, endpoint.URL, "alpha")
	betaSpec := writeSchemaScopedSpec(c, endpoint.URL, "beta")
	runInference(c, ctx, "prepare", "--spec", alphaSpec, "--db-url", dbName, "--run-id", "alpha")
	runInference(c, ctx, "prepare", "--spec", betaSpec, "--db-url", dbName, "--run-id", "beta")
	c.Assert(schemaTriggerCount(c, ctx, db, "alpha"), qt.Equals, 2)
	c.Assert(schemaTriggerCount(c, ctx, db, "beta"), qt.Equals, 2)

	// Alpha's generation, retired while the operator holds beta's file.
	generation := generationOfRun(c, ctx, db, "alpha")
	digest := retirementDigestOf(c, ctx, betaSpec, dbName, generation)
	output := runInference(c, ctx, "retire",
		"--spec", betaSpec, "--db-url", dbName, "--generation", generation,
		"--approve", digest, "--approver", "an operator")

	c.Assert(output, qt.Contains, "the outbox is gone")
	c.Assert(output, qt.Contains, "alpha.articles")
	c.Assert(schemaTriggerCount(c, ctx, db, "alpha"), qt.Equals, 0)
	c.Assert(schemaTriggerCount(c, ctx, db, "beta"), qt.Equals, 2)
}

// TestInferenceRetirementReadsTheModeFromTheGenerationE2E covers the third way
// the wrong outbox answer was reached.
//
// Whether there was an outbox to remove came off the specification handed to
// THIS invocation. Retiring an outbox-built generation while passing a file
// that declares `immutable` therefore returned before asking anything, left the
// triggers installed, and printed no line about them at all -- the same silence
// the removal was added to end.
func TestInferenceRetirementReadsTheModeFromTheGenerationE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshOutboxOwnershipDatabase(c, ctx, dbURL, "ptah_outbox_mode")
	seedCLIArticles(c, ctx, db)
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	built := writeCLISpec(c, endpoint.URL)
	runInference(c, ctx, "prepare", "--spec", built, "--db-url", dbName, "--run-id", cliRunID)
	c.Assert(outboxTriggerCount(c, ctx, db), qt.Equals, 2)

	// The same source, the same target, the same column: only the consistency
	// mode differs, so nothing but the mode can decide the outcome.
	asImmutable := writeCLISpecWithMode(c, endpoint.URL, "immutable")
	generation := generationOfRun(c, ctx, db, cliRunID)
	digest := retirementDigestOf(c, ctx, asImmutable, dbName, generation)
	output := runInference(c, ctx, "retire",
		"--spec", asImmutable, "--db-url", dbName, "--generation", generation,
		"--approve", digest, "--approver", "an operator")

	c.Assert(output, qt.Contains, "the outbox is gone")
	c.Assert(outboxTriggerCount(c, ctx, db), qt.Equals, 0)
	c.Assert(outboxObjectsExist(c, ctx, db), qt.IsFalse)
}

// freshOutboxOwnershipDatabase makes a database of its own and hands back a
// connection and the URL that reaches it.
func freshOutboxOwnershipDatabase(
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

// seedSchemaScopedArticles creates one schema's copy of the source table.
func seedSchemaScopedArticles(c *qt.C, ctx context.Context, db *sql.DB, schema string) {
	c.Helper()
	for _, statement := range []string{
		fmt.Sprintf(`CREATE SCHEMA %s`, schema),
		fmt.Sprintf(`CREATE TABLE %s.articles (
			id BIGINT PRIMARY KEY, title TEXT, body TEXT, updated_at TEXT NOT NULL)`, schema),
		fmt.Sprintf(`INSERT INTO %s.articles (id, title, body, updated_at)
			VALUES (1, 'First', 'about pricing', '7')`, schema),
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// writeSchemaScopedSpec writes a specification reading and writing in one named
// schema, so two of them differ in nothing but that.
//
// Written here rather than through the shared template because the schema is
// the varying part and that template fixes it at `public`; threading a tenth
// parameter through it would touch every caller to serve one test.
func writeSchemaScopedSpec(c *qt.C, endpoint, schema string) string {
	c.Helper()
	document := fmt.Sprintf(`
version: 1
name: schema scoped articles
source:
  schema: %s
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
  schema: %s
  table: articles
  column: embedding
  representation: vector
  metric: cosine
consistency:
  mode: outbox
policy:
  require_exact_approval: true
  require_consistency_mode: true
`, schema, endpoint, schema)
	path := filepath.Join(c.TempDir(), schema+"-spec.yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// generationOfRun reads the generation a run was prepared for.
//
// Straight off the run row rather than through `status`, because the subject
// here is retirement and a status reader that changed shape would fail these
// tests for a reason that has nothing to do with an outbox.
func generationOfRun(c *qt.C, ctx context.Context, db *sql.DB, runID string) string {
	c.Helper()
	var generation string
	c.Assert(db.QueryRowContext(ctx,
		`SELECT generation_identity FROM ptah_embedding_run WHERE id = $1`, runID).
		Scan(&generation), qt.IsNil)
	return generation
}

// sourceOfRun reads what a run recorded as the source it reads.
//
// Straight off the row, because no verb prints it: it is written by createRun
// and read back by OutboxFloor, and those two agreeing is the whole subject.
func sourceOfRun(c *qt.C, ctx context.Context, db *sql.DB, runID string) string {
	c.Helper()
	var source string
	c.Assert(db.QueryRowContext(ctx,
		`SELECT source FROM ptah_embedding_run WHERE id = $1`, runID).
		Scan(&source), qt.IsNil)
	return source
}

// schemaTriggerCount counts Ptah's outbox triggers on one schema's articles.
func schemaTriggerCount(c *qt.C, ctx context.Context, db *sql.DB, schema string) int {
	c.Helper()
	var count int
	c.Assert(db.QueryRowContext(ctx,
		`SELECT count(*) FROM pg_trigger t
		   JOIN pg_class r ON r.oid = t.tgrelid
		   JOIN pg_namespace n ON n.oid = r.relnamespace
		  WHERE n.nspname = $1 AND r.relname = 'articles' AND NOT t.tgisinternal
		    AND t.tgname LIKE 'ptah_embedding_outbox%'`, schema).Scan(&count), qt.IsNil)
	return count
}

// outboxEventCount counts the rows the capture has recorded.
//
// The table's name carries a digest of the source's qualified name, so it is
// found by prefix rather than spelled out, the same way outboxObjectsExist
// finds it.
func outboxEventCount(c *qt.C, ctx context.Context, db *sql.DB) int {
	c.Helper()
	var table string
	c.Assert(db.QueryRowContext(ctx,
		`SELECT tablename FROM pg_tables
		  WHERE tablename LIKE 'ptah_embedding_outbox_articles%'`).Scan(&table), qt.IsNil)
	var count int
	c.Assert(db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT count(*) FROM %q`, table)).Scan(&count), qt.IsNil)
	return count
}

// TestInferenceAnImmutableGenerationIsNoOutboxReaderE2E closes the hole the
// first version of this change left, and it is the one a reviewer found rather
// than a measurement.
//
// Counting every live generation over the source counts generations that were
// never fed by the outbox. `immutable` installs none, so a generation in that
// mode over the same table would have kept the change capture installed for
// good: it holds the count above zero while any outbox generation is retired,
// and retiring it in turn removes nothing, because its own mode never put an
// outbox there to remove.
func TestInferenceAnImmutableGenerationIsNoOutboxReaderE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshOutboxOwnershipDatabase(c, ctx, dbURL, "ptah_outbox_immutable")
	seedCLIArticles(c, ctx, db)
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	// One outbox generation and one immutable one, over the same source. Only
	// the first installs anything.
	watching := writeCLISpec(c, endpoint.URL)
	frozen := writeCLISpecWithMetric(c, endpoint.URL, "cosine", "embedding_frozen")
	frozen = withImmutableMode(c, frozen)
	runInference(c, ctx, "prepare", "--spec", watching, "--db-url", dbName, "--run-id", "watching")
	runInference(c, ctx, "prepare", "--spec", frozen, "--db-url", dbName, "--run-id", "frozen")
	c.Assert(outboxTriggerCount(c, ctx, db), qt.Equals, 2)

	generation := generationOfRun(c, ctx, db, "watching")
	digest := retirementDigestOf(c, ctx, watching, dbName, generation)
	output := runInference(c, ctx, "retire",
		"--spec", watching, "--db-url", dbName, "--generation", generation,
		"--approve", digest, "--approver", "an operator")

	// The immutable generation is still live and still reads the same table,
	// and it is not a reader of the outbox.
	c.Assert(output, qt.Contains, "the outbox is gone")
	c.Assert(outboxTriggerCount(c, ctx, db), qt.Equals, 0)
	c.Assert(outboxObjectsExist(c, ctx, db), qt.IsFalse)
}

// withImmutableMode rewrites a specification's consistency mode in place.
//
// The shared template takes a mode or a column but not both, and this test
// needs a generation that differs from its sibling in exactly two ways: where
// its vectors go, and what watches the source.
func withImmutableMode(c *qt.C, specPath string) string {
	c.Helper()
	body, err := os.ReadFile(specPath)
	c.Assert(err, qt.IsNil)
	rewritten := strings.Replace(string(body), "  mode: outbox", "  mode: immutable", 1)
	c.Assert(rewritten, qt.Not(qt.Equals), string(body))
	path := filepath.Join(c.TempDir(), "immutable-spec.yaml")
	c.Assert(os.WriteFile(path, []byte(rewritten), 0o600), qt.IsNil)
	return path
}
