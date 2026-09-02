//go:build integration

// The two row shapes only a two-relation walk can produce.
//
// stokaro/ptah#2736 taught the verification walk to join the source and target
// relations when a specification names two. The join produces a position only
// the source has and a position only the target has -- neither of which one
// relation can hold, because there a row is both halves or it is nothing. Three
// places downstream still read every position as both, and stokaro/ptah#2781 is
// each of them:
//
//   - the walk canonicalized a target-only position as if it were a source row,
//     so a refusing null or empty policy turned every tombstone into a fatal
//     error for `verify`, `status` and the cutover they gate;
//   - a source-only position was handed over as a stored row, so it was counted
//     in the target-row total and reported as a row carrying no vector -- about
//     a row the target relation does not have;
//   - the WHERE that keeps a stray sidecar row out was emitted only when the
//     specification carried a filter, so a no-op `filter: 'id > 0'` turned a
//     blocking finding into a clean report on identical data.

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestInferenceARefusingInputPolicyDoesNotJudgeATargetOnlyRowE2E is the first
// defect and the control that keeps the policy in force.
//
// `preprocessing.null_policy: refuse` says what to do with a source row whose
// input field is NULL. A position the source does not have is not such a row:
// its input columns are NULL because the join had nothing to put there. Judging
// it by that policy stranded the generation -- and a repair that simply stopped
// canonicalizing would silence the policy for the rows it is actually about,
// which is what the second half of this test measures.
func TestInferenceARefusingInputPolicyDoesNotJudgeATargetOnlyRowE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshTwoRelationDatabase(c, ctx, dbURL, "ptah_refusing_policy")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	spec := defaultCLISpec(endpoint.URL)
	spec.targetTable = twoRelationTargetTable
	spec.nullPolicy = "refuse"
	specPath := writeCLISpecFrom(c, spec)

	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", "refusing")
	runInference(c, ctx, "backfill", "--spec", specPath, "--db-url", dbName, "--run-id", "refusing")
	runInference(c, ctx, "catchup", "--spec", specPath, "--db-url", dbName, "--run-id", "refusing")

	// A tombstone: the shape catch-up writes when a source row is deleted, and
	// the one the join exists to see.
	c.Assert(tombstonedTargetRow(c, ctx, db, generationOfRun(c, ctx, db, "refusing"), 99),
		qt.Equals, int64(1))

	// The walk reads it rather than refusing it, and the breakdown proves it was
	// read rather than dropped: a repair that filtered target-only positions out
	// of the corpus would also produce no refusal, and would report three.
	found := runInference(c, ctx, "verify", "--spec", specPath, "--db-url", dbName, "--run-id", "refusing")
	c.Assert(found, qt.Not(qt.Contains), "row refused by the specification")
	c.Assert(found, qt.Contains, "4 target rows (3 with a vector, 1 tombstoned)")
	c.Assert(found, qt.Contains, "every deterministic layer passed")

	// The control. A row the specification DOES ask for, whose input field is
	// NULL, is exactly what `refuse` is about, and the walk still refuses it.
	// Without this, deleting the canonicalization outright reads as the fix.
	_, err := db.ExecContext(ctx,
		`INSERT INTO articles (id, title, body, updated_at) VALUES (4, NULL, 'about refunds', '9')`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx,
		`INSERT INTO `+twoRelationTargetTable+` (id) VALUES (4)`)
	c.Assert(err, qt.IsNil)

	refused, err := runInferenceExpectingFailure(c, ctx,
		"verify", "--spec", specPath, "--db-url", dbName, "--run-id", "refusing")
	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", refused))
	c.Assert(err.Error(), qt.Contains, "row refused by the specification")
	c.Assert(err.Error(), qt.Contains, `field "title" is NULL and the null policy is "refuse"`)
}

// TestInferenceASourceRowWithNoTargetRowIsNotCountedAsOneE2E is the second
// defect: a position the target relation has no row for was handed over as a
// stored row.
//
// The count and the finding are asserted together because they are two ways of
// saying the same wrong thing, and each survives a repair of the other. The
// count is what an operator reads first; the `vector_validity` finding is what
// blocks the cutover, describing a row that does not exist.
func TestInferenceASourceRowWithNoTargetRowIsNotCountedAsOneE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshTwoRelationDatabase(c, ctx, dbURL, "ptah_absent_target_row")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	specPath := writeCLISpecWithTargetTable(c, endpoint.URL, twoRelationTargetTable)
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", "absent")
	runInference(c, ctx, "backfill", "--spec", specPath, "--db-url", dbName, "--run-id", "absent")
	runInference(c, ctx, "catchup", "--spec", specPath, "--db-url", dbName, "--run-id", "absent")

	// The control: with a row per source row, the two counts agree.
	clean := runInference(c, ctx, "verify", "--spec", specPath, "--db-url", dbName, "--run-id", "absent")
	c.Assert(clean, qt.Contains, "3 source rows, 3 target rows")
	c.Assert(clean, qt.Contains, "every deterministic layer passed")

	c.Assert(deleteTwoRelationTargetRow(c, ctx, db, 2), qt.Equals, int64(1))
	c.Assert(rowsIn(c, ctx, db, twoRelationTargetTable), qt.Equals, 2)

	found, err := runInferenceExpectingFailure(c, ctx,
		"verify", "--spec", specPath, "--db-url", dbName, "--run-id", "absent")
	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", found))
	// Two, which is how many rows the relation holds.
	c.Assert(found, qt.Contains, "3 source rows, 2 target rows")
	c.Assert(found, qt.Contains, "1 in-scope source rows have no vector in this generation")
	// And nothing claiming a stored row carries no vector, because no such row
	// is stored.
	c.Assert(found, qt.Not(qt.Contains), "carry no vector and are not marked skipped or deleted")
}

// TestInferenceATwoRelationVerdictIgnoresAnUnrelatedFilterE2E is the third
// defect: the verdict for one database depended on whether the specification
// carried a filter that had nothing to do with the row.
//
// Two runs over the same rows, in two databases seeded identically, whose
// specifications differ only by a filter that excludes nothing. The control is
// the second half: a filter that DOES exclude something still narrows the walk,
// so the repair is not "the filter stopped mattering".
func TestInferenceATwoRelationVerdictIgnoresAnUnrelatedFilterE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	unfiltered := defaultCLISpec(endpoint.URL)
	unfiltered.targetTable = twoRelationTargetTable
	// A predicate every seeded row satisfies, so it narrows nothing and the two
	// runs are asked about the same corpus.
	noop := unfiltered
	noop.filter = "id > 0"

	withoutFilter := aStraySidecarRun(c, ctx, dbURL, "ptah_stray_unfiltered",
		writeCLISpecFrom(c, unfiltered))
	withNoOpFilter := aStraySidecarRun(c, ctx, dbURL, "ptah_stray_noop_filter",
		writeCLISpecFrom(c, noop))

	c.Assert(withoutFilter, qt.Contains, "3 source rows, 3 target rows")
	c.Assert(withoutFilter, qt.Contains, "every deterministic layer passed")
	c.Assert(withNoOpFilter, qt.Contains, "3 source rows, 3 target rows")
	c.Assert(withNoOpFilter, qt.Contains, "every deterministic layer passed")

	// The control. A filter that excludes a real row still excludes it, so the
	// walk did not stop reading the filter.
	narrowing := unfiltered
	narrowing.filter = "id < 3"
	narrowed := aStraySidecarRun(c, ctx, dbURL, "ptah_stray_narrowed",
		writeCLISpecFrom(c, narrowing))
	c.Assert(narrowed, qt.Contains, "2 source rows")
}

// TestInferenceACompositeKeyJoinsOnEveryComponentE2E pins the one piece of key
// logic the joined shape has that the single-relation shape does not.
//
// Every other fixture here declares `key_fields: [id]`, and a one-element join
// condition renders identically however its parts are combined -- so joining
// the components with OR instead of AND passes the whole suite. These rows
// share a component pairwise on purpose: under OR each source row matches three
// target rows, the counts multiply and one key arrives twice.
func TestInferenceACompositeKeyJoinsOnEveryComponentE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshCompositeKeyDatabase(c, ctx, dbURL)
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	spec := defaultCLISpec(endpoint.URL)
	spec.sourceTable, spec.targetTable = "documents", "document_vectors"
	spec.keyFields = []string{"tenant", "id"}
	specPath := writeCLISpecFrom(c, spec)

	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", "composite")
	runInference(c, ctx, "backfill", "--spec", specPath, "--db-url", dbName, "--run-id", "composite")
	runInference(c, ctx, "catchup", "--spec", specPath, "--db-url", dbName, "--run-id", "composite")

	verified := runInference(c, ctx,
		"verify", "--spec", specPath, "--db-url", dbName, "--run-id", "composite")
	c.Assert(verified, qt.Contains, "3 source rows, 3 target rows")
	c.Assert(verified, qt.Contains, "every deterministic layer passed")
	c.Assert(vectorsInComposite(c, ctx, db), qt.Equals, 3)
}

// aStraySidecarRun seeds a database, runs the lifecycle, adds a target row for
// a key the source does not have and that nothing ever wrote, and returns what
// `verify` printed.
//
// A row carrying no generation belongs to no generation, so no generation's
// verification is where it is reported. It is the row the joined WHERE drops,
// and the one whose treatment used to depend on an unrelated filter.
func aStraySidecarRun(
	c *qt.C, ctx context.Context, dbURL, prefix, specPath string,
) string {
	c.Helper()
	db, dbName := freshTwoRelationDatabase(c, ctx, dbURL, prefix)
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", "stray")
	runInference(c, ctx, "backfill", "--spec", specPath, "--db-url", dbName, "--run-id", "stray")
	runInference(c, ctx, "catchup", "--spec", specPath, "--db-url", dbName, "--run-id", "stray")

	result, err := db.ExecContext(ctx,
		`INSERT INTO `+twoRelationTargetTable+` (id) VALUES (77)`)
	c.Assert(err, qt.IsNil)
	affected, err := result.RowsAffected()
	c.Assert(err, qt.IsNil)
	c.Assert(affected, qt.Equals, int64(1),
		qt.Commentf("the fixture added no stray row, so the assertion would measure nothing"))

	return runInference(c, ctx, "verify", "--spec", specPath, "--db-url", dbName, "--run-id", "stray")
}

// freshCompositeKeyDatabase seeds two relations keyed by two columns.
//
// The three rows share a component pairwise -- ("a",1), ("b",1), ("a",2) -- so
// no single component identifies a row. That is what makes the join condition's
// conjunction observable.
func freshCompositeKeyDatabase(
	c *qt.C, ctx context.Context, dbURL string,
) (*sql.DB, string) {
	c.Helper()
	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { adminDB.Close() })

	name := fmt.Sprintf("ptah_composite_key_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, name) })

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { db.Close() })

	for _, statement := range []string{
		`CREATE EXTENSION IF NOT EXISTS vector`,
		`CREATE TABLE documents (
			tenant TEXT NOT NULL, id BIGINT NOT NULL,
			title TEXT, body TEXT, updated_at TEXT NOT NULL,
			PRIMARY KEY (tenant, id))`,
		`INSERT INTO documents (tenant, id, title, body, updated_at) VALUES
			('a', 1, 'Alpha one', 'about pricing', '7'),
			('b', 1, 'Beta one',  'about support', '7'),
			('a', 2, 'Alpha two', 'about billing', '7')`,
		`CREATE TABLE document_vectors (
			tenant TEXT NOT NULL, id BIGINT NOT NULL, PRIMARY KEY (tenant, id))`,
		`INSERT INTO document_vectors (tenant, id) SELECT tenant, id FROM documents`,
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return db, dbName
}

// tombstonedTargetRow marks a key as deleted in the target relation, which is
// what catch-up writes when a source row goes away.
func tombstonedTargetRow(
	c *qt.C, ctx context.Context, db *sql.DB, generation string, id int64,
) int64 {
	c.Helper()
	// #nosec G201 -- the relation is this file's own constant, and the columns
	// are the ones the shared specification names; every value is bound.
	result, err := db.ExecContext(ctx, `INSERT INTO `+twoRelationTargetTable+`
		(id, embedding_generation, embedding_input_hash,
		 embedding_source_version, embedding_state)
		VALUES ($1, $2, 'x', '7', 'tombstone')`, id, generation)
	c.Assert(err, qt.IsNil)
	affected, err := result.RowsAffected()
	c.Assert(err, qt.IsNil)
	return affected
}

// rowsIn counts what a relation actually holds, which is the number a
// target-row total is a claim about.
func rowsIn(c *qt.C, ctx context.Context, db *sql.DB, table string) int {
	c.Helper()
	var count int
	// #nosec G201 -- the relation is this file's own constant.
	c.Assert(db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count), qt.IsNil)
	return count
}

// vectorsInComposite counts the vectors the composite-key target holds.
func vectorsInComposite(c *qt.C, ctx context.Context, db *sql.DB) int {
	c.Helper()
	var count int
	c.Assert(db.QueryRowContext(ctx,
		"SELECT count(*) FROM document_vectors WHERE embedding IS NOT NULL").Scan(&count), qt.IsNil)
	return count
}
