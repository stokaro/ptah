//go:build integration

// A generation whose target relation Ptah creates, and drops.
//
// stokaro/ptah#2624: a specification could already name a target table other
// than its source, and the whole lifecycle ran against one -- but the operator
// had to create the relation and its key column first, because `EnsureTarget`
// was `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` and nothing else. The layout
// existed as a shape and not as an object Ptah made.
//
// Two things follow that this file measures rather than assumes. Creating the
// relation is not enough on its own: Ptah creates it EMPTY and the write path
// was an UPDATE, so a backfill against a relation of Ptah's own would have
// matched no rows and reported success having written nothing. And a relation
// Ptah creates is a relation Ptah drops, so the retirement that removes it has
// to refuse one it did not create.

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

// ownedTargetTable is the relation these specifications ask Ptah to make.
const ownedTargetTable = "article_embeddings"

// TestInferencePreparesBackfillsAndRetiresATableItCreatedE2E is the layout end
// to end, from a database that does not have the relation to one that does not
// have it again.
//
// Through the CLI, because the claim is about verbs an operator runs. Each
// assertion is one an earlier stage could have satisfied by accident, so the
// counts are taken from the catalog rather than from what a verb printed:
// `prepare` reporting success is exactly what it did before this change, on a
// database where the relation was absent and the run went on to write nothing.
func TestInferencePreparesBackfillsAndRetiresATableItCreatedE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshSourceOnlyDatabase(c, ctx, dbURL, "ptah_owned_target")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	// The relation is not there before the run. Asserted, because everything
	// below is about Ptah making it and a fixture that had made it would prove
	// nothing.
	c.Assert(relationExists(c, ctx, db, ownedTargetTable), qt.IsFalse)

	spec := writeCLISpecOwningItsTable(c, endpoint.URL, ownedTargetTable)
	runInference(c, ctx, "prepare", "--spec", spec, "--db-url", dbName, "--run-id", "owned")

	c.Assert(relationExists(c, ctx, db, ownedTargetTable), qt.IsTrue)
	c.Assert(primaryKeyColumnsOf(c, ctx, db, ownedTargetTable), qt.DeepEquals, []string{"id"})
	c.Assert(referencedRelationOf(c, ctx, db, ownedTargetTable), qt.Equals, "articles")
	// Empty, which is the state that made the UPDATE-only write path a defect
	// rather than a detail.
	c.Assert(rowsIn(c, ctx, db, ownedTargetTable), qt.Equals, 0)

	runInference(c, ctx, "backfill", "--spec", spec, "--db-url", dbName, "--run-id", "owned")

	c.Assert(rowsIn(c, ctx, db, ownedTargetTable), qt.Equals, 3)
	c.Assert(vectorsIn(c, ctx, db, ownedTargetTable), qt.Equals, 3)

	runInference(c, ctx, "catchup", "--spec", spec, "--db-url", dbName, "--run-id", "owned")
	verified := runInference(c, ctx, "verify", "--spec", spec, "--db-url", dbName, "--run-id", "owned")
	c.Assert(verified, qt.Contains, "3 source rows, 3 target rows")
	c.Assert(verified, qt.Contains, "every deterministic layer passed")

	// Retirement needs a generation nothing reads, so this one is never made
	// active. The relation is what the retirement destroys, and the sentence
	// the operator reads has to name it.
	generation := generationOfRun(c, ctx, db, "owned")
	digest := retirementPlanDigestOf(c, ctx, spec, dbName, generation)
	retired := runInference(c, ctx, "retire", "--spec", spec, "--db-url", dbName,
		"--generation", generation, "--approve", digest, "--approver", "an operator")

	c.Assert(retired, qt.Contains, "the table public."+ownedTargetTable+" they were in")
	c.Assert(relationExists(c, ctx, db, ownedTargetTable), qt.IsFalse)
	// The application's own table is still there, with every row in it. This
	// is the assertion that separates "dropped the generation's relation" from
	// "dropped a relation".
	c.Assert(rowsIn(c, ctx, db, "articles"), qt.Equals, 3)
}

// TestInferenceRefusesARelationItDidNotCreateE2E is the refusal that keeps the
// layout from being a way to hand Ptah an application's table.
//
// Adopting it would do two things at once: overwrite whatever the application's
// own comment said, and authorize the retirement that follows to DROP the
// relation with every row the application keeps in it.
func TestInferenceRefusesARelationItDidNotCreateE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshSourceOnlyDatabase(c, ctx, dbURL, "ptah_owned_refuse")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	_, err := db.ExecContext(ctx,
		`CREATE TABLE `+ownedTargetTable+` (id BIGINT PRIMARY KEY, kept TEXT)`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, `INSERT INTO `+ownedTargetTable+` (id, kept) VALUES (1, 'mine')`)
	c.Assert(err, qt.IsNil)

	spec := writeCLISpecOwningItsTable(c, endpoint.URL, ownedTargetTable)
	refused, err := runInferenceExpectingFailure(c, ctx,
		"prepare", "--spec", spec, "--db-url", dbName, "--run-id", "refused")

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", refused))
	c.Assert(err.Error(), qt.Contains, "already exists and Ptah did not create it")
	// The relation and its row are untouched, which is the half a refusal that
	// merely printed something would not give.
	c.Assert(rowsIn(c, ctx, db, ownedTargetTable), qt.Equals, 1)
	c.Assert(columnExists(c, ctx, db, ownedTargetTable, "embedding"), qt.IsFalse)
}

// TestInferenceATargetTableOfItsOwnFollowsASourceDeleteE2E measures what the
// foreign key is for.
//
// The vectors are in a relation of their own, so nothing about deleting a
// source row would remove them by itself: without the reference they would
// stay, addressed by a key nothing has, and the next verification would report
// them as rows outside the generation's scope forever.
func TestInferenceATargetTableOfItsOwnFollowsASourceDeleteE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshSourceOnlyDatabase(c, ctx, dbURL, "ptah_owned_cascade")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	spec := writeCLISpecOwningItsTable(c, endpoint.URL, ownedTargetTable)
	runInference(c, ctx, "prepare", "--spec", spec, "--db-url", dbName, "--run-id", "cascade")
	runInference(c, ctx, "backfill", "--spec", spec, "--db-url", dbName, "--run-id", "cascade")
	c.Assert(rowsIn(c, ctx, db, ownedTargetTable), qt.Equals, 3)

	_, err := db.ExecContext(ctx, `DELETE FROM articles WHERE id = 2`)
	c.Assert(err, qt.IsNil)

	c.Assert(rowsIn(c, ctx, db, ownedTargetTable), qt.Equals, 2)
	c.Assert(rowsIn(c, ctx, db, "articles"), qt.Equals, 2)
}

// TestInferenceRefusesToDropATableThatStoppedBeingPtahsE2E is the guard on the
// one statement in this feature that takes rows with it.
//
// The registry says the generation stores its vectors in a relation of its
// own, and the relation says otherwise. That disagreement is reachable without
// anybody doing anything strange -- a specification edited to point an
// own-table generation at an existing relation, a restore that rebuilt the
// table without its comment, an operator who renamed one relation onto
// another -- and on the other side of it is DROP TABLE.
//
// Nothing else in the lifecycle refuses this. The plan is well formed, the
// approval matches it, the generation is not active and not a rollback target,
// and the row count is exactly what was approved. The comment is the only fact
// that separates a relation Ptah made from one it was merely pointed at.
func TestInferenceRefusesToDropATableThatStoppedBeingPtahsE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshSourceOnlyDatabase(c, ctx, dbURL, "ptah_owned_marker")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	spec := writeCLISpecOwningItsTable(c, endpoint.URL, ownedTargetTable)
	runInference(c, ctx, "prepare", "--spec", spec, "--db-url", dbName, "--run-id", "marker")
	runInference(c, ctx, "backfill", "--spec", spec, "--db-url", dbName, "--run-id", "marker")

	// The relation stops being Ptah's while the registry goes on saying it is.
	_, err := db.ExecContext(ctx,
		`COMMENT ON TABLE `+ownedTargetTable+` IS 'the application keeps this'`)
	c.Assert(err, qt.IsNil)

	generation := generationOfRun(c, ctx, db, "marker")
	digest := retirementPlanDigestOf(c, ctx, spec, dbName, generation)
	refused, err := runInferenceExpectingFailure(c, ctx, "retire",
		"--spec", spec, "--db-url", dbName, "--generation", generation,
		"--approve", digest, "--approver", "an operator")

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", refused))
	c.Assert(err.Error(), qt.Contains, "is not Ptah's to drop")
	c.Assert(relationExists(c, ctx, db, ownedTargetTable), qt.IsTrue)
	c.Assert(rowsIn(c, ctx, db, ownedTargetTable), qt.Equals, 3)
	// And the registry did not record a retirement the database refused, which
	// is what the transaction is for: a generation marked retired beside its
	// surviving relation is the state nothing afterwards can reason about.
	c.Assert(retiredAtOf(c, ctx, db, generation), qt.Equals, "")
}

// retiredAtOf reads whether the registry has recorded a retirement, as text so
// an unset timestamp is the empty string rather than a zero instant.
func retiredAtOf(c *qt.C, ctx context.Context, db *sql.DB, generation string) string {
	c.Helper()
	var retiredAt string
	c.Assert(db.QueryRowContext(ctx,
		`SELECT COALESCE(retired_at::text, '') FROM ptah_embedding_generation WHERE identity = $1`,
		generation).Scan(&retiredAt), qt.IsNil)
	return retiredAt
}

// freshSourceOnlyDatabase seeds a database that has the source relation and
// nothing else.
//
// Deliberately different from freshTwoRelationDatabase, which creates the
// target and fills it with the source's keys because the write path there is
// an UPDATE. Here the absence is the subject.
func freshSourceOnlyDatabase(
	c *qt.C, ctx context.Context, dbURL, prefix string,
) (*sql.DB, string) {
	c.Helper()
	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { adminDB.Close() })

	name := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, name) })

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { db.Close() })

	seedCLIArticles(c, ctx, db)
	return db, dbName
}

// relationExists asks the catalog rather than trying to read the relation,
// because a failing read cannot tell an absent relation from a broken one.
func relationExists(c *qt.C, ctx context.Context, db *sql.DB, table string) bool {
	c.Helper()
	var present bool
	c.Assert(db.QueryRowContext(ctx,
		`SELECT to_regclass($1) IS NOT NULL`, "public."+table).Scan(&present), qt.IsNil)
	return present
}

// columnExists asks whether a relation has a column.
func columnExists(c *qt.C, ctx context.Context, db *sql.DB, table, column string) bool {
	c.Helper()
	var present bool
	c.Assert(db.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2)`,
		table, column).Scan(&present), qt.IsNil)
	return present
}

// primaryKeyColumnsOf reads a relation's primary key, in key order.
func primaryKeyColumnsOf(c *qt.C, ctx context.Context, db *sql.DB, table string) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx, `SELECT attribute.attname
		FROM pg_constraint AS constraint_row
		JOIN LATERAL unnest(constraint_row.conkey) WITH ORDINALITY AS key(attnum, ordinality) ON TRUE
		JOIN pg_attribute AS attribute
			ON attribute.attrelid = constraint_row.conrelid AND attribute.attnum = key.attnum
		WHERE constraint_row.conrelid = to_regclass($1) AND constraint_row.contype = 'p'
		ORDER BY key.ordinality`, "public."+table)
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	columns := make([]string, 0, 2)
	for rows.Next() {
		var column string
		c.Assert(rows.Scan(&column), qt.IsNil)
		columns = append(columns, column)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return columns
}

// referencedRelationOf names what a relation's one foreign key points at, or
// nothing when it has none.
func referencedRelationOf(c *qt.C, ctx context.Context, db *sql.DB, table string) string {
	c.Helper()
	var referenced string
	c.Assert(db.QueryRowContext(ctx, `SELECT COALESCE(
		(SELECT confrelid::regclass::text FROM pg_constraint
		 WHERE conrelid = to_regclass($1) AND contype = 'f' LIMIT 1), '')`,
		"public."+table).Scan(&referenced), qt.IsNil)
	return referenced
}

// retirementPlanDigestOf reads the digest a retirement refuses with, which is
// what an approval is given for.
func retirementPlanDigestOf(
	c *qt.C, ctx context.Context, specPath, dbURL, generation string,
) string {
	c.Helper()
	refused, err := runInferenceExpectingFailure(c, ctx, "retire",
		"--spec", specPath, "--db-url", dbURL, "--generation", generation)
	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", refused))
	return planDigestFrom(c, refused)
}
