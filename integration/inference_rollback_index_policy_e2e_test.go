//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/embedspec"
	"ptah.run/internal/embedstore"
)

// TestInferenceRollbackRequiresTheDestinationIndex prevents a rollback that
// silently replaces indexed queries with a sequential scan. The invocation's
// current specification declares no index, while the destination's recorded
// specification does and the catalog contains none.
func TestInferenceRollbackRequiresTheDestinationIndex(t *testing.T) {
	fixture := newRollbackIndexFixture(t, "", "hnsw")

	retirement, retirementErr := runInferenceExpectingFailure(
		fixture.c, fixture.ctx, "retire",
		"--spec", fixture.currentPath, "--db-url", fixture.databaseURL,
		"--generation", fixture.destination)
	fixture.c.Assert(retirementErr, qt.IsNotNil)
	fixture.c.Assert(retirement, qt.Contains, "that way back is no longer eligible")
	fixture.c.Assert(retirement, qt.Not(qt.Contains),
		"can still be rolled back to this one")

	output, rollbackErr := runInferenceExpectingFailure(
		fixture.c, fixture.ctx, "rollback",
		"--spec", fixture.currentPath, "--db-url", fixture.databaseURL,
		"--to", fixture.destination)
	fixture.c.Assert(rollbackErr, qt.IsNotNil)
	fixture.c.Assert(output, qt.Contains,
		"the generation's index is absent or invalid")
	fixture.c.Assert(activePointerOf(fixture.c, fixture.ctx, fixture.db),
		qt.Equals, fixture.current)
}

// TestInferenceRollbackDoesNotInventAnIndexRequirement is the opposite
// control. The current specification declares an index, but the destination's
// recorded specification does not; an absent index therefore cannot refuse the
// rollback or make retirement treat the way back as ineligible.
func TestInferenceRollbackDoesNotInventAnIndexRequirement(t *testing.T) {
	fixture := newRollbackIndexFixture(t, "hnsw", "")

	retirement, retirementErr := runInferenceExpectingFailure(
		fixture.c, fixture.ctx, "retire",
		"--spec", fixture.currentPath, "--db-url", fixture.databaseURL,
		"--generation", fixture.destination)
	fixture.c.Assert(retirementErr, qt.IsNotNil)
	fixture.c.Assert(retirement, qt.Contains,
		"can still be rolled back to this one")
	fixture.c.Assert(retirement, qt.Contains, "that way back is still eligible")

	output, rollbackErr := runInferenceExpectingFailure(
		fixture.c, fixture.ctx, "rollback",
		"--spec", fixture.currentPath, "--db-url", fixture.databaseURL,
		"--to", fixture.destination)
	fixture.c.Assert(rollbackErr, qt.IsNil, qt.Commentf("%s", output))
	fixture.c.Assert(output, qt.Contains, "queries now read "+fixture.destination)
	fixture.c.Assert(output, qt.Not(qt.Contains), "index is absent or invalid")
	fixture.c.Assert(activePointerOf(fixture.c, fixture.ctx, fixture.db),
		qt.Equals, fixture.destination)
}

// rollbackIndexFixture is the live state shared by the opposite policy tests.
type rollbackIndexFixture struct {
	c           *qt.C
	ctx         context.Context
	db          *sql.DB
	databaseURL string
	currentPath string
	destination string
	current     string
}

// newRollbackIndexFixture builds a fresh, positioned destination generation
// with no physical index. The two callers vary only the index declarations in
// the invocation and recorded destination specifications.
func newRollbackIndexFixture(
	t *testing.T, currentIndex, destinationIndex string,
) rollbackIndexFixture {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	c.Cleanup(cancel)

	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	name := fmt.Sprintf("ptah_rbindex_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, name) })

	databaseURL := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", databaseURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = db.Close() })
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(embeddingsHandler(c))
	c.Cleanup(endpoint.Close)

	destinationDocument := defaultCLISpec(endpoint.URL)
	destinationDocument.column = "embedding_previous"
	destinationDocument.indexMethod = destinationIndex
	destinationPath := writeCLISpecFrom(c, destinationDocument)
	destination := specificationIdentity(c, destinationPath)

	const destinationRun = "rollback-destination"
	runInference(c, ctx, "prepare", "--spec", destinationPath,
		"--db-url", databaseURL, "--run-id", destinationRun)
	runInference(c, ctx, "backfill", "--spec", destinationPath,
		"--db-url", databaseURL, "--run-id", destinationRun,
		"--batch-rows", "10", "--batch-inputs", "10")
	runInference(c, ctx, "catchup", "--spec", destinationPath,
		"--db-url", databaseURL, "--run-id", destinationRun,
		"--batch-rows", "10")

	// The test controls index readiness independently from freshness. Record
	// the successful freshness measurement and a live maintenance window while
	// deliberately leaving the destination index absent.
	_, err = db.ExecContext(ctx, `UPDATE `+embedstore.GenerationTable+`
		SET verified_at = clock_timestamp(),
			maintained_until = clock_timestamp() + interval '1 hour'
		WHERE identity = $1`, destination)
	c.Assert(err, qt.IsNil)

	currentDocument := defaultCLISpec(endpoint.URL)
	currentDocument.column = "embedding_current"
	currentDocument.indexMethod = currentIndex
	currentPath := writeCLISpecFrom(c, currentDocument)
	const current = "current-generation"
	registerBareGenerationInColumn(
		c, ctx, db, currentPath, current, currentDocument.column)
	_, err = db.ExecContext(ctx, `INSERT INTO `+embedstore.PointerTable+` (
		target_schema, target_table, active_generation, previous_generation,
		cut_over_at, cut_over_by)
		VALUES ('public', 'articles', $1, $2, clock_timestamp(), 'test')`,
		current, destination)
	c.Assert(err, qt.IsNil)

	return rollbackIndexFixture{
		c: c, ctx: ctx, db: db, databaseURL: databaseURL,
		currentPath: currentPath, destination: destination, current: current,
	}
}

// specificationIdentity reads the generation identity from the exact file
// recorded by prepare.
func specificationIdentity(c *qt.C, path string) string {
	c.Helper()
	body, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	loaded, err := embedspec.Parse(body, path)
	c.Assert(err, qt.IsNil)
	return loaded.Spec.Identity().Digest
}
