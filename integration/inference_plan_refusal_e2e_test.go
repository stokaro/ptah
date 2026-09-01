//go:build integration

// A blocked plan refuses, and `ptah inference plan` says so with its exit code.
//
// stokaro/ptah#2648 finding 1: create-first-generation.md promises "The plan
// refuses if the specification cannot be satisfied", and measured against
// PostgreSQL with pgvector the plan exited 0 for every case the sentence names.
// A CI job gating on the plan passed against a specification that could not
// run, and the run then failed at `prepare` — or, for an index method the
// server refuses, after the whole provider bill for the corpus had been paid.
//
// The decision existed the entire time: embedplan.Plan.Runnable() was computed,
// tested five times, and read by nothing. internal/embedguard reports exactly
// that shape and did not, because it matches a declaration by bare name and
// cobra's own Command.Runnable is called in cmd/atlas — the false negative that
// package's own doc comment names as the direction it accepts.
//
// This file drives the cobra tree an operator runs, because an exit code is the
// one thing no package-level test observes.

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

	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestInferencePlanRefusesABlockedSpecificationE2E measures the three shapes
// the promise names, and the control that keeps the refusal from being
// unconditional.
func TestInferencePlanRefusesABlockedSpecificationE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_plan_refusal_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	assertASatisfiableSpecificationStillPlans(c, ctx, endpoint.URL, dbName)
	assertAMissingSourceTableIsRefused(c, ctx, endpoint.URL, dbName)
	assertAMissingTargetTableIsRefused(c, ctx, endpoint.URL, dbName)
	assertAnUnbuildableIndexIsRefused(c, ctx, endpoint.URL, dbName)
	assertABuildableSparseIndexIsNotRefused(c, ctx, endpoint.URL, dbName)
	assertABlockedPlanStillLeavesItsRecord(c, ctx, endpoint.URL, dbName)
}

// assertASatisfiableSpecificationStillPlans is the control, and it runs first.
//
// Every refusal below is satisfied by a plan that refuses everything, and this
// is the only assertion that separates the fix from that. It is also what says
// the seeded database is the one the refusals are measured against.
//
// It carries a second guarantee, because writeCLISpec declares no index method:
// a specification asking for no index has no index the target could fail to
// build, and a buildability check that did not say so would refuse every
// generation that deliberately has none.
func assertASatisfiableSpecificationStillPlans(
	c *qt.C, ctx context.Context, endpoint, dbName string,
) {
	c.Helper()
	specPath := writeCLISpec(c, endpoint)

	output := runInference(c, ctx, "plan", "--spec", specPath, "--db-url", dbName)

	c.Assert(output, qt.Not(qt.Contains), "blocked:")
}

// assertAMissingSourceTableIsRefused is the case the issue leads with.
//
// It planned green with no blocker at all: the only trace was
// `source.estimated_rows = unknown`, whose stated reason is about cost and
// duration rather than about a table. Both halves are asserted — that the
// absence is NAMED, and that the exit code carries it — because printing the
// blocker and still exiting 0 is the state this started in.
func assertAMissingSourceTableIsRefused(
	c *qt.C, ctx context.Context, endpoint, dbName string,
) {
	c.Helper()
	specPath := writeCLISpecWithSourceTable(c, endpoint, "no_such_source")

	output, err := runInferenceExpectingFailure(
		c, ctx, "plan", "--spec", specPath, "--db-url", dbName)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains,
		"blocked: the source table is not there, so there is nothing to read from")
}

// assertAMissingTargetTableIsRefused is the case the issue's own list did not
// reach and its verification did: with the source present, an absent target
// table produced a completely clean plan — a measured row count, no blocker,
// exit 0 — and the run died at `prepare`.
func assertAMissingTargetTableIsRefused(
	c *qt.C, ctx context.Context, endpoint, dbName string,
) {
	c.Helper()
	specPath := writeCLISpecWithTargetTable(c, endpoint, "no_such_target")

	output, err := runInferenceExpectingFailure(
		c, ctx, "plan", "--spec", specPath, "--db-url", dbName)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains,
		"blocked: the target table is not there, so the generation's column has nowhere to go")
	c.Assert(output, qt.Not(qt.Contains), "blocked: the source table is not there")
}

// assertAnUnbuildableIndexIsRefused is the third case the promise names and the
// one that costs the most to get wrong.
//
// `vector_index` answering true says the server builds vector indexes, not that
// it builds this one. Measured on pgvector 0.8.1, ivfflat with sparsevec was
// reported as `target.capability.vector_index = true (measured)` with no
// blocker, and the run reached the index step having already paid the whole
// provider bill for the corpus.
func assertAnUnbuildableIndexIsRefused(
	c *qt.C, ctx context.Context, endpoint, dbName string,
) {
	c.Helper()
	specPath := writeCLISpecWithIndex(c, endpoint, "ivfflat", "sparsevec")

	output, err := runInferenceExpectingFailure(
		c, ctx, "plan", "--spec", specPath, "--db-url", dbName)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains, "blocked: the target database has no operator class")
}

// assertABuildableSparseIndexIsNotRefused is what makes the assertion above
// about the PAIR rather than about sparsevec.
//
// hnsw takes sparsevec_cosine_ops and ivfflat does not, both measured in the
// same catalog. A check that refused the representation would satisfy the test
// above and reject a specification the server accepts.
func assertABuildableSparseIndexIsNotRefused(
	c *qt.C, ctx context.Context, endpoint, dbName string,
) {
	c.Helper()
	specPath := writeCLISpecWithIndex(c, endpoint, "hnsw", "sparsevec")

	output := runInference(c, ctx, "plan", "--spec", specPath, "--db-url", dbName)

	c.Assert(output, qt.Not(qt.Contains), "blocked:")
}

// assertABlockedPlanStillLeavesItsRecord pins the ORDER, which is the half of
// this change that only a comment was holding.
//
// publishRelease publishes a blocked plan deliberately: the release states the
// generation, the document that proposed it, what it replaces and whether it
// can be rebuilt, and every one of those is true of a plan that cannot run yet.
// The proposal waiting on something is the one an operator most wants to
// circulate.
//
// So the refusal comes last. A `planRefusal` moved ahead of the publication
// would take that capability away silently, and every other assertion in this
// file would still pass -- they only read the exit code and the blockers.
func assertABlockedPlanStillLeavesItsRecord(
	c *qt.C, ctx context.Context, endpoint, dbName string,
) {
	c.Helper()
	specPath := writeCLISpecWithSourceTable(c, endpoint, "no_such_source")
	recordPath := filepath.Join(c.TempDir(), "release.json")

	output, err := runInferenceExpectingFailure(c, ctx, "plan",
		"--spec", specPath, "--db-url", dbName, "--evidence-file", recordPath)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
	written, readErr := os.ReadFile(recordPath)
	c.Assert(readErr, qt.IsNil, qt.Commentf("the refusal took the record with it"))
	c.Assert(len(written) > 0, qt.IsTrue)
}
