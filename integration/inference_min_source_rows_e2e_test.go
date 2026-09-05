//go:build integration

// The corpus floor an environment can require.
//
// stokaro/ptah#2870's other half. The advisory says a generation covers no
// rows; this is what an environment does about it, and it is opt-in because an
// empty generation is not wrong -- a table backfilled before its first rows
// arrive is a specification doing what it says.
//
// Both tests drive the whole chain: the verification's count reaches the plan,
// the plan carries it into the digest and into the file an approver reads, and
// the policy decides against it. Nothing below the CLI is reachable by a unit
// test, because the plan is built from a live database.

package integration_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestInferenceRefusesACutoverBelowThePolicysCorpusFloorE2E is the refusal an
// environment asks for.
//
// Three source rows against a floor of five. The filter is not the point here
// -- the corpus is simply smaller than this environment moves queries onto.
func TestInferenceRefusesACutoverBelowThePolicysCorpusFloorE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	_, dbName := freshSourceOnlyDatabase(c, ctx, dbURL, "ptah_min_rows_refused")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	spec := defaultCLISpec(endpoint.URL)
	spec.targetTable = "articles"
	spec.minSourceRows = 5
	path := writeCLISpecFrom(c, spec)

	runInference(c, ctx, "prepare", "--spec", path, "--db-url", dbName, "--run-id", "floor")
	runInference(c, ctx, "backfill", "--spec", path, "--db-url", dbName, "--run-id", "floor")
	runInference(c, ctx, "catchup", "--spec", path, "--db-url", dbName, "--run-id", "floor")
	runInference(c, ctx, "verify", "--spec", path, "--db-url", dbName, "--run-id", "floor")

	planFile := filepath.Join(c.TempDir(), "cutover.plan")
	refused, err := runInferenceExpectingFailure(c, ctx,
		"cutover", "--spec", path, "--db-url", dbName, "--run-id", "floor",
		"--plan-file", planFile)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", refused))
	c.Assert(refused, qt.Contains,
		"this generation covers 3 source rows and this policy requires at least 5")

	// The count reached the file an approver signs, which the refusal does not
	// prove on its own: a plan whose digest distinguishes the count and whose
	// file does not is a fact somebody signed for and could not have checked.
	// #nosec G304 -- the path is this test's own temporary directory.
	written, readErr := os.ReadFile(planFile)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(written), qt.Contains, "source rows: 3")
}

// TestInferenceCutsOverWithNoCorpusFloorE2E is the control, and it is what
// makes the refusal opt-in rather than arrived.
//
// The same three rows, the same lifecycle, no floor. Without this a floor that
// refused whenever it was consulted would satisfy the test above and block
// every cutover in the suite.
func TestInferenceCutsOverWithNoCorpusFloorE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshSourceOnlyDatabase(c, ctx, dbURL, "ptah_min_rows_absent")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	spec := defaultCLISpec(endpoint.URL)
	spec.targetTable = "articles"
	path := writeCLISpecFrom(c, spec)

	runInference(c, ctx, "prepare", "--spec", path, "--db-url", dbName, "--run-id", "nofloor")
	runInference(c, ctx, "backfill", "--spec", path, "--db-url", dbName, "--run-id", "nofloor")
	runInference(c, ctx, "catchup", "--spec", path, "--db-url", dbName, "--run-id", "nofloor")
	runInference(c, ctx, "verify", "--spec", path, "--db-url", dbName, "--run-id", "nofloor")

	digest := planDigestOfRun(c, ctx, path, dbName, "nofloor")
	cutover := runInference(c, ctx, "cutover", "--spec", path, "--db-url", dbName,
		"--run-id", "nofloor", "--approve", digest, "--approver", "an operator")

	c.Assert(cutover, qt.Contains, "queries now read generation "+generationOfRun(c, ctx, db, "nofloor"))
}
