//go:build integration

// A generation that covers no source rows.
//
// stokaro/ptah#2870: every layer passes over an empty corpus, and that reads
// exactly like every layer passing. The reachable cause is not an empty table
// but a `source.filter` with a typo in it — the backfill embeds nothing, the
// verification reads the same nothing through the same predicate, and the two
// agree perfectly.

package integration_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestInferenceSaysWhenAGenerationCoversNoRowsE2E is the issue's reproduction,
// driven the way it was reported.
//
// Through the CLI because the claim is about what an operator and a pipeline
// read. The filter is one an author can write by accident over a table whose
// ids are 1..3.
func TestInferenceSaysWhenAGenerationCoversNoRowsE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	_, dbName := freshSourceOnlyDatabase(c, ctx, dbURL, "ptah_empty_corpus")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	spec := defaultCLISpec(endpoint.URL)
	spec.filter = "id > 100000"
	path := writeCLISpecFrom(c, spec)

	runInference(c, ctx, "prepare", "--spec", path, "--db-url", dbName, "--run-id", "empty")
	runInference(c, ctx, "backfill", "--spec", path, "--db-url", dbName, "--run-id", "empty")
	runInference(c, ctx, "catchup", "--spec", path, "--db-url", dbName, "--run-id", "empty")
	verified := runInference(c, ctx, "verify", "--spec", path, "--db-url", dbName, "--run-id", "empty")

	c.Assert(verified, qt.Contains, "0 source rows, 0 target rows")
	c.Assert(verified, qt.Contains, "[coverage/advisory] this generation covers no source rows")
	// It does not refuse: `verify` exits 0, which runInference has already
	// asserted, and no finding is blocking.
	c.Assert(verified, qt.Not(qt.Contains), "[coverage/blocking]")

	// The half #2870 leaves open, asserted so the scope of this change is
	// written down rather than inferred. An empty generation is a
	// specification doing what it says -- a table backfilled before its first
	// rows arrive is the legitimate case -- so whether an environment can
	// require otherwise is a policy question, and readiness is unchanged.
	status := runInference(c, ctx, "status", "--spec", path, "--db-url", dbName, "--run-id", "empty")
	c.Assert(status, qt.Contains, "cutover ready: true")
	c.Assert(status, qt.Contains, "this generation covers no source rows")
}

// TestInferenceSaysNothingWhenAGenerationCoversRowsE2E is the control.
//
// An advisory that appeared on every run would satisfy the assertion above and
// stop being read, which is the failure mode of a finding nobody can act on.
// The same lifecycle without the filter must not carry it.
func TestInferenceSaysNothingWhenAGenerationCoversRowsE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	_, dbName := freshSourceOnlyDatabase(c, ctx, dbURL, "ptah_full_corpus")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	spec := defaultCLISpec(endpoint.URL)
	spec.targetTable = "articles"
	path := writeCLISpecFrom(c, spec)

	runInference(c, ctx, "prepare", "--spec", path, "--db-url", dbName, "--run-id", "full")
	runInference(c, ctx, "backfill", "--spec", path, "--db-url", dbName, "--run-id", "full")
	runInference(c, ctx, "catchup", "--spec", path, "--db-url", dbName, "--run-id", "full")
	verified := runInference(c, ctx, "verify", "--spec", path, "--db-url", dbName, "--run-id", "full")

	c.Assert(verified, qt.Contains, "3 source rows")
	c.Assert(verified, qt.Not(qt.Contains), "this generation covers no source rows")
}
