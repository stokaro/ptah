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
)

// TestInferenceAcceptedFindingsE2E is stokaro/ptah#2649.
//
// `policy.allow_accepted_findings` is documented, parsed, carried into the
// decision and bound into the plan digest -- and nothing populated
// `Evidence.AcceptedFindings`, so `len(...) == 0` refused first and the policy
// was never consulted. An operator setting it got "verification did not pass
// and nothing was accepted", forever, with no way to accept anything.
//
// The missing half was the operator's input. `--accept-finding` names a
// blocking finding by its exact summary, and the plan records both what was
// accepted and what was not.
//
// Five cases, and the fifth is the one that decides whether this is safe.
func TestInferenceAcceptedFindingsE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_accept_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	permissive := writeAcceptingSpec(c, endpoint.URL, "embedding_accept", true)
	const runID = "accept-1"
	prepareBlockedGeneration(c, ctx, db, permissive, dbName, runID, "embedding_accept")

	blocking := blockingSummaries(c, ctx, permissive, dbName, runID)
	c.Assert(len(blocking) >= 1, qt.IsTrue, qt.Commentf("nothing blocks, so nothing can be accepted"))

	assertNothingAcceptedIsRefused(c, ctx, permissive, dbName, runID)
	assertASummaryNothingSaysIsRefused(c, ctx, permissive, dbName, runID)
	assertAPolicyThatForbidsItRefuses(c, ctx, db, endpoint.URL, dbName)
	assertNamingOneFindingTwiceIsOneAcceptance(c, ctx, permissive, dbName, runID, blocking)
	assertAcceptingEveryBlockerCutsOver(c, ctx, db, permissive, dbName, runID, blocking)
}

// prepareBlockedGeneration builds a generation whose CONSISTENCY is satisfied
// and whose verification is not.
//
// The distinction is the whole fixture. A run that has not caught up is refused
// by the consistency decision, which acceptance does not reach and must not:
// accepting "changes between them are unprocessed" would be accepting a cutover
// onto a generation nobody claims covers the source. So the catch-up runs, and
// the blocker is made afterwards by blanking one row's vector THROUGH THE
// GENERATION'S OWN COLUMNS -- which the outbox trigger's WHEN clause excludes by
// design, so the source is untouched and the barrier stays reached.
func prepareBlockedGeneration(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL, runID, column string,
) {
	c.Helper()
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID, "--batch-rows", "10")

	_, err := db.ExecContext(ctx, fmt.Sprintf(
		`UPDATE articles SET %q = NULL, %q = NULL, %q = NULL, %q = NULL, %q = NULL
		 WHERE id = (SELECT MIN(id) FROM articles)`,
		column, column+"_generation", column+"_input_hash",
		column+"_source_version", column+"_state"))
	c.Assert(err, qt.IsNil)
}

// assertNothingAcceptedIsRefused is the state before the flag.
func assertNothingAcceptedIsRefused(
	c *qt.C, ctx context.Context, specPath, dbURL, runID string,
) {
	c.Helper()
	output, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID)

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "nothing was accepted")
}

// assertASummaryNothingSaysIsRefused keeps an acceptance from outliving its
// finding.
//
// An acceptance gets copied into a runbook, a pipeline, a shell history. One
// that silently applies to nothing is an operator believing they have looked at
// something they have not, so a summary matching no blocking finding is refused
// rather than ignored.
func assertASummaryNothingSaysIsRefused(
	c *qt.C, ctx context.Context, specPath, dbURL, runID string,
) {
	c.Helper()
	output, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID,
		"--accept-finding", "a finding this report does not carry")

	c.Assert(err, qt.IsNotNil)
	c.Assert(output+err.Error(), qt.Contains, "there is nothing there to accept")
}

// assertAPolicyThatForbidsItRefuses is the policy half.
//
// The specification decides whether accepting is permitted at all; the flag
// decides what is accepted. A build honoring the flag without the policy would
// make the specification's statement decorative.
func assertAPolicyThatForbidsItRefuses(
	c *qt.C, ctx context.Context, db *sql.DB, endpointURL, dbURL string,
) {
	c.Helper()
	strict := writeAcceptingSpec(c, endpointURL, "embedding_strict", false)
	const runID = "accept-strict"
	prepareBlockedGeneration(c, ctx, db, strict, dbURL, runID, "embedding_strict")

	accepting := []string{"--spec", strict, "--db-url", dbURL, "--run-id", runID}
	for _, summary := range blockingSummaries(c, ctx, strict, dbURL, runID) {
		accepting = append(accepting, "--accept-finding", summary)
	}
	output, err := runInferenceExpectingFailure(c, ctx, append([]string{"cutover"}, accepting...)...)

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "does not permit accepting findings")
}

// assertNamingOneFindingTwiceIsOneAcceptance keeps a repeated flag out of the
// plan digest.
//
// `--accept-finding` is repeatable, and an operator assembling it from a loop or
// a runbook can pass the same summary twice. Carrying the repeat into
// AcceptedFindings would put it in the digest, so two invocations accepting
// exactly the same thing would be two different plans -- and the digest a
// refusal published would not match the plan the approval is offered against.
//
// The assertion is that the two refusals publish the SAME digest. Asserting
// only that both are refused passes whatever the encoding does with the repeat.
func assertNamingOneFindingTwiceIsOneAcceptance(
	c *qt.C, ctx context.Context, specPath, dbURL, runID string, blocking []string,
) {
	c.Helper()
	once := []string{"cutover", "--spec", specPath, "--db-url", dbURL, "--run-id", runID}
	for _, summary := range blocking {
		once = append(once, "--accept-finding", summary)
	}
	twice := append(append([]string(nil), once...), "--accept-finding", blocking[0])

	first, err := runInferenceExpectingFailure(c, ctx, once...)
	c.Assert(err, qt.IsNotNil)
	second, err := runInferenceExpectingFailure(c, ctx, twice...)
	c.Assert(err, qt.IsNotNil)

	c.Assert(planDigestFrom(c, second), qt.Equals, planDigestFrom(c, first))
}

// assertAcceptingEveryBlockerCutsOver is the capability, and the case that
// decides whether it is safe.
//
// Accepting ONE of several blocking findings must not carry the others. The
// first assertion is that partial acceptance is refused and names what was left
// -- a build reading only the accepted list lets those through, which is an
// acceptance for one finding authorizing a cutover over findings nobody looked
// at. Only then is the full acceptance allowed.
func assertAcceptingEveryBlockerCutsOver(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL, runID string, blocking []string,
) {
	c.Helper()
	partial := []string{"--spec", specPath, "--db-url", dbURL, "--run-id", runID,
		"--accept-finding", blocking[0]}
	output, err := runInferenceExpectingFailure(c, ctx, append([]string{"cutover"}, partial...)...)
	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "was not accepted")

	full := []string{"--spec", specPath, "--db-url", dbURL, "--run-id", runID}
	for _, summary := range blocking {
		full = append(full, "--accept-finding", summary)
	}
	refused, err := runInferenceExpectingFailure(c, ctx, append([]string{"cutover"}, full...)...)
	c.Assert(err, qt.IsNotNil)
	c.Assert(refused, qt.Not(qt.Contains), "was not accepted")

	approved := append(full,
		"--approve", planDigestFrom(c, refused), "--approver", "an operator")
	runInference(c, ctx, append([]string{"cutover"}, approved...)...)

	// The pointer, not the message. A cutover that printed success and moved
	// nothing satisfies an assertion on stdout.
	var active string
	c.Assert(db.QueryRowContext(ctx,
		`SELECT active_generation FROM ptah_embedding_pointer`).Scan(&active), qt.IsNil)
	c.Assert(active, qt.Not(qt.Equals), "")
}

// blockingSummaries reads the blocking findings out of `verify`.
//
// Off the verb rather than out of a literal in this file. A summary carries
// counts, so a literal would be a second answer to what the product says and
// would rot the first time a count moved -- and the acceptance flag matches by
// exact summary, so a stale literal reads as "accepting works" while accepting
// nothing.
func blockingSummaries(c *qt.C, ctx context.Context, specPath, dbURL, runID string) []string {
	c.Helper()
	output, err := runInferenceExpectingFailure(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	c.Assert(err, qt.IsNotNil)

	var summaries []string
	for line := range strings.SplitSeq(output, "\n") {
		after, found := strings.CutPrefix(strings.TrimSpace(line), "- [")
		layer, rest, split := strings.Cut(after, "] ")
		summaries = appendBlocking(summaries, found && split, layer, rest)
	}
	return summaries
}

// appendBlocking adds one printed finding when it is a blocking one.
//
// A helper rather than a conditional in the loop above, which the style rule
// forbids: the branch is which lines are findings and which are the report's
// other bullets.
func appendBlocking(summaries []string, matched bool, layer, summary string) []string {
	if !matched || !strings.HasSuffix(layer, "/blocking") {
		return summaries
	}
	return append(summaries, summary)
}

// writeAcceptingSpec writes a specification with a chosen acceptance policy.
func writeAcceptingSpec(c *qt.C, endpoint, column string, allow bool) string {
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
  representation: vector
  metric: cosine
consistency:
  mode: outbox
policy:
  require_exact_approval: true
  allow_accepted_findings: %t
`, endpoint, column, allow)
	path := filepath.Join(c.TempDir(), "spec-accept-"+column+".yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}
