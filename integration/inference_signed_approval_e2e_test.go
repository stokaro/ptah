//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"go.5x5.cz/ptah/cmd/root"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/embedrelease"
)

// TestInferenceSignedApprovalE2E is a cutover authorized by a signature rather
// than by a name somebody typed.
//
// `--approve <digest> --approver "a name"` records an assertion: it says who the
// operator wrote down, and a shell history cannot say more. A signature says
// whose key covered these exact bytes, which is the question asked six months
// later. It reuses the mechanism the schema surface already has -- OpenSSH
// detached signatures over a file, verified against a committed allowed-signers
// list -- rather than inventing one (stokaro/ptah#2068).
//
// End to end because the halves are in different places: the plan file is
// written by the cutover verb, signed by `ptah schema approve`, and verified
// back into an approval bound to a plan the database decides. A package test
// can measure any one of those and not the chain.
//
// It does not skip when ssh-keygen is absent, and the absence is not a reason
// to. An integration test that skips reads as one that passed -- which is why
// the contour runner fails on a skip -- and a runner with no OpenSSH is one
// where this capability genuinely does not work. The unit tests beside
// cmd/inference skip, correctly: they are not the contour.
func TestInferenceSignedApprovalE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_signed_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()
	specPath := writeCLISpec(c, endpoint.URL)

	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "verify", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)

	planPath, allowedSigners := assertARefusedCutoverWritesAPlanToSign(c, ctx, specPath, dbName)
	assertAnUnsignedPlanIsRefusedAsUnreviewed(c, ctx, specPath, dbName, planPath, allowedSigners)
	assertASignedPlanCutsOverAndRecordsThePrincipal(c, ctx, db, specPath, dbName, planPath, allowedSigners)
}

// assertARefusedCutoverWritesAPlanToSign is the first step of the flow.
//
// The file names the operation and what it would do rather than only the
// digest: a signature over sixty-four hex characters attests to a number nobody
// could have checked.
func assertARefusedCutoverWritesAPlanToSign(
	c *qt.C, ctx context.Context, specPath, dbURL string,
) (planPath, allowedSigners string) {
	c.Helper()
	dir := c.TempDir()
	planPath = filepath.Join(dir, "cutover.plan")

	output, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID, "--plan-file", planPath)

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "cutover refused")
	c.Assert(output, qt.Contains, "sign it with")

	body, err := os.ReadFile(planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(body), qt.Contains, "ptah inference cutover plan")
	c.Assert(string(body), qt.Contains, "target: public.articles.embedding")
	c.Assert(string(body), qt.Contains, "plan: ")
	// The digest in the file is the one the verb printed, which is what makes
	// the signature bind to the plan the operator read.
	c.Assert(string(body), qt.Contains, planDigestFrom(c, output))
	// And the verification line names a MEASUREMENT rather than the generation.
	//
	// It held spec.Identity().Digest, so the approver was shown the same
	// sixty-four characters twice under two labels and the plan digest did not
	// move when the report changed -- an approval that bound to no measurement
	// at all (stokaro/ptah#2643). Asserting the two lines differ is the whole
	// defect: they were byte-identical in every plan the audit produced.
	c.Assert(planFieldFrom(c, string(body), "verification"), qt.Not(qt.Equals),
		planFieldFrom(c, string(body), "generation"))
	c.Assert(planFieldFrom(c, string(body), "verification"), qt.Not(qt.Equals), "")

	return planPath, anAllowedSignersFile(c, dir, "alice@example.com")
}

// assertAnUnsignedPlanIsRefusedAsUnreviewed keeps the two problems apart.
//
// A plan nobody signed and a signature that does not check out call for
// different actions, and telling an operator their plan was tampered with when
// it was merely unreviewed sends them looking for an attacker.
func assertAnUnsignedPlanIsRefusedAsUnreviewed(
	c *qt.C, ctx context.Context, specPath, dbURL, planPath, allowedSigners string,
) {
	c.Helper()
	_, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--approval", planPath, "--allowed-signers", allowedSigners)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "carries no approval")
}

// assertASignedPlanCutsOverAndRecordsThePrincipal is the whole point.
//
// The approver recorded on the pointer is the principal the signature verified
// as, not a string the command line carried -- and this run gives no
// --approver at all, so a recorded name can only have come from the signature.
func assertASignedPlanCutsOverAndRecordsThePrincipal(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL, planPath, allowedSigners string,
) {
	c.Helper()
	keyPath := filepath.Join(filepath.Dir(allowedSigners), "id_ed25519")
	signed := runNative(c, ctx, "schema", "approve", "--plan", planPath, "--key", keyPath)
	c.Assert(signed, qt.Contains, "Approved "+planPath)

	evidencePath := filepath.Join(filepath.Dir(planPath), "cutover.json")
	output := runInference(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--approval", planPath, "--allowed-signers", allowedSigners,
		"--evidence-file", evidencePath)

	c.Assert(output, qt.Contains, "queries now read generation ")

	// The record says the approval was a signature rather than a name.
	//
	// Two cutovers of one target -- this one, authorized by alice's key, and
	// the one in the release-evidence suite, authorized by anybody who could
	// type "an operator" -- were identical in every published field
	// (stokaro/ptah#2643 finding 4). That suite asserts the false half.
	body, err := os.ReadFile(evidencePath)
	c.Assert(err, qt.IsNil)
	var record embedrelease.Cutover
	c.Assert(json.Unmarshal(body, &record), qt.IsNil)
	c.Assert(record.ApprovalSigned, qt.IsTrue)
	c.Assert(record.Approver, qt.Equals, "alice@example.com")
	// The record cites what the plan cited, so the two cannot disagree about
	// what "verification digest" means.
	c.Assert(record.VerificationDigest, qt.Equals,
		planFieldFrom(c, planText(c, planPath), "verification"))

	var approver string
	c.Assert(db.QueryRowContext(ctx,
		`SELECT cut_over_by FROM ptah_embedding_pointer WHERE target_table = 'articles'`).
		Scan(&approver), qt.IsNil)
	c.Assert(approver, qt.Equals, "alice@example.com")
}

// planText reads a rendered plan file.
func planText(c *qt.C, planPath string) string {
	c.Helper()
	body, err := os.ReadFile(planPath)
	c.Assert(err, qt.IsNil)
	return string(body)
}

// planFieldFrom reads one `name: value` line out of a rendered cutover plan.
func planFieldFrom(c *qt.C, plan, name string) string {
	c.Helper()
	for _, line := range strings.Split(plan, "\n") {
		value, found := strings.CutPrefix(line, name+": ")
		for range onlyWhenFound(found) {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// onlyWhenFound yields once when found, so planFieldFrom returns without an
// `if`. It is a helper rather than a conditional in a test function, which the
// repository's test style forbids.
func onlyWhenFound(found bool) []struct{} {
	return map[bool][]struct{}{true: {{}}, false: nil}[found]
}

// runNative drives a verb outside the inference namespace.
//
// The signing step is `ptah schema approve`, which is the point: the approval
// is the mechanism this repository already has rather than a second one grown
// beside it.
func runNative(c *qt.C, ctx context.Context, args ...string) string {
	c.Helper()
	cmd := root.NewRootCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs(args)
	c.Assert(cmd.ExecuteContext(ctx), qt.IsNil, qt.Commentf("%s", output.String()))
	return output.String()
}

// anAllowedSignersFile makes a keypair and the approver list naming it.
func anAllowedSignersFile(c *qt.C, dir, principal string) string {
	c.Helper()
	keyPath := filepath.Join(dir, "id_ed25519")
	out, err := exec.Command("ssh-keygen", "-t", "ed25519", "-N", "", "-C", principal,
		"-f", keyPath).CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	pub, err := os.ReadFile(keyPath + ".pub")
	c.Assert(err, qt.IsNil)
	allowedSigners := filepath.Join(dir, "allowed_signers")
	// #nosec G703 -- allowedSigners is built from c.TempDir(); no external input reaches the path
	c.Assert(os.WriteFile(allowedSigners, []byte(principal+" "+string(pub)), 0o600), qt.IsNil)
	return allowedSigners
}

// TestInferenceUnapprovedCutoverE2E is the policy every other test here does
// not run.
//
// `require_exact_approval: false` is a real configuration -- a development
// environment, a pipeline whose review happens elsewhere -- and it is the one
// path where a cutover proceeds with no approval object at all. Every
// specification in this suite requires an approval, so nothing exercised it,
// and reading the approver off an approval that was never given panicked the
// process at the moment the pointer moved (stokaro/ptah#2068).
func TestInferenceUnapprovedCutoverE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_unapproved_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()
	specPath := writeUnapprovedCLISpec(c, endpoint.URL)

	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "verify", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)

	// No --approve and no --approver: the policy asks for neither.
	output := runInference(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	c.Assert(output, qt.Contains, "queries now read generation ")

	// And the pointer records that nobody signed for it, rather than a name
	// nobody supplied.
	var approver sql.NullString
	err = db.QueryRowContext(ctx,
		`SELECT cut_over_by FROM ptah_embedding_pointer WHERE target_table = 'articles'`).
		Scan(&approver)
	c.Assert(err, qt.IsNil)
	c.Assert(approver.String, qt.Equals, "")
}

// writeUnapprovedCLISpec writes the shared specification with its approval
// requirement turned off.
//
// A substitution into the one document every other test here uses, so the only
// difference between this run and theirs is the policy line this test is about.
func writeUnapprovedCLISpec(c *qt.C, endpoint string) string {
	c.Helper()
	source, err := os.ReadFile(writeCLISpec(c, endpoint))
	c.Assert(err, qt.IsNil)
	document := strings.Replace(string(source),
		"require_exact_approval: true", "require_exact_approval: false", 1)
	c.Assert(document, qt.Not(qt.Equals), string(source))

	path := filepath.Join(c.TempDir(), "unapproved-spec.yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}
