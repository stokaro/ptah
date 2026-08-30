//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/exeext"
)

// TestInferenceRolloutGateE2E is the interface a Kubernetes rollout waits on.
//
// A deployment of a new model must not start until the persistent state it
// reads has been built and measured, and what holds it back is an init
// container that keeps failing. So the contract under test is a process exit
// status, and it can only be measured from a process: a command run in this
// test's own process returns an error and never a status, and every mapping
// between the two lives in the root command this does not call.
//
// It builds the binary for that reason alone. The rest of the lifecycle is
// covered in-process by TestInferenceCLIE2E; what is here is the pair -- the
// gate closed, then the same gate open -- because a --require-ready that always
// succeeded would pass the second assertion and a gate that always failed would
// pass the first (stokaro/ptah#2068).
func TestInferenceRolloutGateE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_gate_%d", time.Now().UnixNano())
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

	repoRoot := e2eRepoRoot(t)
	binary := filepath.Join(c.TempDir(), "ptah"+exeext.Suffix)
	buildPtah(c, ctx, repoRoot, binary)

	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")

	assertTheGateHoldsBeforeCatchUp(c, ctx, binary, specPath, dbName)

	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "index", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)

	assertTheGateOpensWhenTheStateIsThere(c, ctx, binary, specPath, dbName)
	assertTheSecretIsNotInTheProcessArguments(c, ctx, binary, specPath, dbName)
}

// assertTheGateHoldsBeforeCatchUp is the closed half.
//
// Exit 1 rather than 2: the documented contract separates "the command found
// the condition you asked about" from "the command did not complete", and a
// gate that could not tell them apart would treat a typo in a database URL as a
// corpus that is not ready yet.
func assertTheGateHoldsBeforeCatchUp(
	c *qt.C, ctx context.Context, binary, specPath, dbURL string,
) {
	c.Helper()
	output, code := runPtahForStatus(ctx, binary,
		"inference", "status", "--spec", specPath, "--db-url", dbURL,
		"--run-id", cliRunID, "--require-ready")

	c.Assert(code, qt.Equals, 1, qt.Commentf("%s", output))
	// And it says what is missing. A gate that failed silently leaves whoever
	// reads the pod's logs with a number and nothing else.
	c.Assert(output, qt.Contains, "cutover ready: false")
}

// assertTheGateOpensWhenTheStateIsThere is the open half, and the control for
// the one above.
func assertTheGateOpensWhenTheStateIsThere(
	c *qt.C, ctx context.Context, binary, specPath, dbURL string,
) {
	c.Helper()
	output, code := runPtahForStatus(ctx, binary,
		"inference", "status", "--spec", specPath, "--db-url", dbURL,
		"--run-id", cliRunID, "--require-ready")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains, "verified: true, cutover ready: true")
	// The approval is still owed and the gate is still open, which is the whole
	// distinction: under the policy production environments run, folding the
	// signature into readiness would hold every deployment forever.
	c.Assert(output, qt.Contains, "an approval is required")
}

// assertTheSecretIsNotInTheProcessArguments is what a Kubernetes manifest
// depends on.
//
// A pod's command line is readable by anything that can read the pod
// specification, so the database URL and the provider token reach Ptah as
// environment variables. This runs the gate with an empty argv for both and the
// values in the environment, and requires the same answer.
func assertTheSecretIsNotInTheProcessArguments(
	c *qt.C, ctx context.Context, binary, specPath, dbURL string,
) {
	c.Helper()
	output, code := runPtahForStatusWithEnv(ctx, binary,
		[]string{"PTAH_DB_URL=" + dbURL, "PTAH_SPEC=" + specPath, "PTAH_RUN_ID=" + cliRunID},
		"inference", "status", "--require-ready")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains, "verified: true, cutover ready: true")
}

// runPtahForStatus runs the binary and answers with its output and exit status.
func runPtahForStatus(ctx context.Context, binary string, args ...string) (string, int) {
	return runPtahForStatusWithEnv(ctx, binary, nil, args...)
}

// runPtahForStatusWithEnv is the same with extra environment.
//
// The process starts from an EMPTY environment plus what is named, so a
// variable the test did not set cannot be inherited from whoever ran it -- which
// is how an argv assertion comes to pass because the value was in the ambient
// environment all along.
func runPtahForStatusWithEnv(
	ctx context.Context, binary string, environment []string, args ...string,
) (string, int) {
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = environment
	output, err := command.CombinedOutput()
	if err == nil {
		return string(output), 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return string(output), exitErr.ExitCode()
	}
	// Something other than the process refusing: report it as the failure it
	// is rather than as a status the gate could act on.
	return string(output) + "\n" + err.Error(), -1
}
