//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// checksEntities is the publisher's working copy: one table, so the artifact
// has a schema to carry the checks beside.
const checksEntities = `package entities

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64

	//ptah:schema:field name="tier" type="TEXT"
	Tier string
}
`

// approvedChecks is what a reviewer signed off: the release requires every
// user to have a tier.
const approvedChecks = `-- +ptah check name="every user has a tier" assert="SELECT COUNT(*) = 0 FROM users WHERE tier IS NULL"
`

// replacedChecks is what someone published afterwards under the same tag. It
// asserts nothing about the release and would pass on any database.
const replacedChecks = `-- +ptah check name="anything at all" assert="SELECT 1 = 1"
`

// TestOCISchemaChecksBindTheApprovedAssertionsE2E is the acceptance question
// stokaro/ptah#3458 exists for: an artifact whose checks were replaced after
// approval must not verify clean.
//
// The table is seeded with a row that violates the approved requirement, so a
// run that evaluates those assertions fails and a run that evaluates the
// replacement passes. The digest captured at publish time is the approval, and
// it keeps naming the bytes it named even after the tag moves.
//
// Only a registry and a server can answer it: the binding rests on content
// addressing, which a fixture cannot imitate, and the verdicts come from a
// database reading its own rows.
func TestOCISchemaChecksBindTheApprovedAssertionsE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	dbURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	repoRoot := e2eRepoRoot(t)
	workDir := c.TempDir()
	binaryPath := filepath.Join(workDir, "ptah")
	buildPtah(c, ctx, repoRoot, binaryPath)

	table := fmt.Sprintf("users_%d", time.Now().UnixNano()%100000)
	entitiesDir := filepath.Join(workDir, "entities")
	c.Assert(os.MkdirAll(entitiesDir, 0o750), qt.IsNil)
	writeChecksFixture(c, filepath.Join(entitiesDir, "entities.go"),
		strings.ReplaceAll(checksEntities, "users", table))
	approvedPath := filepath.Join(workDir, "approved.sql")
	writeChecksFixture(c, approvedPath, strings.ReplaceAll(approvedChecks, "users", table))
	replacedPath := filepath.Join(workDir, "replaced.sql")
	writeChecksFixture(c, replacedPath, replacedChecks)

	reference := fmt.Sprintf("oci://%s/ptah-checks-%d:v1", registry, time.Now().UnixNano()%100000)
	approvedDigest := pushChecksArtifact(c, ctx, binaryPath, workDir, reference, approvedPath)

	seedChecksTable(c, ctx, dbURL, table)

	// The approved assertions, addressed by the digest the publish reported.
	pinned := pinnedChecksReference(reference, approvedDigest)
	output, status := runPtahForStatus(ctx, binaryPath,
		"db", "verify", "--db-url", dbURL, "--checks", pinned, "--plain-http")
	c.Assert(status, qt.Equals, 1, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains, "every user has a tier")

	// The tag is moved to an artifact whose checks assert nothing.
	pushChecksArtifact(c, ctx, binaryPath, workDir, reference, replacedPath)
	output, status = runPtahForStatus(ctx, binaryPath,
		"db", "verify", "--db-url", dbURL, "--checks", reference, "--plain-http")
	c.Assert(status, qt.Equals, 0, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains, "anything at all")

	// The pinned reference still evaluates what was approved, which is the
	// whole point: a moved tag cannot turn a violated requirement into a clean
	// release.
	output, status = runPtahForStatus(ctx, binaryPath,
		"db", "verify", "--db-url", dbURL, "--checks", pinned, "--plain-http")
	c.Assert(status, qt.Equals, 1, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains, "every user has a tier")
}

// TestOCISchemaChecksRefuseAnArtifactWithoutThemE2E keeps "this artifact
// publishes no assertions" separate from "the assertions all held". An
// operator who named an artifact expected its checks.
func TestOCISchemaChecksRefuseAnArtifactWithoutThemE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	dbURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	repoRoot := e2eRepoRoot(t)
	workDir := c.TempDir()
	binaryPath := filepath.Join(workDir, "ptah")
	buildPtah(c, ctx, repoRoot, binaryPath)

	entitiesDir := filepath.Join(workDir, "entities")
	c.Assert(os.MkdirAll(entitiesDir, 0o750), qt.IsNil)
	writeChecksFixture(c, filepath.Join(entitiesDir, "entities.go"), checksEntities)
	reference := fmt.Sprintf("oci://%s/ptah-nochecks-%d:v1", registry, time.Now().UnixNano()%100000)

	output, err := runPtahInDir(ctx, workDir, binaryPath,
		"schema", "push", reference, "--root-dir", entitiesDir, "--plain-http")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))

	output, status := runPtahForStatus(ctx, binaryPath,
		"db", "verify", "--db-url", dbURL, "--checks", reference, "--plain-http")

	c.Assert(status, qt.Equals, 2, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains, "publishes no release assertions")
}

// pushChecksArtifact publishes the schema with a checks file and returns the
// digest the publish reported, which is what a reviewer would record.
func pushChecksArtifact(
	c *qt.C,
	ctx context.Context,
	binaryPath, workDir, reference, checksPath string,
) string {
	c.Helper()
	output, err := runPtahInDir(ctx, workDir, binaryPath,
		"schema", "push", reference,
		"--root-dir", filepath.Join(workDir, "entities"),
		"--checks", checksPath,
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	digest := regexp.MustCompile(`sha256:[0-9a-f]{64}`).FindString(output)
	c.Assert(digest, qt.Not(qt.Equals), "", qt.Commentf("%s", output))
	return digest
}

// pinnedChecksReference addresses the artifact by digest, dropping the tag the
// publish used.
func pinnedChecksReference(reference, digest string) string {
	repository, _, _ := strings.Cut(reference, ":v1")
	return repository + "@" + digest
}

// seedChecksTable creates the table the assertions are about and puts one row
// in it that violates the approved requirement, so a run that evaluates those
// assertions has something to fail on.
func seedChecksTable(c *qt.C, ctx context.Context, dbURL, table string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	statements := []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", table),
		fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY, tier TEXT)", table),
		fmt.Sprintf("INSERT INTO %s (id, tier) VALUES (1, NULL)", table),
	}
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("%s", statement))
	}
	c.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	})
}

func writeChecksFixture(c *qt.C, path, contents string) {
	c.Helper()
	c.Assert(os.WriteFile(path, []byte(contents), 0o600), qt.IsNil)
}
