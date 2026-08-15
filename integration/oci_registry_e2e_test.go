//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/ocireferrers"
)

const ociMigrationVersion = int64(1775000101)

func TestOCIRegistryConcurrentReferrerPublicationE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	reference := fmt.Sprintf(
		"oci://%s/ptah/oci-referrer-concurrency-%d:latest",
		registry,
		time.Now().UnixNano(),
	)
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	subject, err := client.Push(ctx, reference, fstest.MapFS{
		"migration.sql": {Data: []byte("SELECT 1;\n")},
	}, ociartifact.PushOptions{ArtifactType: ociartifact.MigrationArtifactType})
	c.Assert(err, qt.IsNil)
	subjectReference := digestReference(reference, subject.Descriptor.Digest.String())
	repoRoot := e2eRepoRoot(t)
	helperPath := filepath.Join(t.TempDir(), "oci-attach-helper")
	buildOCIReferrerHelper(c.TB, ctx, repoRoot, helperPath)

	const attachmentCount = 8
	errs := make([]error, attachmentCount)
	outputs := make([][]byte, attachmentCount)
	var wait sync.WaitGroup
	wait.Add(attachmentCount)
	for index := range attachmentCount {
		go func() {
			defer wait.Done()
			command := exec.CommandContext(
				ctx,
				helperPath,
				"--reference", subjectReference,
				"--worker", strconv.Itoa(index),
			)
			outputs[index], errs[index] = command.CombinedOutput()
		}()
	}
	wait.Wait()
	for index, attachErr := range errs {
		c.Assert(
			attachErr,
			qt.IsNil,
			qt.Commentf("worker %d output:\n%s", index, string(outputs[index])),
		)
	}

	referrers, err := client.Referrers(ctx, subjectReference, ociartifact.LintArtifactType)
	c.Assert(err, qt.IsNil)
	c.Assert(referrers, qt.HasLen, attachmentCount)
}

func buildOCIReferrerHelper(
	tb testing.TB,
	ctx context.Context,
	repoRoot string,
	binaryPath string,
) {
	c := qt.New(tb)
	command := exec.CommandContext(
		ctx,
		"go", "build",
		"-o", binaryPath,
		"./integration/testdata/oci-attach-helper",
	)
	command.Dir = repoRoot
	output, err := command.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("helper build output:\n%s", string(output)))
}

func TestOCIRegistryMigrationWorkflowE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	adminURL := requiredPostgresE2EURL(t)
	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c.TB, ctx, repoRoot, binaryPath)

	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	suffix := time.Now().UnixNano()
	databaseName := fmt.Sprintf("ptah_oci_%d", suffix)
	createE2EDatabase(c.TB, ctx, adminDB, databaseName)
	defer dropE2EDatabase(c.TB, context.Background(), adminDB, databaseName)
	databaseURL := replaceDatabaseName(c.TB, adminURL, databaseName)

	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	writeOCIMigration(c.TB, migrationsDir, ociMigrationVersion, "widgets")
	firstSnapshot := snapshotDirectory(c.TB, migrationsDir)
	reference := fmt.Sprintf("oci://%s/ptah/oci-migrations-%d:latest", registry, suffix)

	pushOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "push", reference,
		"--migrations-dir", migrationsDir,
		"--version", "v1775000101",
		"--verify-sum",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("push output:\n%s", pushOutput))
	firstDigest := digestFromPushOutput(c.TB, pushOutput)
	firstDigestReference := digestReference(reference, firstDigest)

	firstPullDir := filepath.Join(t.TempDir(), "first-pull")
	pullOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "pull", reference,
		"--out", firstPullDir,
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("pull output:\n%s", pullOutput))
	c.Assert(snapshotDirectory(c.TB, firstPullDir), qt.DeepEquals, firstSnapshot)

	lintOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "lint",
		"--dir", firstDigestReference,
		"--format", "json",
		"--attach",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("lint output:\n%s", lintOutput))
	c.Assert(readStatusField(c.TB, lintOutput, "failed"), qt.Equals, false)
	standardLintReports := standardOCIReferrers(
		c.TB,
		ctx,
		firstDigestReference,
		ociartifact.LintArtifactType,
	)
	c.Assert(standardLintReports, qt.HasLen, 1)

	upOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "up",
		"--db-url", databaseURL,
		"--migrations-dir", reference,
		"--verify-sum",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("up output:\n%s", upOutput))
	c.Assert(upOutput, qt.Contains, "Database is now at version: 1775000101")
	assertTableExists(c.TB, ctx, databaseURL, "widgets")

	statusOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "status",
		"--db-url", databaseURL,
		"--migrations-dir", firstDigestReference,
		"--json",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("status output:\n%s", statusOutput))
	c.Assert(readStatusField(c.TB, statusOutput, "current_version"), qt.Equals, float64(ociMigrationVersion))

	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	reports, err := client.Referrers(ctx, firstDigestReference, ociartifact.DeploymentArtifactType)
	c.Assert(err, qt.IsNil)
	c.Assert(reports, qt.HasLen, 1)
	noOpOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "up",
		"--db-url", databaseURL,
		"--migrations-dir", firstDigestReference,
		"--verify-sum",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("no-op up output:\n%s", noOpOutput))
	reports, err = client.Referrers(ctx, firstDigestReference, ociartifact.DeploymentArtifactType)
	c.Assert(err, qt.IsNil)
	c.Assert(reports, qt.HasLen, 1)
	lintReports, err := client.Referrers(ctx, firstDigestReference, ociartifact.LintArtifactType)
	c.Assert(err, qt.IsNil)
	c.Assert(lintReports, qt.HasLen, 1)

	referrerOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"oci", "referrers", firstDigestReference,
		"--type", "deployment",
		"--format", "json",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("deployment referrers output:\n%s", referrerOutput))
	referrerRecords := readReferrerRecords(c.TB, referrerOutput)
	c.Assert(referrerRecords, qt.HasLen, 1)
	c.Assert(referrerRecords[0].ArtifactType, qt.Equals, ociartifact.DeploymentArtifactType)

	allReferrersOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"oci", "referrers", firstDigestReference,
		"--format", "json",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("all referrers output:\n%s", allReferrersOutput))
	allReferrers := readReferrerRecords(c.TB, allReferrersOutput)
	c.Assert(allReferrers, qt.HasLen, 2)
	c.Assert(allReferrers[0].ArtifactType, qt.Equals, ociartifact.DeploymentArtifactType)
	c.Assert(allReferrers[1].ArtifactType, qt.Equals, ociartifact.LintArtifactType)

	downOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "down",
		"--db-url", databaseURL,
		"--migrations-dir", firstDigestReference,
		"--target", "0",
		"--confirm",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("down output:\n%s", downOutput))
	assertTableMissing(c.TB, ctx, databaseURL, "widgets")

	dryRunOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "up",
		"--db-url", databaseURL,
		"--migrations-dir", firstDigestReference,
		"--verify-sum",
		"--dry-run",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("dry-run output:\n%s", dryRunOutput))
	assertTableMissing(c.TB, ctx, databaseURL, "widgets")
	reports, err = client.Referrers(ctx, firstDigestReference, ociartifact.DeploymentArtifactType)
	c.Assert(err, qt.IsNil)
	c.Assert(reports, qt.HasLen, 1)

	skipReportOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "up",
		"--db-url", databaseURL,
		"--migrations-dir", firstDigestReference,
		"--verify-sum",
		"--skip-report",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("skip-report output:\n%s", skipReportOutput))
	assertTableExists(c.TB, ctx, databaseURL, "widgets")
	reports, err = client.Referrers(ctx, firstDigestReference, ociartifact.DeploymentArtifactType)
	c.Assert(err, qt.IsNil)
	c.Assert(reports, qt.HasLen, 1)

	secondDownOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "down",
		"--db-url", databaseURL,
		"--migrations-dir", firstDigestReference,
		"--target", "0",
		"--confirm",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("second down output:\n%s", secondDownOutput))
	assertTableMissing(c.TB, ctx, databaseURL, "widgets")

	writeOCIMigration(c.TB, migrationsDir, 1775000202, "gadgets")
	secondSnapshot := snapshotDirectory(c.TB, migrationsDir)
	conflictOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "push", reference,
		"--migrations-dir", migrationsDir,
		"--version", "v1775000101",
		"--verify-sum",
		"--plain-http",
	)
	c.Assert(err, qt.IsNotNil)
	c.Assert(conflictOutput, qt.Contains, "OCI write-once tag already exists")
	conflictPullDir := filepath.Join(t.TempDir(), "conflict-pull")
	conflictPullOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "pull", reference,
		"--out", conflictPullDir,
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("post-conflict pull output:\n%s", conflictPullOutput))
	c.Assert(snapshotDirectory(c.TB, conflictPullDir), qt.DeepEquals, firstSnapshot)

	secondPushOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "push", reference,
		"--migrations-dir", migrationsDir,
		"--version", "v1775000202",
		"--verify-sum",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("second push output:\n%s", secondPushOutput))
	secondDigest := digestFromPushOutput(c.TB, secondPushOutput)
	c.Assert(secondDigest, qt.Not(qt.Equals), firstDigest)

	pinnedPullDir := filepath.Join(t.TempDir(), "pinned-pull")
	pinnedPullOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "pull", firstDigestReference,
		"--out", pinnedPullDir,
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("digest-pinned pull output:\n%s", pinnedPullOutput))
	c.Assert(snapshotDirectory(c.TB, pinnedPullDir), qt.DeepEquals, firstSnapshot)

	latestPullDir := filepath.Join(t.TempDir(), "latest-pull")
	latestPullOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "pull", reference,
		"--out", latestPullDir,
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("latest pull output:\n%s", latestPullOutput))
	c.Assert(snapshotDirectory(c.TB, latestPullDir), qt.DeepEquals, secondSnapshot)
}

func TestOCIRegistrySchemaWorkflowE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	adminURL := requiredPostgresE2EURL(t)
	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c.TB, ctx, repoRoot, binaryPath)

	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	suffix := time.Now().UnixNano()
	databaseName := fmt.Sprintf("ptah_oci_schema_%d", suffix)
	createE2EDatabase(c.TB, ctx, adminDB, databaseName)
	defer dropE2EDatabase(c.TB, context.Background(), adminDB, databaseName)
	databaseURL := replaceDatabaseName(c.TB, adminURL, databaseName)

	schemaSQL := "CREATE TABLE desired_widgets (id BIGINT NOT NULL PRIMARY KEY);\n"
	schemaFile := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schemaFile, []byte(schemaSQL), 0o600), qt.IsNil)
	reference := fmt.Sprintf("oci://%s/ptah/oci-schema-%d:latest", registry, suffix)

	pushOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"schema", "push", reference,
		"--schema-file", schemaFile,
		"--dialect", "postgres",
		"--version", "v20260727000303",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("schema push output:\n%s", pushOutput))
	schemaDigest := digestFromPushOutput(c.TB, pushOutput)
	schemaDigestReference := digestReference(reference, schemaDigest)

	pulledSchema := filepath.Join(t.TempDir(), "schema.hcl")
	pullOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"schema", "pull", reference,
		"--out", pulledSchema,
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("schema pull output:\n%s", pullOutput))
	pulled, err := os.ReadFile(pulledSchema)
	c.Assert(err, qt.IsNil)
	c.Assert(string(pulled), qt.Contains, "desired_widgets")

	schemaDB, err := sql.Open("pgx", databaseURL)
	c.Assert(err, qt.IsNil)
	defer schemaDB.Close()
	_, err = schemaDB.ExecContext(ctx, schemaSQL)
	c.Assert(err, qt.IsNil)

	compareOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"schema", "compare",
		"--schema-file", reference,
		"--db-url", databaseURL,
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("schema compare output:\n%s", compareOutput))
	c.Assert(compareOutput, qt.Not(qt.Contains), "CREATE TABLE")
	c.Assert(compareOutput, qt.Not(qt.Contains), "DROP TABLE")

	driftOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"schema", "drift",
		"--schema-file", reference,
		"--db-url", databaseURL,
		"--format", "json",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("schema drift output:\n%s", driftOutput))
	c.Assert(driftOutput, qt.Contains, `"drift": false`)

	planOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "plan",
		"--schema-file", schemaDigestReference,
		"--db-url", databaseURL,
		"--report", "json",
		"--attach",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("schema plan output:\n%s", planOutput))
	c.Assert(readStatusField(c.TB, planOutput, "destructive"), qt.Equals, false)

	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	plans, err := client.Referrers(ctx, schemaDigestReference, ociartifact.PlanArtifactType)
	c.Assert(err, qt.IsNil)
	c.Assert(plans, qt.HasLen, 1)

	planReferrersOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"oci", "referrers", schemaDigestReference,
		"--type", "plan",
		"--format", "json",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("plan referrers output:\n%s", planReferrersOutput))
	planReferrers := readReferrerRecords(c.TB, planReferrersOutput)
	c.Assert(planReferrers, qt.HasLen, 1)
	c.Assert(planReferrers[0].ArtifactType, qt.Equals, ociartifact.PlanArtifactType)

	destructiveSchemaFile := filepath.Join(t.TempDir(), "destructive.sql")
	c.Assert(os.WriteFile(
		destructiveSchemaFile,
		[]byte("CREATE TABLE replacement_widgets (id BIGINT NOT NULL PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	destructiveReference := fmt.Sprintf("oci://%s/ptah/oci-schema-destructive-%d:latest", registry, suffix)
	destructivePushOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"schema", "push", destructiveReference,
		"--schema-file", destructiveSchemaFile,
		"--dialect", "postgres",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("destructive schema push output:\n%s", destructivePushOutput))
	destructiveDigestReference := digestReference(
		destructiveReference,
		digestFromPushOutput(c.TB, destructivePushOutput),
	)

	destructivePlanOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "plan",
		"--schema-file", destructiveDigestReference,
		"--db-url", databaseURL,
		"--report", "json",
		"--check-destructive",
		"--attach",
		"--plain-http",
	)
	c.Assert(err, qt.IsNotNil)
	c.Assert(destructivePlanOutput, qt.Contains, `"destructive": true`)
	c.Assert(destructivePlanOutput, qt.Contains, "destructive migration statements require --allow-destructive")
	destructivePlans, err := client.Referrers(
		ctx,
		destructiveDigestReference,
		ociartifact.PlanArtifactType,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(destructivePlans, qt.HasLen, 1)
}

func requiredOCIRegistry(t *testing.T) string {
	t.Helper()
	registry := os.Getenv("PTAH_OCI_TEST_REGISTRY")
	if registry == "" {
		t.Skip("PTAH_OCI_TEST_REGISTRY is not set")
	}
	return registry
}

func requiredPostgresE2EURL(t *testing.T) string {
	t.Helper()
	databaseURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	// dbtarget refuses an address carrying another engine's scheme, but it
	// admits a schemeless driver DSN on purpose. These tests rewrite the
	// database name through net/url, so the URL shape is still their own
	// requirement and the check stays.
	if !strings.HasPrefix(databaseURL, "postgres://") &&
		!strings.HasPrefix(databaseURL, "postgresql://") {
		t.Skip("PostgreSQL URL required for OCI registry e2e tests")
	}
	return databaseURL
}

func writeOCIMigration(tb testing.TB, dir string, version int64, table string) {
	c := qt.New(tb)
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	prefix := fmt.Sprintf("%010d_create_%s", version, table)
	up := fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY);\n", table)
	down := fmt.Sprintf("DROP TABLE %s;\n", table)
	c.Assert(os.WriteFile(filepath.Join(dir, prefix+".up.sql"), []byte(up), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, prefix+".down.sql"), []byte(down), 0o600), qt.IsNil)
	_, err := migratesum.Write(dir)
	c.Assert(err, qt.IsNil)
}

func snapshotDirectory(tb testing.TB, dir string) map[string]string {
	c := qt.New(tb)
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	snapshot := make(map[string]string, len(entries))
	for _, entry := range entries {
		c.Assert(entry.IsDir(), qt.IsFalse)
		contents, readErr := os.ReadFile(filepath.Join(dir, entry.Name()))
		c.Assert(readErr, qt.IsNil)
		snapshot[entry.Name()] = string(contents)
	}
	return snapshot
}

func digestFromPushOutput(tb testing.TB, output string) string {
	c := qt.New(tb)
	match := regexp.MustCompile(`(?m)^Digest: (sha256:[a-f0-9]{64})$`).FindStringSubmatch(output)
	c.Assert(match, qt.HasLen, 2, qt.Commentf("push output:\n%s", output))
	return match[1]
}

func digestReference(reference, digest string) string {
	return strings.TrimSuffix(reference, ":latest") + "@" + digest
}

// tamperOCIMigrationAndRehash injects a statement nobody reviewed and rehashes,
// which is exactly what a repository writer can do: the sum file travels inside
// the artifact, so rehashing keeps every self-consistency check green.
func tamperOCIMigrationAndRehash(tb testing.TB, dir string, version int64, table string) {
	c := qt.New(tb)
	path := filepath.Join(dir, fmt.Sprintf("%010d_create_%s.up.sql", version, table))
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	c.Assert(err, qt.IsNil)
	_, err = file.WriteString("CREATE TABLE evil (id BIGINT PRIMARY KEY);\n")
	c.Assert(err, qt.IsNil)
	c.Assert(file.Close(), qt.IsNil)
	_, err = migratesum.Write(dir)
	c.Assert(err, qt.IsNil)
}

func sqliteTableCount(tb testing.TB, ctx context.Context, dbPath, table string) int {
	c := qt.New(tb)
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`,
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

// TestOCIMigrationTagSumProvenanceE2E reproduces stokaro/ptah#944 against a
// live registry. Measured on master: `ptah.sum verified: migrations directory
// is intact` printed byte-identically for the reviewed artifact and for one
// whose tag had been repointed at an injected `CREATE TABLE evil`, and `up`
// printed the resolved digest nowhere, so no operator could learn after the
// fact which bytes ran. The apply keeps exiting 0 — the flag's contract is
// honest about what it checks — but a tag-resolved verification must now name
// the tag, the digest it resolved to, and the pin, while a digest reference
// must stay silent and must still run the reviewed bytes.
func TestOCIMigrationTagSumProvenanceE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c.TB, ctx, repoRoot, binaryPath)

	suffix := time.Now().UnixNano()
	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	writeOCIMigration(c.TB, migrationsDir, ociMigrationVersion, "widgets")
	base := fmt.Sprintf("oci://%s/ptah/oci-provenance-%d", registry, suffix)
	tagReference := base + ":release"

	pushOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "push", tagReference,
		"--migrations-dir", migrationsDir,
		"--version", fmt.Sprintf("v%d", suffix),
		"--verify-sum",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("push output:\n%s", pushOutput))
	firstDigest := digestFromPushOutput(c.TB, pushOutput)
	firstDigestReference := base + "@" + firstDigest

	reviewedDB := filepath.Join(t.TempDir(), "reviewed.db")
	reviewedOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "up",
		"--db-url", "sqlite://"+reviewedDB,
		"--migrations-dir", tagReference,
		"--verify-sum",
		"--skip-report",
		"--plain-http",
		"--verbose",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("reviewed up output:\n%s", reviewedOutput))
	c.Assert(reviewedOutput, qt.Contains, "ptah.sum verified: migrations directory is intact")
	c.Assert(reviewedOutput, qt.Contains, "Warning: "+tagReference+" is a movable tag")
	c.Assert(reviewedOutput, qt.Contains, "This tag resolved to "+firstDigest)
	c.Assert(reviewedOutput, qt.Contains, "pass "+firstDigestReference+" to pin these exact bytes.")
	c.Assert(sqliteTableCount(c.TB, ctx, reviewedDB, "widgets"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c.TB, ctx, reviewedDB, "evil"), qt.Equals, 0)

	tamperOCIMigrationAndRehash(c.TB, migrationsDir, ociMigrationVersion, "widgets")
	repointOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "push", tagReference,
		"--migrations-dir", migrationsDir,
		"--version", fmt.Sprintf("v%d", suffix+1),
		"--verify-sum",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("repoint push output:\n%s", repointOutput))
	secondDigest := digestFromPushOutput(c.TB, repointOutput)
	c.Assert(secondDigest, qt.Not(qt.Equals), firstDigest)

	// Byte-identical command, repointed tag. The sum still verifies, because the
	// rehashed sum travelled with the rewritten files; the provenance line is
	// the only thing that reports which bytes that covered.
	repointedDB := filepath.Join(t.TempDir(), "repointed.db")
	repointedOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "up",
		"--db-url", "sqlite://"+repointedDB,
		"--migrations-dir", tagReference,
		"--verify-sum",
		"--skip-report",
		"--plain-http",
		"--verbose",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("repointed up output:\n%s", repointedOutput))
	c.Assert(repointedOutput, qt.Contains, "ptah.sum verified: migrations directory is intact")
	c.Assert(repointedOutput, qt.Contains, "This tag resolved to "+secondDigest)
	c.Assert(repointedOutput, qt.Not(qt.Contains), firstDigest)
	c.Assert(sqliteTableCount(c.TB, ctx, repointedDB, "evil"), qt.Equals, 1)

	// The digest pin selects the reviewed bytes and says nothing extra: there is
	// no tag left to qualify.
	pinnedDB := filepath.Join(t.TempDir(), "pinned.db")
	pinnedOutput, err := runPtahInDir(
		ctx,
		repoRoot,
		binaryPath,
		"migrations", "up",
		"--db-url", "sqlite://"+pinnedDB,
		"--migrations-dir", firstDigestReference,
		"--verify-sum",
		"--skip-report",
		"--plain-http",
		"--verbose",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("pinned up output:\n%s", pinnedOutput))
	c.Assert(pinnedOutput, qt.Contains, "ptah.sum verified: migrations directory is intact")
	c.Assert(pinnedOutput, qt.Not(qt.Contains), "movable tag")
	c.Assert(pinnedOutput, qt.Not(qt.Contains), "Warning:")
	c.Assert(sqliteTableCount(c.TB, ctx, pinnedDB, "widgets"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c.TB, ctx, pinnedDB, "evil"), qt.Equals, 0)
}

func readReferrerRecords(tb testing.TB, output string) []ocireferrers.Record {
	c := qt.New(tb)
	var records []ocireferrers.Record
	c.Assert(json.Unmarshal([]byte(output), &records), qt.IsNil, qt.Commentf("referrers output:\n%s", output))
	return records
}

func standardOCIReferrers(
	tb testing.TB,
	ctx context.Context,
	reference string,
	artifactType string,
) []ocispec.Descriptor {
	c := qt.New(tb)
	ref, err := ociartifact.ParseRef(reference)
	c.Assert(err, qt.IsNil)
	repository, err := ociartifact.NewRepository(ref, ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	subject, err := repository.Resolve(ctx, ref.Selector())
	c.Assert(err, qt.IsNil)
	var result []ocispec.Descriptor
	err = repository.Referrers(ctx, subject, artifactType, func(page []ocispec.Descriptor) error {
		result = append(result, page...)
		return nil
	})
	c.Assert(err, qt.IsNil)
	return result
}

func assertTableExists(tb testing.TB, ctx context.Context, databaseURL, table string) {
	c := qt.New(tb)
	db, err := sql.Open("pgx", databaseURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	var got string
	err = db.QueryRowContext(
		ctx,
		`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1`,
		table,
	).Scan(&got)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, table)
}

func assertTableMissing(tb testing.TB, ctx context.Context, databaseURL, table string) {
	c := qt.New(tb)
	db, err := sql.Open("pgx", databaseURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	var count int
	err = db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1`,
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}
