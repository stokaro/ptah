//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestOCIRegistryMigrationsValidateHashedArtifactE2E is the live acceptance for
// stokaro/ptah#1499 against a real registry over plain HTTP.
//
// The defect it closes is not a wrong answer but a missing one: `migrations
// validate --dir oci://…` reported `stat oci://…: no such file or directory`,
// so the read-only integrity question had no spelling at all for a registry
// source. Only `up --verify-sum` and `down --verify-sum` could ask it, and both
// answer by writing to a database.
//
// The directory is measured TWICE — once against the local directory the
// artifact was built from, once against the pushed artifact — and the two must
// agree on the exit status and on the sentence. That pairing is what makes this
// a result rather than a smoke test, and its discrimination is completed by
// TestOCIRegistryMigrationsValidateFailurePathE2E: an artifact path that
// resolved the scheme but validated something else — an empty snapshot, say —
// would exit 2 whatever it was handed, which the 0 demanded here and the 1
// demanded there both refuse while their local controls keep reporting 0, 1
// and 2.
func TestOCIRegistryMigrationsValidateHashedArtifactE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	repoRoot := e2eRepoRoot(t)
	binaryPath := ociValidateBinary(c, ctx, repoRoot)
	dir := hashedOCIMigrationsDir(c)

	localOutput, localErr := runPtahInDir(
		ctx, repoRoot, binaryPath,
		"migrations", "validate",
		"--dir", dir,
	)
	c.Assert(exitStatusOf(c, localErr), qt.Equals, 0,
		qt.Commentf("local output:\n%s", localOutput))
	c.Assert(localOutput, qt.Contains, "OK: migrations directory matches ptah.sum")
	// The local control carries no reference at all, so it can never be
	// qualified — which is also what makes the artifact assertions below a
	// statement about the tag rather than about validate in general.
	c.Assert(localOutput, qt.Not(qt.Contains), "is a movable tag")

	reference := fmt.Sprintf("oci://%s/ptah/oci-validate-hashed-%d:latest", registry, time.Now().UnixNano())
	pushOCIMigrations(c, ctx, repoRoot, binaryPath, reference, dir)

	artifactOutput, artifactErr := runPtahInDir(
		ctx, repoRoot, binaryPath,
		"migrations", "validate",
		"--dir", reference,
		"--plain-http",
	)
	c.Assert(exitStatusOf(c, artifactErr), qt.Equals, 0,
		qt.Commentf("artifact output:\n%s", artifactOutput))
	c.Assert(artifactOutput, qt.Contains, "OK: migrations directory matches ptah.sum")
	// The path-shaped failure the issue reported. It is asserted separately
	// from the exit status because the failure-path rows next door would
	// otherwise pass on it: `stat oci://…: no such file or directory` was
	// itself an exit 2.
	c.Assert(artifactOutput, qt.Not(qt.Contains), "no such file or directory")
	// A sum that travels inside the artifact proves the pulled files are
	// internally consistent, not that they are the reviewed ones. `up`, `down`
	// and `status` already say so; a `validate` that printed a bare OK would be
	// the one verb over-claiming.
	c.Assert(artifactOutput, qt.Contains, "is a movable tag")
	c.Assert(artifactOutput, qt.Contains, "@sha256:")
}

// TestOCIRegistryMigrationsValidateFailurePathE2E covers the two ways an
// operator arrives at a directory whose sum no longer describes it: files that
// changed under a sum still listing them, and no sum at all. Each is measured
// twice, once against the local directory and once against the pushed
// artifact, and the two must agree on the exit status and on the sentence.
//
// Neither run verifies anything, so neither has a claim to qualify. The tag
// warning belongs to TestOCIRegistryMigrationsValidateHashedArtifactE2E, and
// printing it here would be `validate` narrating a provenance it never
// established.
func TestOCIRegistryMigrationsValidateFailurePathE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	repoRoot := e2eRepoRoot(t)
	binaryPath := ociValidateBinary(c, ctx, repoRoot)
	suffix := time.Now().UnixNano()

	tests := []struct {
		name string
		// corrupt is this row's route to a directory whose sum no longer
		// describes it. It reports rather than asserts, so the row stays data
		// and the one assertion about the arrangement stays in the body.
		corrupt func(dir string) error
		// wantExit is the documented contract: 1 drift, 2 the sum is missing or
		// unreadable.
		wantExit   int
		wantOutput string
	}{
		{
			name: "tampered artifact reports drift",
			corrupt: func(dir string) error {
				return os.WriteFile(
					filepath.Join(dir, fmt.Sprintf("%010d_create_widgets.up.sql", ociMigrationVersion)),
					[]byte("CREATE TABLE widgets (id TEXT PRIMARY KEY);\n"),
					0o600,
				)
			},
			wantExit:   1,
			wantOutput: "changed:",
		},
		{
			name: "unhashed artifact reports no sum at all",
			corrupt: func(dir string) error {
				return os.Remove(filepath.Join(dir, "ptah.sum"))
			},
			wantExit:   2,
			wantOutput: "ptah.sum not found",
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := hashedOCIMigrationsDir(c)
			c.Assert(test.corrupt(dir), qt.IsNil)

			localOutput, localErr := runPtahInDir(
				ctx, repoRoot, binaryPath,
				"migrations", "validate",
				"--dir", dir,
			)
			c.Assert(exitStatusOf(c, localErr), qt.Equals, test.wantExit,
				qt.Commentf("local output:\n%s", localOutput))
			c.Assert(localOutput, qt.Contains, test.wantOutput)

			reference := fmt.Sprintf("oci://%s/ptah/oci-validate-%d-%d:latest", registry, suffix, index)
			pushOCIMigrations(c, ctx, repoRoot, binaryPath, reference, dir)

			artifactOutput, artifactErr := runPtahInDir(
				ctx, repoRoot, binaryPath,
				"migrations", "validate",
				"--dir", reference,
				"--plain-http",
			)
			c.Assert(exitStatusOf(c, artifactErr), qt.Equals, test.wantExit,
				qt.Commentf("artifact output:\n%s", artifactOutput))
			c.Assert(artifactOutput, qt.Contains, test.wantOutput)
			// The path-shaped failure the issue reported. It is asserted
			// separately from the exit status because both rows expect a
			// non-zero status and would otherwise pass on it: `stat oci://…: no
			// such file or directory` was itself an exit 2.
			c.Assert(artifactOutput, qt.Not(qt.Contains), "no such file or directory")
			c.Assert(artifactOutput, qt.Not(qt.Contains), "is a movable tag")
			// The local control carries no reference at all, so it can never be
			// qualified.
			c.Assert(localOutput, qt.Not(qt.Contains), "is a movable tag")
		})
	}
}

// TestOCIRegistryMigrationsValidateDigestSourceIsNotQualifiedE2E is the
// inverse of the hashed-artifact acceptance above, and it is what stops the
// qualifier from being a sentence this verb prints for every artifact.
//
// A digest names exact bytes, so there is no movable pointer left to warn
// about. A build that printed the warning here would be telling an operator
// who did the right thing that they had not.
func TestOCIRegistryMigrationsValidateDigestSourceIsNotQualifiedE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	repoRoot := e2eRepoRoot(t)
	binaryPath := ociValidateBinary(c, ctx, repoRoot)

	dir := hashedOCIMigrationsDir(c)
	reference := fmt.Sprintf("oci://%s/ptah/oci-validate-digest-%d:latest", registry, time.Now().UnixNano())

	pushOutput := pushOCIMigrations(c, ctx, repoRoot, binaryPath, reference, dir)
	pinned := digestReference(reference, digestFromPushOutput(c, pushOutput))

	output, err := runPtahInDir(
		ctx, repoRoot, binaryPath,
		"migrations", "validate",
		"--dir", pinned,
		"--plain-http",
	)

	c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("output:\n%s", output))
	c.Assert(output, qt.Contains, "OK: migrations directory matches ptah.sum")
	c.Assert(output, qt.Not(qt.Contains), "is a movable tag")
}

// TestOCIRegistryMigrationsValidateWithoutPlainHTTPE2E pins the other
// direction: the flag is what selects an unencrypted connection, and a run
// without it must refuse to talk to the plain-HTTP registry rather than
// silently succeeding.
//
// Without this, a build whose OCI client had stopped defaulting to TLS would
// pass every acceptance above.
func TestOCIRegistryMigrationsValidateWithoutPlainHTTPE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	repoRoot := e2eRepoRoot(t)
	binaryPath := ociValidateBinary(c, ctx, repoRoot)

	dir := hashedOCIMigrationsDir(c)
	reference := fmt.Sprintf("oci://%s/ptah/oci-validate-tls-%d:latest", registry, time.Now().UnixNano())

	pushOCIMigrations(c, ctx, repoRoot, binaryPath, reference, dir)

	output, err := runPtahInDir(
		ctx, repoRoot, binaryPath,
		"migrations", "validate",
		"--dir", reference,
	)

	c.Assert(exitStatusOf(c, err), qt.Not(qt.Equals), 0, qt.Commentf("output:\n%s", output))
	c.Assert(output, qt.Contains, "https://", qt.Commentf("output:\n%s", output))
}

// ociValidateBinary builds ptah into a directory of its own and returns the
// path, asserting only that the build succeeded.
func ociValidateBinary(c *qt.C, ctx context.Context, repoRoot string) string {
	binaryPath := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, repoRoot, binaryPath)
	return binaryPath
}

// hashedOCIMigrationsDir provisions a one-migration directory with a matching
// ptah.sum and returns it, asserting only that the files were written.
func hashedOCIMigrationsDir(c *qt.C) string {
	dir := filepath.Join(c.TempDir(), "migrations")
	writeOCIMigration(c, dir, ociMigrationVersion, "widgets")
	return dir
}

// pushOCIMigrations publishes dir at reference over plain HTTP and returns the
// push output. It asserts only that its own push succeeded; what the artifact
// then reports about itself is the caller's question.
func pushOCIMigrations(c *qt.C, ctx context.Context, repoRoot, binaryPath, reference, dir string) string {
	output, err := runPtahInDir(
		ctx, repoRoot, binaryPath,
		"migrations", "push", reference,
		"--migrations-dir", dir,
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("push output:\n%s", output))
	return output
}
