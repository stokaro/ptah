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

// TestOCIRegistryMigrationsValidateE2E is the live acceptance for
// stokaro/ptah#1499 against a real registry over plain HTTP.
//
// The defect it closes is not a wrong answer but a missing one: `migrations
// validate --dir oci://…` reported `stat oci://…: no such file or directory`,
// so the read-only integrity question had no spelling at all for a registry
// source. Only `up --verify-sum` and `down --verify-sum` could ask it, and both
// answer by writing to a database.
//
// Each row is measured TWICE — once against the local directory the artifact
// was built from, once against the pushed artifact — and the two must agree on
// the exit status and on the sentence. That pairing is what makes the row a
// result rather than a smoke test: an artifact path that resolved the scheme
// but validated something else (an empty snapshot, say) would exit 2 on all
// three rows while the local control kept reporting 0, 1 and 2.
func TestOCIRegistryMigrationsValidateE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c, ctx, repoRoot, binaryPath)
	suffix := time.Now().UnixNano()

	tests := []struct {
		name string
		// prepare mutates the freshly hashed directory before it is pushed.
		// It carries the branch so the test body does not: an untouched
		// directory is the clean row, and the other two are the ways an
		// operator arrives at a directory whose sum no longer describes it.
		prepare func(c *qt.C, dir string)
		// wantExit is the documented contract: 0 matches, 1 drift, 2 the sum
		// is missing or unreadable.
		wantExit   int
		wantOutput string
	}{
		{
			name:       "hashed artifact validates",
			prepare:    func(*qt.C, string) {},
			wantExit:   0,
			wantOutput: "OK: migrations directory matches ptah.sum",
		},
		{
			name: "tampered artifact reports drift",
			prepare: func(c *qt.C, dir string) {
				c.Assert(os.WriteFile(
					filepath.Join(dir, fmt.Sprintf("%010d_create_widgets.up.sql", ociMigrationVersion)),
					[]byte("CREATE TABLE widgets (id TEXT PRIMARY KEY);\n"),
					0o600,
				), qt.IsNil)
			},
			wantExit:   1,
			wantOutput: "changed:",
		},
		{
			name: "unhashed artifact reports no sum at all",
			prepare: func(c *qt.C, dir string) {
				c.Assert(os.Remove(filepath.Join(dir, "ptah.sum")), qt.IsNil)
			},
			wantExit:   2,
			wantOutput: "ptah.sum not found",
		},
	}

	for index, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			dir := filepath.Join(c.TempDir(), "migrations")
			writeOCIMigration(c, dir, ociMigrationVersion, "widgets")
			test.prepare(c, dir)

			localOutput, localErr := runPtahInDir(
				ctx, repoRoot, binaryPath,
				"migrations", "validate",
				"--dir", dir,
			)
			c.Assert(exitStatusOf(c, localErr), qt.Equals, test.wantExit,
				qt.Commentf("local output:\n%s", localOutput))
			c.Assert(localOutput, qt.Contains, test.wantOutput)

			reference := fmt.Sprintf("oci://%s/ptah/oci-validate-%d-%d:latest", registry, suffix, index)
			pushOutput, pushErr := runPtahInDir(
				ctx, repoRoot, binaryPath,
				"migrations", "push", reference,
				"--migrations-dir", dir,
				"--plain-http",
			)
			c.Assert(pushErr, qt.IsNil, qt.Commentf("push output:\n%s", pushOutput))

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
			// separately from the exit status because the two rows that expect
			// a non-zero status would otherwise pass on it: `stat oci://…: no
			// such file or directory` was itself an exit 2.
			c.Assert(artifactOutput, qt.Not(qt.Contains), "no such file or directory")
		})
	}
}

// TestOCIRegistryMigrationsValidateWithoutPlainHTTPE2E pins the other
// direction: the flag is what selects an unencrypted connection, and a run
// without it must refuse to talk to the plain-HTTP registry rather than
// silently succeeding.
//
// Without this, a build whose OCI client had stopped defaulting to TLS would
// pass every row above.
func TestOCIRegistryMigrationsValidateWithoutPlainHTTPE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c, ctx, repoRoot, binaryPath)

	dir := filepath.Join(c.TempDir(), "migrations")
	writeOCIMigration(c, dir, ociMigrationVersion, "widgets")
	reference := fmt.Sprintf("oci://%s/ptah/oci-validate-tls-%d:latest", registry, time.Now().UnixNano())

	pushOutput, pushErr := runPtahInDir(
		ctx, repoRoot, binaryPath,
		"migrations", "push", reference,
		"--migrations-dir", dir,
		"--plain-http",
	)
	c.Assert(pushErr, qt.IsNil, qt.Commentf("push output:\n%s", pushOutput))

	output, err := runPtahInDir(
		ctx, repoRoot, binaryPath,
		"migrations", "validate",
		"--dir", reference,
	)

	c.Assert(exitStatusOf(c, err), qt.Not(qt.Equals), 0, qt.Commentf("output:\n%s", output))
	c.Assert(output, qt.Contains, "https://", qt.Commentf("output:\n%s", output))
}
