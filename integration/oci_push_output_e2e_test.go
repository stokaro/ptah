//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ociartifact"
)

// pushRecord is the part of the `--format json` push document these rows read.
type pushRecord struct {
	Reference       string   `json:"reference"`
	PinnedReference string   `json:"pinned_reference"`
	Digest          string   `json:"digest"`
	ArtifactType    string   `json:"artifact_type"`
	Tags            []string `json:"tags"`
}

// resolvePinned asks the registry what tag names now, through `ptah oci
// resolve`, which prints the pinned reference alone.
func resolvePinned(c *qt.C, ctx context.Context, binaryPath, reference string) string {
	stdout, stderr, err := runLintE2EBinary(ctx, binaryPath, "oci", "resolve", reference, "--plain-http")
	c.Assert(err, qt.IsNil, qt.Commentf("resolve stderr:\n%s", stderr))
	return strings.TrimSuffix(stdout, "\n")
}

// TestOCIRegistryMigrationsPushJSONNamesTheArtifactE2E pushes a migration
// directory to a real registry with --format json and hands the pinned
// reference to the verb that consumes an artifact.
//
// The unit rows run against an in-process registry. This one is what a
// registry:2 answers: the digest the push reports is the digest the registry
// serves under the tag, and the pinned reference is one `migrations validate`
// accepts as its source.
func TestOCIRegistryMigrationsPushJSONNamesTheArtifactE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	binaryPath := ociValidateBinary(c, ctx, e2eRepoRoot(t))
	reference := fmt.Sprintf("oci://%s/ptah/oci-push-json-%d:release", registry, time.Now().UnixNano())

	stdout, stderr, err := runLintE2EBinary(ctx, binaryPath,
		"migrations", "push", reference,
		"--migrations-dir", hashedOCIMigrationsDir(c),
		"--plain-http",
		"--format", "json",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("push stderr:\n%s", stderr))
	c.Assert(stderr, qt.Equals, "")
	var record pushRecord
	c.Assert(json.Unmarshal([]byte(stdout), &record), qt.IsNil, qt.Commentf("push stdout:\n%s", stdout))
	c.Assert(record.Reference, qt.Equals, reference)
	c.Assert(record.ArtifactType, qt.Equals, ociartifact.MigrationArtifactType)
	c.Assert(record.Tags, qt.DeepEquals, []string{"release"})
	c.Assert(record.PinnedReference, qt.Equals, strings.TrimSuffix(reference, ":release")+"@"+record.Digest)
	c.Assert(resolvePinned(c, ctx, binaryPath, reference), qt.Equals, record.PinnedReference)

	output, err := runPtahInDir(ctx, e2eRepoRoot(t), binaryPath,
		"migrations", "validate",
		"--dir", record.PinnedReference,
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("validate output:\n%s", output))
	c.Assert(output, qt.Contains, "OK: migrations directory matches ptah.sum")
}

// TestOCIRegistrySchemaPushReferenceNamesTheArtifactE2E pushes a desired
// schema to a real registry with --format reference, so standard output is the
// pinned reference and nothing else, and reads the artifact back through it.
func TestOCIRegistrySchemaPushReferenceNamesTheArtifactE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	binaryPath := ociValidateBinary(c, ctx, e2eRepoRoot(t))
	reference := fmt.Sprintf("oci://%s/ptah/oci-push-reference-%d:release", registry, time.Now().UnixNano())
	schemaFile := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schemaFile, []byte("CREATE TABLE desired_widgets (id BIGINT NOT NULL PRIMARY KEY);\n"), 0o600), qt.IsNil)

	stdout, stderr, err := runLintE2EBinary(ctx, binaryPath,
		"schema", "push", reference,
		"--schema-file", schemaFile,
		"--dialect", "postgres",
		"--plain-http",
		"--format", "reference",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("push stderr:\n%s", stderr))
	c.Assert(stderr, qt.Equals, "")
	pinned := strings.TrimSuffix(stdout, "\n")
	c.Assert(stdout, qt.Equals, pinned+"\n")
	c.Assert(resolvePinned(c, ctx, binaryPath, reference), qt.Equals, pinned)

	inspected, stderr, err := runLintE2EBinary(ctx, binaryPath,
		"oci", "inspect", pinned,
		"--no-referrers",
		"--format", "json",
		"--plain-http",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("inspect stderr:\n%s", stderr))
	var manifest struct {
		PinnedReference string `json:"pinned_reference"`
		ArtifactType    string `json:"artifact_type"`
	}
	c.Assert(json.Unmarshal([]byte(inspected), &manifest), qt.IsNil, qt.Commentf("inspect stdout:\n%s", inspected))
	c.Assert(manifest.PinnedReference, qt.Equals, pinned)
	c.Assert(manifest.ArtifactType, qt.Equals, ociartifact.SchemaArtifactType)
}
