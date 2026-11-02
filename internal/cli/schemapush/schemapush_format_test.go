package schemapush_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/opencontainers/go-digest"

	"ptah.run/internal/cli/root"
	"ptah.run/internal/ociartifact"
	"ptah.run/internal/ociregistrytest"
)

const repository = "acme/app-schema"

// ptahRun is what one invocation of the binary's command tree left behind.
type ptahRun struct {
	stdout string
	stderr string
	code   int
}

// runPtah runs the command tree the binary ships, so the exit code and the
// streams are the ones a script sees.
func runPtah(c *qt.C, args ...string) ptahRun {
	c.Helper()
	cmd := root.NewRootCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	code := root.RunContext(c.Context(), cmd, args...)
	return ptahRun{stdout: stdout.String(), stderr: stderr.String(), code: code}
}

// startRegistry serves an empty repository and keeps the client away from the
// developer's credential stores. The registry never challenges, so no
// credential is read; the client still opens the stores when it is built.
func startRegistry(c *qt.C) *ociregistrytest.Registry {
	c.Helper()
	c.Setenv("DOCKER_CONFIG", c.TempDir())
	c.Setenv("PTAH_OCI_CONFIG", filepath.Join(c.TempDir(), "registries.json"))
	c.Setenv("PTAH_OCI_CREDENTIAL_STORE", "file")
	return ociregistrytest.Start(c, repository)
}

func writeSchema(c *qt.C, table string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	contents := fmt.Sprintf("CREATE TABLE %s (id BIGINT NOT NULL PRIMARY KEY);\n", table)
	c.Assert(os.WriteFile(path, []byte(contents), 0o600), qt.IsNil)
	return path
}

func pushArgs(reference, schemaFile string, extra ...string) []string {
	return append([]string{
		"schema", "push", reference,
		"--schema-file", schemaFile,
		"--dialect", "postgres",
		"--plain-http",
	}, extra...)
}

// written is the digest of the manifest the registry stored under tag.
func written(c *qt.C, registry *ociregistrytest.Registry, tag string) digest.Digest {
	c.Helper()
	dgst, ok := registry.Written(tag)
	c.Assert(ok, qt.IsTrue, qt.Commentf("the registry stored nothing under %q", tag))
	return dgst
}

// The JSON document describes the manifest the registry holds, read back from
// the registry rather than from the command: digest, size and artifact type are
// the stored manifest's own.
func TestSchemaPush_FormatJSONDescribesTheManifestItWrote(t *testing.T) {
	c := qt.New(t)
	registry := startRegistry(c)

	got := runPtah(c, pushArgs(registry.Reference("release"), writeSchema(c, "widgets"),
		"--version", "v1.0.0", "--tag", "stable", "--format", "json")...)

	c.Assert(got.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.stderr))
	c.Assert(got.stderr, qt.Equals, "")
	dgst := written(c, registry, "release")
	body, _, ok := registry.Manifest(dgst)
	c.Assert(ok, qt.IsTrue)
	var manifest struct {
		ArtifactType string `json:"artifactType"`
	}
	c.Assert(json.Unmarshal(body, &manifest), qt.IsNil)
	c.Assert(manifest.ArtifactType, qt.Equals, ociartifact.SchemaArtifactType)
	c.Assert(got.stdout, qt.Equals, fmt.Sprintf(`{
  "reference": "%s",
  "pinned_reference": "%s",
  "digest": "%s",
  "media_type": "application/vnd.oci.image.manifest.v1+json",
  "size": %d,
  "artifact_type": "%s",
  "tags": [
    "v1.0.0",
    "stable",
    "release"
  ],
  "version": "v1.0.0"
}
`, registry.Reference("release"), registry.PinnedReference(dgst), dgst, len(body), manifest.ArtifactType))
}

// The reference format prints the pinned reference and nothing else, so
// `REF=$(ptah schema push ... --format reference)` captures exactly it.
func TestSchemaPush_FormatReferencePrintsOnlyThePinnedReference(t *testing.T) {
	c := qt.New(t)
	registry := startRegistry(c)

	got := runPtah(c, pushArgs(registry.Reference("release"), writeSchema(c, "widgets"), "--format", "reference")...)

	c.Assert(got.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.stderr))
	c.Assert(got.stderr, qt.Equals, "")
	c.Assert(got.stdout, qt.Equals, registry.PinnedReference(written(c, registry, "release"))+"\n")
}

// The reference names the manifest this push wrote even when the tag it pushed
// to names another one by the time anything reads it. The registry answers every
// read of the tag with the other writer's manifest, so a command that resolved
// the tag after pushing would print that one instead.
func TestSchemaPush_PinnedReferenceIsTheManifestThisPushWrote(t *testing.T) {
	c := qt.New(t)
	registry := startRegistry(c)
	first := runPtah(c, pushArgs(registry.Reference("other"), writeSchema(c, "gadgets"), "--format", "reference")...)
	c.Assert(first.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", first.stderr))
	other := written(c, registry, "other")
	registry.RepointReads("release", other)

	got := runPtah(c, pushArgs(registry.Reference("release"), writeSchema(c, "widgets"), "--format", "reference")...)

	c.Assert(got.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.stderr))
	ours := written(c, registry, "release")
	c.Assert(ours, qt.Not(qt.Equals), other)
	c.Assert(got.stdout, qt.Equals, registry.PinnedReference(ours)+"\n")
}

// Without --format the receipt a person reads is unchanged.
func TestSchemaPush_TextReceiptIsTheDefault(t *testing.T) {
	c := qt.New(t)
	registry := startRegistry(c)

	got := runPtah(c, pushArgs(registry.Reference("release"), writeSchema(c, "widgets"))...)

	c.Assert(got.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.stderr))
	c.Assert(got.stdout, qt.Equals, fmt.Sprintf(
		"Pushed schema as %s\nDigest: %s\nVersion: \nTags: [release]\n",
		registry.Reference("release"), written(c, registry, "release"),
	))
}

// A format the command cannot print is refused before the registry is reached,
// so nothing is published that the caller will not hear about.
func TestSchemaPush_RefusesAnUnsupportedFormatBeforePushing(t *testing.T) {
	c := qt.New(t)
	registry := startRegistry(c)

	got := runPtah(c, pushArgs(registry.Reference("release"), writeSchema(c, "widgets"), "--format", "yaml")...)

	c.Assert(got.code, qt.Equals, 2)
	c.Assert(got.stdout, qt.Equals, "")
	c.Assert(got.stderr, qt.Contains, `unsupported output format "yaml": expected text, json, or reference`)
	c.Assert(registry.Requests(), qt.Equals, int64(0))
}

// A layout has no pinned reference, so a machine format is refused before the
// layout is written: the directory a push would create does not exist after.
func TestSchemaPush_RefusesAMachineFormatForALayoutBeforePushing(t *testing.T) {
	c := qt.New(t)
	layout := filepath.Join(c.TempDir(), "layout")

	got := runPtah(c, pushArgs("oci-layout://"+layout+":v1", writeSchema(c, "widgets"), "--format", "reference")...)

	c.Assert(got.code, qt.Equals, 2)
	c.Assert(got.stdout, qt.Equals, "")
	c.Assert(got.stderr, qt.Contains, "an oci-layout:// destination has none")
	_, err := os.Stat(layout)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

// A push that failed after the manifest landed still prints no reference: the
// command failed, and a caller must not carry its output forward.
func TestSchemaPush_PartialPushPrintsNoReference(t *testing.T) {
	c := qt.New(t)
	registry := startRegistry(c)
	registry.RefuseTag("release")

	got := runPtah(c, pushArgs(registry.Reference("release"), writeSchema(c, "widgets"), "--format", "reference")...)

	c.Assert(got.code, qt.Equals, 2)
	c.Assert(got.stdout, qt.Equals, "")
	c.Assert(got.stderr, qt.Contains, `was published and tags [] were applied before tag "release" failed`)
}

// A write-once tag that already names other content fails the push, and the
// failure prints no reference in either machine format.
func TestSchemaPush_WriteOnceConflictPrintsNoReference(t *testing.T) {
	tests := []struct {
		name   string
		format string
	}{
		{name: "reference", format: "reference"},
		{name: "json", format: "json"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			registry := startRegistry(c)
			first := runPtah(c, pushArgs(registry.Reference("release"), writeSchema(c, "gadgets"),
				"--version", "v1.0.0", "--format", tc.format)...)
			c.Assert(first.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", first.stderr))

			got := runPtah(c, pushArgs(registry.Reference("release"), writeSchema(c, "widgets"),
				"--version", "v1.0.0", "--format", tc.format)...)

			c.Assert(got.code, qt.Equals, 2)
			c.Assert(got.stdout, qt.Equals, "")
			c.Assert(got.stderr, qt.Contains, "OCI write-once tag already exists")
		})
	}
}
