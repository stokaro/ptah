package migrationspush_test

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
	"ptah.run/internal/pathguard"
)

const repository = "acme/app-migrations"

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

func writeMigrations(c *qt.C, table string) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"0000000001_create_" + table + ".up.sql":   "CREATE TABLE " + table + " (id BIGINT PRIMARY KEY);\n",
		"0000000001_create_" + table + ".down.sql": "DROP TABLE " + table + ";\n",
	}
	for name, contents := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600), qt.IsNil)
	}
	return dir
}

func pushArgs(reference, dir string, extra ...string) []string {
	return append([]string{
		"migrations", "push", reference,
		"--migrations-dir", dir,
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
func TestMigrationsPush_FormatJSONDescribesTheManifestItWrote(t *testing.T) {
	c := qt.New(t)
	registry := startRegistry(c)

	got := runPtah(c, pushArgs(registry.Reference("release"), writeMigrations(c, "widgets"),
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
	c.Assert(manifest.ArtifactType, qt.Equals, ociartifact.MigrationArtifactType)
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
// `REF=$(ptah migrations push ... --format reference)` captures exactly it.
func TestMigrationsPush_FormatReferencePrintsOnlyThePinnedReference(t *testing.T) {
	c := qt.New(t)
	registry := startRegistry(c)

	got := runPtah(c, pushArgs(registry.Reference("release"), writeMigrations(c, "widgets"), "--format", "reference")...)

	c.Assert(got.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.stderr))
	c.Assert(got.stderr, qt.Equals, "")
	c.Assert(got.stdout, qt.Equals, registry.PinnedReference(written(c, registry, "release"))+"\n")
}

// The reference names the manifest this push wrote even when the tag it pushed
// to names another one by the time anything reads it. The registry answers every
// read of the tag with the other writer's manifest, so a command that resolved
// the tag after pushing would print that one instead.
func TestMigrationsPush_PinnedReferenceIsTheManifestThisPushWrote(t *testing.T) {
	c := qt.New(t)
	registry := startRegistry(c)
	first := runPtah(c, pushArgs(registry.Reference("other"), writeMigrations(c, "gadgets"), "--format", "reference")...)
	c.Assert(first.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", first.stderr))
	other := written(c, registry, "other")
	registry.RepointReads("release", other)

	got := runPtah(c, pushArgs(registry.Reference("release"), writeMigrations(c, "widgets"), "--format", "reference")...)

	c.Assert(got.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.stderr))
	ours := written(c, registry, "release")
	c.Assert(ours, qt.Not(qt.Equals), other)
	c.Assert(got.stdout, qt.Equals, registry.PinnedReference(ours)+"\n")
}

// Without --format the receipt a person reads is unchanged.
func TestMigrationsPush_TextReceiptIsTheDefault(t *testing.T) {
	c := qt.New(t)
	registry := startRegistry(c)
	dir := writeMigrations(c, "widgets")
	resolved, err := pathguard.ResolveCLIPath(dir)
	c.Assert(err, qt.IsNil)

	got := runPtah(c, pushArgs(registry.Reference("release"), dir)...)

	c.Assert(got.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.stderr))
	c.Assert(got.stdout, qt.Equals, fmt.Sprintf(
		"Pushed %s as %s\nDigest: %s\nVersion: \nTags: [release]\n",
		resolved, registry.Reference("release"), written(c, registry, "release"),
	))
}

// A format the command cannot print is refused before the registry is reached,
// so nothing is published that the caller will not hear about.
func TestMigrationsPush_RefusesAnUnsupportedFormatBeforePushing(t *testing.T) {
	c := qt.New(t)
	registry := startRegistry(c)

	got := runPtah(c, pushArgs(registry.Reference("release"), writeMigrations(c, "widgets"), "--format", "yaml")...)

	c.Assert(got.code, qt.Equals, 2)
	c.Assert(got.stdout, qt.Equals, "")
	c.Assert(got.stderr, qt.Contains, `unsupported output format "yaml": expected text, json, or reference`)
	c.Assert(registry.Requests(), qt.Equals, int64(0))
}

// A layout has no pinned reference, so a machine format is refused before the
// layout is written: the directory a push would create does not exist after.
func TestMigrationsPush_RefusesAMachineFormatForALayoutBeforePushing(t *testing.T) {
	c := qt.New(t)
	layout := filepath.Join(c.TempDir(), "layout")

	got := runPtah(c, pushArgs("oci-layout://"+layout+":v1", writeMigrations(c, "widgets"), "--format", "json")...)

	c.Assert(got.code, qt.Equals, 2)
	c.Assert(got.stdout, qt.Equals, "")
	c.Assert(got.stderr, qt.Contains, "an oci-layout:// destination has none")
	_, err := os.Stat(layout)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

// A push that failed after the manifest landed still prints no reference: the
// command failed, and a caller must not carry its output forward.
func TestMigrationsPush_PartialPushPrintsNoReference(t *testing.T) {
	c := qt.New(t)
	registry := startRegistry(c)
	registry.RefuseTag("release")

	got := runPtah(c, pushArgs(registry.Reference("release"), writeMigrations(c, "widgets"), "--format", "json")...)

	c.Assert(got.code, qt.Equals, 2)
	c.Assert(got.stdout, qt.Equals, "")
	c.Assert(got.stderr, qt.Contains, `was published and tags [] were applied before tag "release" failed`)
}

// A push to a digest is refused, since a digest names content that already
// exists, and the refusal prints no reference.
func TestMigrationsPush_DigestDestinationPrintsNoReference(t *testing.T) {
	c := qt.New(t)
	registry := startRegistry(c)
	destination := registry.PinnedReference(digest.FromString("unrelated"))

	got := runPtah(c, pushArgs(destination, writeMigrations(c, "widgets"), "--format", "reference")...)

	c.Assert(got.code, qt.Equals, 2)
	c.Assert(got.stdout, qt.Equals, "")
	c.Assert(got.stderr, qt.Contains, "cannot push to a digest reference")
	c.Assert(registry.Requests(), qt.Equals, int64(0))
}
