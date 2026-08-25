//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/internal/atlasregistry"
	"go.5x5.cz/ptah/internal/migrationartifact"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/migration/migrationfile"
)

// TestOCIRegistryAtlasReferenceE2E is the proof stokaro/ptah#1210's Definition
// of done asks for: one artifact in a local registry, reached by both
// spellings, applied through the compatibility surface, and pinned across a tag
// move.
//
// Every claim here is about BYTES rather than about exit codes. `atlas://` is a
// compatibility spelling over the same OCI storage, so the two references have
// to resolve to the same digest -- a run that merely succeeded through both
// would pass even if they addressed different artifacts.
func TestOCIRegistryAtlasReferenceE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := requiredOCIRegistry(t)
	repository := fmt.Sprintf("ptah-atlas-reference-%d", time.Now().UnixNano())
	ociReference := fmt.Sprintf("oci://%s/%s:prod", registry, repository)
	c.Setenv(atlasregistry.NamespaceEnvVar, registry)
	c.Setenv(atlasregistry.PlainHTTP.Name(), "1")

	first := writeMigrationDirectory(c, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	pushed, err := migrationartifact.PushDirectory(ctx, migrationartifact.DirectoryPushOptions{
		Reference: ociReference,
		Directory: first,
		Tags:      []string{"prod"},
		Version:   "20260101000000",
		DirFormat: migrationfile.DirFormatPtah,
		PlainHTTP: true,
	})
	c.Assert(err, qt.IsNil)

	// 1. The vendor spelling resolves to the reference that was pushed.
	resolved, err := atlasregistry.Resolve(fmt.Sprintf("atlas://%s?tag=prod", repository))
	c.Assert(err, qt.IsNil)
	c.Assert(resolved.OCI, qt.Equals, ociReference)

	// 2. Both spellings pull the same BYTES, which is what makes one a
	// compatibility spelling of the other rather than a second route.
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	viaOCI, err := migrationartifact.Pull(ctx, client, ociReference)
	c.Assert(err, qt.IsNil)
	viaAtlas, err := migrationartifact.Pull(ctx, client, resolved.OCI)
	c.Assert(err, qt.IsNil)
	c.Assert(viaAtlas.Descriptor.Digest.String(), qt.Equals, viaOCI.Descriptor.Digest.String())

	// 3. The immutable version addresses the same artifact as the movable tag,
	// before anything moves.
	version, err := atlasregistry.Resolve(fmt.Sprintf("atlas://%s?version=20260101000000", repository))
	c.Assert(err, qt.IsNil)
	viaVersion, err := migrationartifact.Pull(ctx, client, version.OCI)
	c.Assert(err, qt.IsNil)
	c.Assert(viaVersion.Descriptor.Digest.String(), qt.Equals, pushed.Descriptor.Digest.String())

	// 4. A migration applies from the vendor spelling, through the
	// compatibility surface, with no local directory anywhere.
	databasePath := filepath.Join(c.TB.TempDir(), "app.db")
	applyOut := runCompatMigrateStatus(c, []string{
		"migrate", "apply",
		"--dir", fmt.Sprintf("atlas://%s?tag=prod", repository),
		"--url", "sqlite://" + filepath.ToSlash(databasePath),
	})
	c.Assert(applyOut, qt.Contains, "Migration complete")

	// 5. A tag MOVES and a version does not. Pushing a second directory to the
	// same tag changes what the tag resolves to, and leaves the version
	// pointing at the first bytes.
	second := writeMigrationDirectory(c,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY);",
	)
	moved, err := migrationartifact.PushDirectory(ctx, migrationartifact.DirectoryPushOptions{
		Reference: ociReference,
		Directory: second,
		Tags:      []string{"prod"},
		Version:   "20260202000000",
		DirFormat: migrationfile.DirFormatPtah,
		PlainHTTP: true,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(moved.Descriptor.Digest.String(), qt.Not(qt.Equals), pushed.Descriptor.Digest.String())

	afterMove, err := migrationartifact.Pull(ctx, client, resolved.OCI)
	c.Assert(err, qt.IsNil)
	c.Assert(afterMove.Descriptor.Digest.String(), qt.Equals, moved.Descriptor.Digest.String())

	stillFirst, err := migrationartifact.Pull(ctx, client, version.OCI)
	c.Assert(err, qt.IsNil)
	c.Assert(stillFirst.Descriptor.Digest.String(), qt.Equals, pushed.Descriptor.Digest.String())
}

// writeMigrationDirectory writes one up/down pair per statement and returns the
// directory, hashed so a push accepts it.
func writeMigrationDirectory(c *qt.C, statements ...string) string {
	c.Helper()
	dir := c.TB.TempDir()
	for i, statement := range statements {
		version := 1775000000 + i
		up := filepath.Join(dir, fmt.Sprintf("%d_step.up.sql", version))
		down := filepath.Join(dir, fmt.Sprintf("%d_step.down.sql", version))
		c.Assert(os.WriteFile(up, []byte(statement+"\n"), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(down, []byte("SELECT 1;\n"), 0o600), qt.IsNil)
	}
	out := runCompatMigrateStatus(c, []string{"migrate", "hash", "--dir", "file://" + filepath.ToSlash(dir)})
	c.Assert(out, qt.Not(qt.Contains), "Error")
	return dir
}

// runCompatMigrateStatus runs one ptah-compat invocation in process and returns
// its combined output, failing the test when the command does.
func runCompatMigrateStatus(c *qt.C, args []string) string {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
	return out.String()
}
