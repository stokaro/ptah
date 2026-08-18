package ociartifact_test

import (
	"context"
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/ociartifact"
)

func TestLayoutPath(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		path string
		tag  string
	}{
		{name: "no tag takes the default", raw: "oci-layout:///tmp/bundle", path: "/tmp/bundle", tag: "latest"},
		{name: "an explicit tag", raw: "oci-layout:///tmp/bundle:v1", path: "/tmp/bundle", tag: "v1"},
		{name: "a relative directory", raw: "oci-layout://bundle:v1", path: "bundle", tag: "v1"},
		{name: "padding is trimmed", raw: "  oci-layout:///tmp/bundle  ", path: "/tmp/bundle", tag: "latest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			path, tag, err := ociartifact.LayoutPath(tc.raw)

			c.Assert(err, qt.IsNil)
			c.Assert(path, qt.Equals, tc.path)
			c.Assert(tag, qt.Equals, tc.tag)
		})
	}
}

func TestLayoutPath_FailurePath(t *testing.T) {
	t.Run("a registry reference is not a layout", func(t *testing.T) {
		c := qt.New(t)

		_, _, err := ociartifact.LayoutPath("oci://registry.invalid/acme/db:latest")

		c.Assert(err, qt.ErrorMatches, `.*is not an oci-layout:// reference`)
	})

	t.Run("a scheme naming no directory", func(t *testing.T) {
		c := qt.New(t)

		_, _, err := ociartifact.LayoutPath("oci-layout://")

		c.Assert(err, qt.ErrorMatches, `oci-layout:// reference names no directory`)
	})
}

// TestCopyArtifact_AcrossImageLayouts is the air-gap workflow end to end, and
// it needs no registry: export into a directory, carry it, import from it. The
// assertion that matters is the digest, because the whole point of carrying an
// artifact rather than rebuilding it is that what arrives is the same bytes.
func TestCopyArtifact_AcrossImageLayouts(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	source := filepath.Join(c.TB.TempDir(), "source")
	destination := filepath.Join(c.TB.TempDir(), "destination")

	store, tag, err := ociartifact.OpenLayout("oci-layout://" + source + ":v1")
	c.Assert(err, qt.IsNil)
	pushed, err := ociartifact.PushTo(ctx, store, fstest.MapFS{
		"migrations/0001.sql": {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
	}, ociartifact.PushOptions{
		ArtifactType: ociartifact.MigrationArtifactType,
		Tags:         []string{tag},
	})
	c.Assert(err, qt.IsNil)

	client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
	c.Assert(err, qt.IsNil)
	copied, err := client.CopyArtifact(ctx,
		"oci-layout://"+source+":v1",
		"oci-layout://"+destination+":v1",
		ociartifact.ArtifactCopyOptions{})

	c.Assert(err, qt.IsNil)
	c.Assert(copied.Digest, qt.Equals, pushed.Descriptor.Digest,
		qt.Commentf("an artifact carried across the gap must be the same artifact, not an equal one"))

	arrived, arrivedTag, err := ociartifact.OpenLayout("oci-layout://" + destination + ":v1")
	c.Assert(err, qt.IsNil)
	artifact, err := ociartifact.PullFrom(ctx, arrived, arrivedTag, ociartifact.PullOptions{
		ExpectedArtifactTypes: []string{ociartifact.MigrationArtifactType},
	})
	c.Assert(err, qt.IsNil)
	contents, err := fs.ReadFile(artifact.FileSystem, "migrations/0001.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "CREATE TABLE users (id INTEGER);\n")
}

// TestCopyArtifact_LayoutDestinationIsCreated keeps the export half from
// demanding by hand what the command is about to do anyway.
func TestCopyArtifact_LayoutDestinationIsCreated(t *testing.T) {
	c := qt.New(t)
	absent := filepath.Join(c.TB.TempDir(), "not-yet")

	target, tag, err := ociartifact.OpenLayout("oci-layout://" + absent)

	c.Assert(err, qt.IsNil)
	c.Assert(target, qt.IsNotNil)
	c.Assert(tag, qt.Equals, "latest")
}
