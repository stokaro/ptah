package ociartifact_test

import (
	"context"
	"io/fs"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/ociartifact"
)

// TestClientRoundTripsAnImageLayout is stokaro/ptah#2623.
//
// `Push` and `Pull` addressed a registry and nothing else, so an environment
// with no registry could consume a promotion -- `ptah oci copy` reaches a
// layout through `copySource`/`copyDestination` -- and could not produce one.
// Both answered `invalid OCI reference: expected oci:// prefix`.
//
// The round trip is one test rather than two because either half alone proves
// less than it appears to: a push whose bytes nothing reads back has not been
// shown to have written an artifact, and a pull needs something to read.
func TestClientRoundTripsAnImageLayout(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	reference := ociartifact.LayoutScheme + c.TempDir() + "/store:v1"
	archive := fstest.MapFS{"release.json": &fstest.MapFile{Data: []byte(`{"generation":"g1"}`)}}

	pushed, err := ociartifact.Push(ctx, reference, archive,
		ociartifact.PushOptions{ArtifactType: releaseTestArtifactType})
	c.Assert(err, qt.IsNil)

	pulled, err := ociartifact.Pull(ctx, reference, ociartifact.PullOptions{})
	c.Assert(err, qt.IsNil)

	c.Assert(pulled.Descriptor.Digest, qt.Equals, pushed.Descriptor.Digest)
	body, err := fs.ReadFile(pulled.FileSystem, "release.json")
	c.Assert(err, qt.IsNil)
	c.Assert(string(body), qt.Equals, `{"generation":"g1"}`)
}

// TestPushReportsTheLayoutItWroteTo pins what a layout push answers for
// [PushResult.Reference].
//
// A [Reference] holds a registry and a repository, and a layout has neither.
// Leaving the field zero would have been the quiet option and renders as
// `oci:///:` -- a registry reference naming nothing, which a caller logs or
// prints as though it were an address. The reference reports the directory it
// actually wrote to.
func TestPushReportsTheLayoutItWroteTo(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	reference := ociartifact.LayoutScheme + c.TempDir() + "/store:v7"

	pushed, err := ociartifact.Push(ctx, reference,
		fstest.MapFS{"a.json": &fstest.MapFile{Data: []byte("{}")}},
		ociartifact.PushOptions{ArtifactType: releaseTestArtifactType})

	c.Assert(err, qt.IsNil)
	c.Assert(pushed.Reference.String(), qt.Equals, reference)
	c.Assert(pushed.Reference.IsLayout(), qt.IsTrue)
	c.Assert(pushed.Tags, qt.DeepEquals, []string{"v7"})
}

// TestPushStillRefusesADigestForARegistry is the control on the resolution
// swap.
//
// `Push` used to refuse a digest reference itself; it now delegates to
// `copyDestination`, which makes the same refusal. A refusal that moved and
// quietly stopped happening would let a publish address content that already
// exists, so the check is asserted where the caller meets it rather than only
// where it is implemented.
func TestPushStillRefusesADigestForARegistry(t *testing.T) {
	c := qt.New(t)

	_, err := ociartifact.Push(context.Background(),
		"oci://registry.example/repo@sha256:"+strings64,
		fstest.MapFS{"a.json": &fstest.MapFile{Data: []byte("{}")}},
		ociartifact.PushOptions{ArtifactType: releaseTestArtifactType})

	c.Assert(err, qt.ErrorIs, ociartifact.ErrDigestPush)
}

const (
	releaseTestArtifactType = "application/vnd.ptah.test.layout"
	strings64               = "0000000000000000000000000000000000000000000000000000000000000000"
)
