package embedrelease_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedrelease"
	"go.5x5.cz/ptah/internal/ociartifact"
)

// specification is a stand-in for the document a release carries.
//
// Not a valid specification, deliberately. This package publishes and reads
// bytes and never parses them, and a fixture that happened to parse would let a
// reader believe otherwise.
const specification = "version: 1\nname: articles v2\n"

// aRelease is what a generation change proposes.
func aRelease() embedrelease.Release {
	return embedrelease.Release{
		Generation: "gen-2", Replaces: "gen-1",
		SpecDigest: "0f0e0d0c", Target: "public.articles.embedding",
		Reproducibility: "exact", CreatedAt: at,
	}
}

// TestFetch_ReadsBackWhatWasPublished is the promotion, end to end, without a
// registry: an environment addresses a release and gets the document it was
// built from.
func TestFetch_ReadsBackWhatWasPublished(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	reference := layoutIn(c) + ":v2"
	record, err := embedrelease.NewReleaseRecord(aRelease(), []byte(specification))
	c.Assert(err, qt.IsNil)
	published := publishToLayout(c, ctx, reference, record)

	fetched, err := embedrelease.Fetch(ctx, reference, embedrelease.FetchOptions{})

	c.Assert(err, qt.IsNil)
	c.Assert(fetched.Release.Generation, qt.Equals, "gen-2")
	c.Assert(fetched.Release.SpecDigest, qt.Equals, "0f0e0d0c")
	// The document itself, which is the half that makes a release runnable
	// somewhere it was not written.
	c.Assert(string(fetched.Specification), qt.Equals, specification)
	// And what the mutable name turned out to address. A promotion that kept
	// only the tag says two environments agreed without establishing that they
	// did, because a tag moves.
	c.Assert(fetched.Reference, qt.Equals, reference)
	c.Assert(fetched.Digest, qt.Equals, published)
}

// TestFetch_RefusesAReleaseWithNoSpecificationInIt names what to do about it.
//
// The reader who meets this is promoting a release published by a build that
// carried none, and "no such file" would send them looking at their registry.
func TestFetch_RefusesAReleaseWithNoSpecificationInIt(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	reference := layoutIn(c) + ":v2"
	release := aRelease()
	release.Version = embedrelease.RecordVersion
	body, err := embedrelease.Encode(release)
	c.Assert(err, qt.IsNil)
	publishFilesToLayout(c, ctx, reference, embedrelease.ReleaseArtifactType, fstest.MapFS{
		embedrelease.ReleaseFileName: &fstest.MapFile{Data: body},
	})

	_, err = embedrelease.Fetch(ctx, reference, embedrelease.FetchOptions{})

	c.Assert(err, qt.ErrorIs, embedrelease.ErrIncompleteRelease)
	c.Assert(err.Error(), qt.Contains, "publish the release again from the specification")
}

// TestFetch_RefusesAnArtifactThatIsNotARelease is what stops a verification
// report, or anything else in the same repository, from being run as one.
func TestFetch_RefusesAnArtifactThatIsNotARelease(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	reference := layoutIn(c) + ":v2"
	record, err := embedrelease.NewVerificationRecord(aVerification())
	c.Assert(err, qt.IsNil)
	publishToLayout(c, ctx, reference, record)

	_, err = embedrelease.Fetch(ctx, reference, embedrelease.FetchOptions{})

	c.Assert(err, qt.ErrorIs, ociartifact.ErrUnexpectedArtifactType)
}

// TestNewReleaseRecord_RefusesOneWithNothingToRun is the publishing half of the
// same contract.
//
// A parameter rather than a field somebody may leave empty, because a release
// that cannot be promoted fails on arrival -- in another environment, a long way
// from the run that published it.
func TestNewReleaseRecord_RefusesOneWithNothingToRun(t *testing.T) {
	c := qt.New(t)

	_, err := embedrelease.NewReleaseRecord(aRelease(), nil)

	c.Assert(err, qt.ErrorMatches,
		`a release carries the specification it was built from, and generation gen-2 was given none`)
}

// TestNewReleaseRecord_ListsTheSpecificationItCarries is what a reader compares
// two releases with before pulling either layer.
func TestNewReleaseRecord_ListsTheSpecificationItCarries(t *testing.T) {
	c := qt.New(t)

	record, err := embedrelease.NewReleaseRecord(aRelease(), []byte(specification))

	c.Assert(err, qt.IsNil)
	c.Assert(record.Annotations["cz.5x5.ptah.inference.specification"], qt.Equals, "0f0e0d0c")
	c.Assert(record.Files[embedrelease.SpecificationFileName], qt.DeepEquals, []byte(specification))
}

// layoutIn is an empty OCI image layout directory.
//
// A directory rather than a registry because this is exactly the air-gapped
// promotion path -- export on one side of the gap, carry, import on the other --
// and a test that needed a server would leave that path measured by nothing.
func layoutIn(c *qt.C) string {
	c.Helper()
	return ociartifact.LayoutScheme + c.TempDir() + "/release"
}

// publishToLayout writes a record into a layout and answers its digest.
func publishToLayout(
	c *qt.C, ctx context.Context, reference string, record embedrelease.Record,
) string {
	c.Helper()
	archive := fstest.MapFS{record.FileName: &fstest.MapFile{Data: record.Body}}
	for name, body := range record.Files {
		archive[name] = &fstest.MapFile{Data: body}
	}
	return publishFilesToLayout(c, ctx, reference, record.ArtifactType, archive)
}

// publishFilesToLayout writes an arbitrary archive, which is how a release with
// a part missing is built.
func publishFilesToLayout(
	c *qt.C, ctx context.Context, reference, artifactType string, archive fstest.MapFS,
) string {
	c.Helper()
	target, tag, err := ociartifact.OpenLayout(reference)
	c.Assert(err, qt.IsNil)
	result, err := ociartifact.PushTo(ctx, target, archive, ociartifact.PushOptions{
		ArtifactType: artifactType, Tags: []string{tag},
	})
	c.Assert(err, qt.IsNil)
	return result.Descriptor.Digest.String()
}
