package ociartifact

// The merge below is white-box on purpose. Reaching it from outside the package
// means driving DiscoverReferrersFrom with a repository that answers BOTH
// discovery mechanisms, and a durable referrer is only accepted when its tag
// name, its subject and its manifest all agree — so a black-box fixture would
// have to reconstruct durableReferrerTag by hand and would then be asserting
// against its own copy of the rule rather than against the rule. The provenance
// decision is what these tests are about, and it is pure.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func descriptorFor(letter, artifactType string) ocispec.Descriptor {
	return ocispec.Descriptor{
		MediaType:    ocispec.MediaTypeImageManifest,
		ArtifactType: artifactType,
		Digest:       digestOf(letter),
		Size:         2,
	}
}

func digestOf(letter string) digest.Digest {
	return digest.Digest("sha256:" + strings.Repeat(letter, 64))
}

func TestMergeDiscovered_RecordsWhichMechanismFoundEach(t *testing.T) {
	t.Run("only the referrers index", func(t *testing.T) {
		c := qt.New(t)
		standard := []ocispec.Descriptor{descriptorFor("a", LintArtifactType)}

		got, err := mergeDiscovered(standard, nil, 10)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.HasLen, 1)
		c.Assert(got[0].Source, qt.Equals, ReferrerSourceAPI)
	})

	t.Run("only the durable tag", func(t *testing.T) {
		c := qt.New(t)
		durable := []ocispec.Descriptor{descriptorFor("b", LintArtifactType)}

		got, err := mergeDiscovered(nil, durable, 10)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.HasLen, 1)
		c.Assert(got[0].Source, qt.Equals, ReferrerSourceDurableTag)
	})

	t.Run("both mechanisms", func(t *testing.T) {
		c := qt.New(t)
		shared := descriptorFor("c", LintArtifactType)

		got, err := mergeDiscovered([]ocispec.Descriptor{shared}, []ocispec.Descriptor{shared}, 10)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.HasLen, 1, qt.Commentf("one descriptor found twice is still one referrer"))
		c.Assert(got[0].Source, qt.Equals, ReferrerSourceBoth)
	})

	t.Run("a mixed subject keeps each answer separate", func(t *testing.T) {
		c := qt.New(t)
		shared := descriptorFor("d", LintArtifactType)
		indexOnly := descriptorFor("e", PlanArtifactType)
		tagOnly := descriptorFor("f", DeploymentArtifactType)

		got, err := mergeDiscovered(
			[]ocispec.Descriptor{shared, indexOnly},
			[]ocispec.Descriptor{shared, tagOnly},
			10,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.HasLen, 3)
		sources := map[string]ReferrerSource{}
		for _, item := range got {
			sources[item.Descriptor.Digest.String()] = item.Source
		}
		c.Assert(sources[shared.Digest.String()], qt.Equals, ReferrerSourceBoth)
		c.Assert(sources[indexOnly.Digest.String()], qt.Equals, ReferrerSourceAPI)
		c.Assert(sources[tagOnly.Digest.String()], qt.Equals, ReferrerSourceDurableTag)
	})
}

// TestMergeDiscovered_UnionMatchesMergeReferrers is the invariant that keeps
// the two listings interchangeable. A referrer an operator sees through inspect
// and cannot fetch through the merged path would mean the two functions
// disagree about what exists in the registry.
func TestMergeDiscovered_UnionMatchesMergeReferrers(t *testing.T) {
	c := qt.New(t)
	shared := descriptorFor("a", LintArtifactType)
	indexOnly := descriptorFor("b", PlanArtifactType)
	tagOnly := descriptorFor("c", DeploymentArtifactType)
	standard := []ocispec.Descriptor{shared, indexOnly}
	durable := []ocispec.Descriptor{shared, tagOnly}

	discovered, err := mergeDiscovered(standard, durable, 10)
	c.Assert(err, qt.IsNil)
	merged, err := mergeReferrers(standard, durable, 10)
	c.Assert(err, qt.IsNil)

	c.Assert(discovered, qt.HasLen, len(merged))
	seen := make(map[string]struct{}, len(discovered))
	for _, item := range discovered {
		seen[item.Descriptor.Digest.String()] = struct{}{}
	}
	for _, descriptor := range merged {
		_, ok := seen[descriptor.Digest.String()]
		c.Assert(ok, qt.IsTrue, qt.Commentf("merged referrer %s is missing from discovery", descriptor.Digest))
	}
}

func TestMergeDiscovered_RefusesPastTheLimit(t *testing.T) {
	c := qt.New(t)
	standard := []ocispec.Descriptor{
		descriptorFor("a", LintArtifactType),
		descriptorFor("b", LintArtifactType),
	}

	_, err := mergeDiscovered(standard, nil, 1)

	c.Assert(err, qt.ErrorIs, ErrArtifactLimit)
}

func TestMergeDiscovered_SortsForStableOutput(t *testing.T) {
	c := qt.New(t)
	standard := []ocispec.Descriptor{
		descriptorFor("c", PlanArtifactType),
		descriptorFor("a", LintArtifactType),
		descriptorFor("b", LintArtifactType),
	}

	got, err := mergeDiscovered(standard, nil, 10)

	c.Assert(err, qt.IsNil)
	c.Assert(got[0].Descriptor.ArtifactType, qt.Equals, LintArtifactType)
	c.Assert(got[1].Descriptor.ArtifactType, qt.Equals, LintArtifactType)
	c.Assert(got[0].Descriptor.Digest, qt.Not(qt.Equals), got[1].Descriptor.Digest)
	c.Assert(got[2].Descriptor.ArtifactType, qt.Equals, PlanArtifactType)
}
