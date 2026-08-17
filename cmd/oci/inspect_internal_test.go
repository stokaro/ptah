package oci

// White-box testing required: summarizeDiscovery is the sentence an operator
// reads to decide whether their referrers are discoverable by anything other
// than Ptah, and newInspectRecord is the mapping behind it. Both are
// unexported, and reaching them from outside would need a registry rigged to
// answer one discovery mechanism and not the other.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"go.5x5.cz/ptah/internal/ociartifact"
)

func discovered(source ociartifact.ReferrerSource) ociartifact.DiscoveredReferrer {
	return ociartifact.DiscoveredReferrer{
		Descriptor: ocispec.Descriptor{ArtifactType: ociartifact.LintArtifactType},
		Source:     source,
	}
}

func TestSummarizeDiscovery(t *testing.T) {
	t.Run("nothing attached", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(summarizeDiscovery(nil), qt.Equals, "none")
	})

	t.Run("every referrer is in the standard index", func(t *testing.T) {
		c := qt.New(t)
		got := summarizeDiscovery([]ociartifact.DiscoveredReferrer{
			discovered(ociartifact.ReferrerSourceBoth),
			discovered(ociartifact.ReferrerSourceBoth),
		})
		c.Assert(got, qt.Equals, string(ociartifact.ReferrerSourceBoth))
	})

	// The summary reports the WEAKEST guarantee present, not the most common
	// one. One referrer reachable only through Ptah's durable tag is exactly
	// the referrer another OCI client will miss, and a summary that reported
	// the majority would hide it behind its better-behaved neighbors.
	t.Run("one durable-tag referrer decides the summary", func(t *testing.T) {
		c := qt.New(t)
		got := summarizeDiscovery([]ociartifact.DiscoveredReferrer{
			discovered(ociartifact.ReferrerSourceBoth),
			discovered(ociartifact.ReferrerSourceBoth),
			discovered(ociartifact.ReferrerSourceDurableTag),
		})
		c.Assert(got, qt.Equals, string(ociartifact.ReferrerSourceDurableTag))
	})

	t.Run("index-only outranks both but loses to durable-tag", func(t *testing.T) {
		c := qt.New(t)
		got := summarizeDiscovery([]ociartifact.DiscoveredReferrer{
			discovered(ociartifact.ReferrerSourceBoth),
			discovered(ociartifact.ReferrerSourceAPI),
		})
		c.Assert(got, qt.Equals, string(ociartifact.ReferrerSourceAPI))
	})
}

func TestNewInspectRecord_CarriesSubjectAndLayerNames(t *testing.T) {
	c := qt.New(t)
	ref, err := ociartifact.ParseRef("oci://registry.invalid/acme/db:latest")
	c.Assert(err, qt.IsNil)
	subject := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.Digest("sha256:" + strings.Repeat("a", 64)),
		Size:      7,
	}
	info := ociartifact.ManifestInfo{
		Reference:    ref,
		Descriptor:   ocispec.Descriptor{Digest: digest.Digest("sha256:" + strings.Repeat("b", 64)), Size: 3},
		ArtifactType: ociartifact.DeploymentArtifactType,
		Subject:      &subject,
		Layers: []ocispec.Descriptor{{
			MediaType:   ociartifact.FileMediaType,
			Digest:      digest.Digest("sha256:" + strings.Repeat("c", 64)),
			Size:        11,
			Annotations: map[string]string{ocispec.AnnotationTitle: "deployment.json"},
		}},
	}

	record := newInspectRecord(info)

	c.Assert(record.ArtifactType, qt.Equals, ociartifact.DeploymentArtifactType)
	c.Assert(record.Subject, qt.IsNotNil)
	c.Assert(record.Subject.Digest, qt.Equals, subject.Digest.String())
	c.Assert(record.Layers, qt.HasLen, 1)
	c.Assert(record.Layers[0].Name, qt.Equals, "deployment.json")
	c.Assert(record.PinnedReference, qt.Contains, "@sha256:")
}
