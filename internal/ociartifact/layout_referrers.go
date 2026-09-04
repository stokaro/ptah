package ociartifact

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/content/oci"
)

// An OCI image layout is a directory, so it serves no HTTP referrers API. It
// does index referrers, though: oras-go records every manifest it holds in
// index.json, tagged or not, and a fresh reader of the directory recovers an
// untagged attachment through the store's predecessor graph. Measured on
// oras-go v2.6.2 (stokaro/ptah#2839).
//
// That is what lets an attachment into a layout mean the same thing an
// attachment into a registry means, and it is why a policy demanding the
// referrers index is satisfied by a directory rather than refused by it: the
// guarantee the policy asks for -- that a consumer can discover the attachment
// without guessing a tag -- is one the layout keeps.

// layoutRepository adapts an image layout to the repository interface an
// attachment needs.
//
// *oci.Store already satisfies two thirds of it, [oras.ReadOnlyTarget] and
// [registry.TagLister]. It has no Referrers method, so the third is derived
// here from the predecessor graph it does keep.
type layoutRepository struct {
	*oci.Store
}

// Referrers lists the manifests in the layout that name desc as their subject.
//
// It is not the predecessor list. Predecessors are every node holding a
// reference to desc, which includes an image index that merely lists desc as a
// member -- measured, such an index comes back beside the real attachment. A
// referrer is narrower: its own subject field points at desc. So each candidate
// is read and asked, rather than trusted for being adjacent.
func (r layoutRepository) Referrers(
	ctx context.Context,
	desc ocispec.Descriptor,
	artifactType string,
	fn func(referrers []ocispec.Descriptor) error,
) error {
	predecessors, err := r.Predecessors(ctx, desc)
	if err != nil {
		return fmt.Errorf("list layout predecessors of %s: %w", desc.Digest, err)
	}
	referrers := make([]ocispec.Descriptor, 0, len(predecessors))
	for _, candidate := range predecessors {
		described, err := r.describe(ctx, candidate)
		if err != nil {
			return err
		}
		if described.subject == nil || described.subject.Digest != desc.Digest {
			continue
		}
		if artifactType != "" && described.artifactType != artifactType {
			continue
		}
		// The descriptor a reopened store hands back carries no artifactType,
		// so the one the manifest declares is put back on the copy that
		// leaves here. A caller filtering the result again -- and
		// listReferrersFrom does -- would otherwise drop what this just
		// matched.
		resolved := candidate
		resolved.ArtifactType = described.artifactType
		referrers = append(referrers, resolved)
	}
	if len(referrers) == 0 {
		return nil
	}
	return fn(referrers)
}

// layoutCandidate is what a predecessor's own bytes say about it.
type layoutCandidate struct {
	subject      *ocispec.Descriptor
	artifactType string
}

// describe reads a candidate's subject and artifact type out of the manifest
// rather than off the descriptor.
//
// The descriptor cannot be trusted for either. Measured on oras-go v2.6.2: the
// store that wrote an attachment hands back a predecessor descriptor carrying
// artifactType, and a store freshly opened over the same directory hands back
// the same predecessor with that field empty. Filtering on the descriptor
// therefore matched while the layout was still open and silently matched
// nothing once it had been carried anywhere -- which is the only way a layout
// is ever used.
//
// Content that cannot carry a subject is reported as having none rather than
// as an error, because a blob is a legitimate predecessor and not a referrer.
func (r layoutRepository) describe(
	ctx context.Context,
	candidate ocispec.Descriptor,
) (layoutCandidate, error) {
	switch candidate.MediaType {
	case ocispec.MediaTypeImageManifest, ocispec.MediaTypeImageIndex:
	default:
		return layoutCandidate{}, nil
	}
	body, err := content.FetchAll(ctx, r.Store, candidate)
	if err != nil {
		return layoutCandidate{}, fmt.Errorf("read layout manifest %s: %w", candidate.Digest, err)
	}
	// A manifest and an index carry the subject in the same place, so one
	// shape reads either. The config is manifest-only and absent from an
	// index, which decodes to the zero value and is what the fallback below
	// expects.
	var carrier struct {
		Subject      *ocispec.Descriptor `json:"subject,omitempty"`
		ArtifactType string              `json:"artifactType,omitempty"`
		Config       ocispec.Descriptor  `json:"config"`
	}
	if err := json.Unmarshal(body, &carrier); err != nil {
		return layoutCandidate{}, fmt.Errorf("decode layout manifest %s: %w", candidate.Digest, err)
	}
	described := layoutCandidate{subject: carrier.Subject, artifactType: carrier.ArtifactType}
	// The image-spec says an absent artifactType falls back to the config
	// media type, which is how a manifest written before artifactType existed
	// still names what it is.
	if described.artifactType == "" {
		described.artifactType = carrier.Config.MediaType
	}
	return described, nil
}

// layoutIndexProbe answers the referrers-index question for a directory.
//
// It reports the index as served, and the reason is the measurement above
// rather than a convenience: index.json lists the attachment whether or not it
// was tagged, so a consumer discovers it without the ptah-r- fallback tag that
// an index-requiring policy deliberately does not write.
func layoutIndexProbe() indexProbe {
	return func(context.Context) (bool, string, error) {
		return true, "the OCI image layout records referrers in index.json", nil
	}
}

// attachmentEndpoint is everything an attachment needs about its destination,
// resolved once for a registry or an image layout.
//
// Attach needs four things where Push needed two, and three of them used to be
// registry-shaped: the repository that resolves the subject, the target the
// referrer is written to, and the probe that answers what discovery the policy
// requires. Resolving them together is what lets a layout be the same
// operation rather than a second code path (stokaro/ptah#2839).
type attachmentEndpoint struct {
	target     oras.Target
	repository referrerRepository
	reference  Reference
	selector   string
	display    string
	probe      indexProbe
}

// attachmentEndpoint opens the side an attachment writes to, and the side a
// referrer listing reads from -- they are one target, and resolving them
// apart is what let a layout be written to and not read back
// (stokaro/ptah#2852).
func (c *Client) attachmentEndpoint(raw string) (attachmentEndpoint, error) {
	if IsLayoutRef(raw) {
		target, tag, err := OpenLayout(raw)
		if err != nil {
			return attachmentEndpoint{}, err
		}
		store, ok := target.(*oci.Store)
		if !ok {
			return attachmentEndpoint{}, fmt.Errorf(
				"the OCI image layout %s did not open as a layout store", strings.TrimSpace(raw))
		}
		repository := layoutRepository{Store: store}
		return attachmentEndpoint{
			target:     store,
			repository: repository,
			reference:  layoutReference(raw, tag),
			selector:   tag,
			display:    strings.TrimSpace(raw),
			probe:      layoutIndexProbe(),
		}, nil
	}
	ref, err := ParseRef(raw)
	if err != nil {
		return attachmentEndpoint{}, err
	}
	repository, err := c.repository(ref)
	if err != nil {
		return attachmentEndpoint{}, err
	}
	target, err := c.attachmentTarget(ref)
	if err != nil {
		return attachmentEndpoint{}, err
	}
	return attachmentEndpoint{
		target:     target,
		repository: repository,
		reference:  ref,
		selector:   ref.Selector(),
		display:    ref.String(),
		probe:      c.indexProbe(raw),
	}, nil
}
