package ociartifact

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/errdef"
)

// ReindexResult reports what a repair pass did.
type ReindexResult struct {
	Reference Reference
	// Subject is the artifact whose attachments were repaired.
	Subject ocispec.Descriptor
	// Indexed were already in the registry's referrers index and were left
	// alone.
	Indexed []ocispec.Descriptor
	// Repaired were reachable only through Ptah's durable tag and have been
	// republished so the index picks them up.
	Repaired []ocispec.Descriptor
	// Unrepaired were reachable only through the durable tag and are still
	// missing from the index after the attempt.
	Unrepaired []ocispec.Descriptor
}

// ReindexReferrers republishes attachments the registry's index does not list.
//
// A registry that gained the referrers index after Ptah published through the
// durable tag has attachments no other OCI client can find, and nothing about
// the artifact says so. Republishing the manifest is what repairs it: the
// content is byte-identical, so the digest does not move, and a registry that
// serves the index builds the entry when it receives a manifest carrying a
// subject.
//
// It is a separate verb rather than something a publish does, because the
// repair is a decision about someone else's registry state. A publish that
// silently rewrote history would be doing work the operator did not ask for on
// artifacts they may not own.
func (c *Client) ReindexReferrers(ctx context.Context, subjectRef string) (ReindexResult, error) {
	subject, discovered, err := c.DiscoverReferrers(ctx, subjectRef, "")
	if err != nil {
		return ReindexResult{}, err
	}

	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	ref, err := ParseRef(subjectRef)
	if err != nil {
		return ReindexResult{}, err
	}
	repository, err := c.repository(ref)
	if err != nil {
		return ReindexResult{}, err
	}
	result := ReindexResult{Reference: ref, Subject: subject}
	limits := c.options.Limits.normalized()

	for _, referrer := range discovered {
		if referrer.Source != ReferrerSourceDurableTag {
			result.Indexed = append(result.Indexed, referrer.Descriptor)
			continue
		}
		manifest, err := content.FetchAll(ctx, repository, referrer.Descriptor)
		if err != nil {
			return result, fmt.Errorf("fetch referrer %s of %s: %w", referrer.Descriptor.Digest, ref, err)
		}
		if int64(len(manifest)) > limits.ManifestBytes {
			return result, fmt.Errorf("%w: referrer %s is %d bytes",
				ErrArtifactLimit, referrer.Descriptor.Digest, len(manifest))
		}
		// Pushing content the registry already has is not a no-op here: the
		// index entry is built on receipt of a manifest naming a subject, and
		// a registry that has the manifest but no entry is exactly the state
		// being repaired. An already-exists answer means the registry declined
		// to reconsider, which the verification below reports rather than
		// treating as success.
		if err := repository.Manifests().Push(ctx, referrer.Descriptor, bytes.NewReader(manifest)); err != nil &&
			!isAlreadyExists(err) {
			return result, fmt.Errorf("republish referrer %s of %s: %w",
				referrer.Descriptor.Digest, ref, err)
		}
		result.Repaired = append(result.Repaired, referrer.Descriptor)
	}

	if len(result.Repaired) == 0 {
		return result, nil
	}
	return c.confirmReindexed(ctx, subjectRef, result)
}

// confirmReindexed re-asks the registry which attachments its index now lists.
//
// The repair is only worth reporting if it worked, and a registry that accepted
// the manifest and built no entry looks identical to one that did until
// somebody asks. So the pass ends by asking.
func (c *Client) confirmReindexed(
	ctx context.Context,
	subjectRef string,
	result ReindexResult,
) (ReindexResult, error) {
	_, after, err := c.DiscoverReferrers(ctx, subjectRef, "")
	if err != nil {
		return result, fmt.Errorf("verify the repaired referrers of %s: %w", result.Reference, err)
	}
	indexed := make(map[string]struct{}, len(after))
	for _, referrer := range after {
		if referrer.Source != ReferrerSourceDurableTag {
			indexed[referrer.Descriptor.Digest.String()] = struct{}{}
		}
	}
	repaired := result.Repaired
	result.Repaired = nil
	for _, descriptor := range repaired {
		if _, ok := indexed[descriptor.Digest.String()]; ok {
			result.Repaired = append(result.Repaired, descriptor)
			continue
		}
		result.Unrepaired = append(result.Unrepaired, descriptor)
	}
	return result, nil
}

func isAlreadyExists(err error) bool {
	return errors.Is(err, errdef.ErrAlreadyExists)
}
