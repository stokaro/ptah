package ociartifact

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/registry"
)

// ReferrerSource names the discovery mechanism a referrer was found through.
//
// Ptah writes the standard referrers index AND its own content-derived durable
// tag, then merges both when listing. Which of the two answered is not an
// implementation detail: a referrer that only the durable tag returns is
// invisible to every other OCI client, so an operator auditing cross-client
// discoverability has to be able to read the difference rather than infer it.
// That is the observation stokaro/ptah#1143 item 4 needs before a publish
// policy can be chosen, and item 2 asks the inspect surface to report it.
type ReferrerSource string

const (
	// ReferrerSourceAPI means the registry's referrers index returned it, so
	// any conformant OCI client discovers it too.
	ReferrerSourceAPI ReferrerSource = "api"
	// ReferrerSourceDurableTag means only Ptah's content-derived tag returned
	// it. Ptah-to-Ptah discovery works; another client may not see it.
	ReferrerSourceDurableTag ReferrerSource = "durable-tag"
	// ReferrerSourceBoth means both mechanisms returned it, which is the state
	// a publish aims for.
	ReferrerSourceBoth ReferrerSource = "both"
)

// DiscoveredReferrer is a referrer descriptor with the mechanism that found it.
type DiscoveredReferrer struct {
	Descriptor ocispec.Descriptor
	Source     ReferrerSource
}

// ManifestInfo describes one artifact manifest without materializing layers.
//
// Pulling to read a descriptor downloads every file the artifact holds. The
// lifecycle verbs need the manifest alone — what it is, what it points at, what
// it carries — so this reads the manifest and stops there.
type ManifestInfo struct {
	Reference    Reference
	Descriptor   ocispec.Descriptor
	ArtifactType string
	Subject      *ocispec.Descriptor
	Annotations  map[string]string
	Layers       []ocispec.Descriptor
}

// LayerName returns the file name a layer carries, or "" when it has none.
func LayerName(layer ocispec.Descriptor) string {
	return layer.Annotations[ocispec.AnnotationTitle]
}

// Resolve turns a reference's selector into the immutable descriptor it names.
//
// This is the first half of the pinning operation a CI pipeline needs: resolve
// a mutable tag, record the digest, then plan and apply that digest rather than
// the tag, so that a tag moved between the two steps cannot change what runs.
func (c *Client) Resolve(ctx context.Context, rawRef string) (Reference, ocispec.Descriptor, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	ref, err := ParseRef(rawRef)
	if err != nil {
		return Reference{}, ocispec.Descriptor{}, err
	}
	repository, err := c.repository(ref)
	if err != nil {
		return Reference{}, ocispec.Descriptor{}, err
	}
	descriptor, err := repository.Resolve(ctx, ref.Selector())
	if err != nil {
		return Reference{}, ocispec.Descriptor{}, fmt.Errorf("resolve %s: %w", ref, err)
	}
	return ref, descriptor, nil
}

// Inspect reads one manifest and reports what it declares.
func (c *Client) Inspect(ctx context.Context, rawRef string) (ManifestInfo, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	ref, err := ParseRef(rawRef)
	if err != nil {
		return ManifestInfo{}, err
	}
	repository, err := c.repository(ref)
	if err != nil {
		return ManifestInfo{}, err
	}
	limits := c.options.Limits.normalized()
	descriptor, manifestBytes, err := oras.FetchBytes(ctx, repository, ref.Selector(), oras.FetchBytesOptions{
		MaxBytes: limits.ManifestBytes,
	})
	if err != nil {
		return ManifestInfo{}, fmt.Errorf("fetch OCI manifest for %s: %w", ref, err)
	}
	if descriptor.MediaType != ocispec.MediaTypeImageManifest {
		return ManifestInfo{}, fmt.Errorf("%w: descriptor media type %q", ErrUnexpectedArtifactType, descriptor.MediaType)
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return ManifestInfo{}, fmt.Errorf("decode OCI manifest for %s: %w", ref, err)
	}
	if manifest.SchemaVersion != 2 || manifest.MediaType != ocispec.MediaTypeImageManifest {
		return ManifestInfo{}, fmt.Errorf("%w: unsupported manifest media type %q", ErrUnexpectedArtifactType, manifest.MediaType)
	}
	if len(manifest.Layers) > limits.Files {
		return ManifestInfo{}, fmt.Errorf("%w: manifest contains %d files, maximum is %d",
			ErrArtifactLimit, len(manifest.Layers), limits.Files)
	}
	info := ManifestInfo{
		Reference:    ref,
		Descriptor:   descriptor,
		ArtifactType: manifest.ArtifactType,
		Annotations:  maps.Clone(manifest.Annotations),
		Layers:       make([]ocispec.Descriptor, 0, len(manifest.Layers)),
	}
	if manifest.Subject != nil {
		subject := *manifest.Subject
		subject.Annotations = maps.Clone(subject.Annotations)
		info.Subject = &subject
	}
	for _, layer := range manifest.Layers {
		layer.Annotations = maps.Clone(layer.Annotations)
		info.Layers = append(info.Layers, layer)
	}
	return info, nil
}

// DiscoverReferrers lists referrers and records which mechanism found each.
//
// [Client.Referrers] answers the same question with the sources already merged
// away, which is the right shape for a caller that only wants the list. This
// one keeps the provenance.
func (c *Client) DiscoverReferrers(
	ctx context.Context,
	subjectRef,
	artifactType string,
) (ocispec.Descriptor, []DiscoveredReferrer, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	ref, err := ParseRef(subjectRef)
	if err != nil {
		return ocispec.Descriptor{}, nil, err
	}
	repository, err := c.repository(ref)
	if err != nil {
		return ocispec.Descriptor{}, nil, err
	}
	subject, err := repository.Resolve(ctx, ref.Selector())
	if err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("resolve referrer subject %s: %w", ref, err)
	}
	limits := c.options.Limits.normalized()
	standard, err := listReferrersFrom(ctx, repository, subject, artifactType, limits.Referrers)
	if err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("list referrers for %s: %w", ref, err)
	}
	durable, err := listDurableReferrers(ctx, repository, subject, artifactType, limits)
	if err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("list referrers for %s: %w", ref, err)
	}
	discovered, err := mergeDiscovered(standard, durable, limits.Referrers)
	if err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("list referrers for %s: %w", ref, err)
	}
	return subject, discovered, nil
}

// ReferrerRepository is the registry surface referrer discovery reads.
//
// It is exported for the same reason [ListReferrersFrom] is: the discovery
// logic is worth testing against a substitute repository, and a test that has
// to stand up a live registry to reach it is a test nobody runs.
type ReferrerRepository interface {
	oras.ReadOnlyTarget
	registry.ReferrerLister
	registry.TagLister
}

// DiscoverReferrersFrom lists a subject's referrers from one repository and
// records which mechanism found each. [Client.DiscoverReferrers] is this over a
// live registry, with the subject resolved from a reference first.
func DiscoverReferrersFrom(
	ctx context.Context,
	repository ReferrerRepository,
	subject ocispec.Descriptor,
	artifactType string,
	limits Limits,
) ([]DiscoveredReferrer, error) {
	if repository == nil {
		return nil, fmt.Errorf("OCI referrer repository is required")
	}
	limits = limits.normalized()
	standard, err := listReferrersFrom(ctx, repository, subject, artifactType, limits.Referrers)
	if err != nil {
		return nil, err
	}
	durable, err := listDurableReferrers(ctx, repository, subject, artifactType, limits)
	if err != nil {
		return nil, err
	}
	return mergeDiscovered(standard, durable, limits.Referrers)
}

// mergeDiscovered unions the two mechanisms' answers and keeps the provenance.
//
// The union must match what [mergeReferrers] produces, because a caller that
// saw a referrer here and could not fetch it there would be reading two
// different registries. What differs is only that this one records which side
// each descriptor came from.
func mergeDiscovered(
	standard,
	durable []ocispec.Descriptor,
	limit int,
) ([]DiscoveredReferrer, error) {
	fromDurable := make(map[string]struct{}, len(durable))
	for _, descriptor := range durable {
		fromDurable[descriptor.Digest.String()] = struct{}{}
	}
	result := make([]DiscoveredReferrer, 0, len(standard)+len(durable))
	seen := make(map[string]struct{}, len(standard)+len(durable))
	for i, descriptor := range slices.Concat(standard, durable) {
		key := descriptor.Digest.String()
		if _, exists := seen[key]; exists {
			continue
		}
		if len(result) >= limit {
			return nil, fmt.Errorf("%w: more than %d referrers", ErrArtifactLimit, limit)
		}
		seen[key] = struct{}{}
		source := ReferrerSourceDurableTag
		if i < len(standard) {
			source = ReferrerSourceAPI
			if _, both := fromDurable[key]; both {
				source = ReferrerSourceBoth
			}
		}
		descriptor.Annotations = maps.Clone(descriptor.Annotations)
		result = append(result, DiscoveredReferrer{Descriptor: descriptor, Source: source})
	}
	slices.SortFunc(result, func(left, right DiscoveredReferrer) int {
		if order := strings.Compare(left.Descriptor.ArtifactType, right.Descriptor.ArtifactType); order != 0 {
			return order
		}
		return strings.Compare(left.Descriptor.Digest.String(), right.Descriptor.Digest.String())
	})
	return result, nil
}

// FetchReferrer pulls one referrer of subjectRef, named by its own digest.
//
// The digest has to be a referrer OF THAT SUBJECT, and this checks it rather
// than trusting the caller. Skipping the check would turn the fetch verb into a
// general-purpose manifest reader, which stokaro/ptah#1143 puts out of scope on
// purpose — `oras` already exists for that — and would let a typo silently
// return an unrelated artifact that happens to live in the same repository.
//
// The check costs a listing even when the caller has already listed, which the
// CLI verb has. That is deliberate: the safety property belongs to this
// function rather than to whoever calls it, and an API whose guarantee holds
// only when the caller remembers to establish it first is not a guarantee.
func (c *Client) FetchReferrer(
	ctx context.Context,
	subjectRef,
	referrerDigest string,
) (Artifact, error) {
	referrerDigest = strings.TrimSpace(referrerDigest)
	if referrerDigest == "" {
		return Artifact{}, fmt.Errorf("referrer digest is required")
	}
	_, discovered, err := c.DiscoverReferrers(ctx, subjectRef, "")
	if err != nil {
		return Artifact{}, err
	}
	if !slices.ContainsFunc(discovered, func(candidate DiscoveredReferrer) bool {
		return candidate.Descriptor.Digest.String() == referrerDigest
	}) {
		return Artifact{}, fmt.Errorf(
			"%s is not a referrer of %s: fetch reads metadata Ptah attached to this artifact, "+
				"not arbitrary manifests in its repository",
			referrerDigest, subjectRef)
	}

	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	ref, err := ParseRef(subjectRef)
	if err != nil {
		return Artifact{}, err
	}
	repository, err := c.repository(ref)
	if err != nil {
		return Artifact{}, err
	}
	artifact, err := PullFrom(ctx, repository, referrerDigest, PullOptions{
		Limits: c.options.Limits,
	})
	if err != nil {
		return Artifact{}, fmt.Errorf("fetch referrer %s of %s: %w", referrerDigest, ref, err)
	}
	artifact.Reference = ref
	return artifact, nil
}
