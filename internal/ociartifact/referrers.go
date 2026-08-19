package ociartifact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry"
)

const (
	attachmentLockCount               = 64
	durableReferrerTagPrefix          = "ptah-r-"
	referrerVisibilityTimeout         = 5 * time.Second
	initialReferrerVisibilityInterval = 25 * time.Millisecond
	maxReferrerVisibilityInterval     = 500 * time.Millisecond
)

var attachmentLocks [attachmentLockCount]sync.Mutex

type referrerRepository interface {
	oras.ReadOnlyTarget
	registry.ReferrerLister
	registry.TagLister
}

// AttachmentOptions describes an artifact attached to an existing manifest.
type AttachmentOptions struct {
	ArtifactType   string
	LayerMediaType string
	Annotations    map[string]string
	Limits         Limits
	// Policy decides which discovery mechanism the attachment gets. The zero
	// value is [ReferrerPolicyAuto].
	Policy ReferrerPolicy
}

// Attach stores fsys as a referrer of subjectRef.
func Attach(
	ctx context.Context,
	subjectRef string,
	fsys fs.FS,
	opts AttachmentOptions,
) (PushResult, error) {
	client, err := NewClient(ClientOptions{})
	if err != nil {
		return PushResult{}, err
	}
	return client.Attach(ctx, subjectRef, fsys, opts)
}

// Referrers lists artifacts that directly reference subjectRef. An empty
// artifactType returns every attachment.
func Referrers(ctx context.Context, subjectRef, artifactType string) ([]ocispec.Descriptor, error) {
	client, err := NewClient(ClientOptions{})
	if err != nil {
		return nil, err
	}
	return client.Referrers(ctx, subjectRef, artifactType)
}

// Attach stores fsys as a referrer using this client's transport.
func (c *Client) Attach(
	ctx context.Context,
	subjectRef string,
	fsys fs.FS,
	opts AttachmentOptions,
) (PushResult, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	ref, err := ParseRef(subjectRef)
	if err != nil {
		return PushResult{}, err
	}
	repository, err := c.repository(ref)
	if err != nil {
		return PushResult{}, err
	}
	target, err := c.attachmentTarget(ref)
	if err != nil {
		return PushResult{}, err
	}
	opts.Limits = mergeLimits(opts.Limits, c.options.Limits)
	subject, err := repository.Resolve(ctx, ref.Selector())
	if err != nil {
		return PushResult{}, fmt.Errorf("resolve attachment subject %s: %w", ref, err)
	}
	opts.Policy = c.effectiveReferrerPolicy(opts.Policy)
	result, err := attachResolved(ctx, target, repository, ref, subject, fsys, opts, c.indexProbe(subjectRef))
	if err != nil {
		return result, fmt.Errorf("attach to %s: %w", ref, err)
	}
	return result, nil
}

// AttachResolved stores fsys as a referrer of an already resolved descriptor.
// This prevents a mutable subject tag from moving between pull and attachment.
func (c *Client) AttachResolved(
	ctx context.Context,
	subjectRef string,
	subject ocispec.Descriptor,
	fsys fs.FS,
	opts AttachmentOptions,
) (PushResult, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	ref, err := ParseRef(subjectRef)
	if err != nil {
		return PushResult{}, err
	}
	repository, err := c.repository(ref)
	if err != nil {
		return PushResult{}, err
	}
	target, err := c.attachmentTarget(ref)
	if err != nil {
		return PushResult{}, err
	}
	opts.Limits = mergeLimits(opts.Limits, c.options.Limits)
	opts.Policy = c.effectiveReferrerPolicy(opts.Policy)
	result, err := attachResolved(ctx, target, repository, ref, subject, fsys, opts, c.indexProbe(subjectRef))
	if err != nil {
		return result, fmt.Errorf("attach to resolved subject %s: %w", ref, err)
	}
	return result, nil
}

// indexProbe answers whether the registry serves the referrers index. It is a
// parameter rather than a method call so that the policy decision can be tested
// without a registry that has to be persuaded to lack a capability.
type indexProbe func(context.Context) (bool, string, error)

// resolveAttachmentPolicy turns the operator's policy into the concrete one the
// write will use, asking the registry only where the answer changes what
// happens.
//
// required-api asks BEFORE anything is written, which is the whole difference
// between it and api: a pipeline that must not publish an undiscoverable
// attachment fails without having created one, rather than failing afterwards
// with the artifact already in the registry.
func resolveAttachmentPolicy(
	ctx context.Context,
	policy ReferrerPolicy,
	probe indexProbe,
) (ReferrerPolicy, error) {
	switch policy.normalized() {
	case ReferrerPolicyAPI, ReferrerPolicyTag:
		return policy.normalized(), nil
	case ReferrerPolicyRequiredAPI:
		supported, detail, err := probe(ctx)
		if err != nil {
			return "", fmt.Errorf("ask the registry for the referrers index: %w", err)
		}
		if !supported {
			return "", fmt.Errorf("%w: the registry does not serve the referrers index%s",
				ErrReferrerIndexRequired, detailSuffix(detail))
		}
		return ReferrerPolicyAPI, nil
	default:
		supported, _, err := probe(ctx)
		if err != nil {
			return "", fmt.Errorf("ask the registry for the referrers index: %w", err)
		}
		if supported {
			return ReferrerPolicyAPI, nil
		}
		return ReferrerPolicyTag, nil
	}
}

func detailSuffix(detail string) string {
	if strings.TrimSpace(detail) == "" {
		return ""
	}
	return " (" + strings.TrimSpace(detail) + ")"
}

func attachResolved(
	ctx context.Context,
	target oras.Target,
	repository referrerRepository,
	ref Reference,
	subject ocispec.Descriptor,
	fsys fs.FS,
	opts AttachmentOptions,
	probe indexProbe,
) (PushResult, error) {
	if err := validateAttachmentSubject(subject); err != nil {
		return PushResult{}, err
	}
	resolvedPolicy, err := resolveAttachmentPolicy(ctx, opts.Policy, probe)
	if err != nil {
		return PushResult{}, err
	}
	opts.Policy = resolvedPolicy
	lock := attachmentLock(ref, subject)
	lock.Lock()
	defer lock.Unlock()

	resolved, err := target.Resolve(ctx, subject.Digest.String())
	if err != nil {
		return PushResult{}, fmt.Errorf("resolve attachment subject digest %s: %w", subject.Digest, err)
	}
	if resolved.Digest != subject.Digest ||
		resolved.MediaType != subject.MediaType ||
		resolved.Size != subject.Size {
		return PushResult{}, fmt.Errorf("attachment subject descriptor does not match repository content")
	}
	result, err := AttachTo(ctx, target, subject, fsys, opts)
	if err != nil {
		return result, err
	}
	if err := ensureReferrerVisible(
		ctx,
		repository,
		subject,
		result.Descriptor,
		opts.ArtifactType,
		opts.Limits.normalized(),
	); err != nil {
		return result, err
	}
	result.Reference = ref
	return result, nil
}

func attachmentLock(ref Reference, subject ocispec.Descriptor) *sync.Mutex {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	hash := offset
	for _, value := range []string{ref.repositoryName(), subject.Digest.String()} {
		for index := range len(value) {
			hash ^= uint64(value[index])
			hash *= prime
		}
	}
	return &attachmentLocks[hash%attachmentLockCount]
}

func ensureReferrerVisible(
	ctx context.Context,
	repository referrerRepository,
	subject,
	attachment ocispec.Descriptor,
	artifactType string,
	limits Limits,
) error {
	visibilityCtx, cancel := context.WithTimeout(ctx, referrerVisibilityTimeout)
	defer cancel()

	interval := initialReferrerVisibilityInterval
	var lastErr error
	for {
		referrers, err := listDiscoverableReferrers(visibilityCtx, repository, subject, artifactType, limits)
		if err != nil && permanentReferrerVisibilityError(err) {
			return fmt.Errorf("verify OCI referrer discovery: %w", err)
		}
		if err == nil {
			for _, referrer := range referrers {
				if referrer.Digest == attachment.Digest {
					return nil
				}
			}
		}
		lastErr = err

		timer := time.NewTimer(interval)
		select {
		case <-visibilityCtx.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if lastErr != nil {
				return fmt.Errorf("verify OCI referrer discovery: %w", lastErr)
			}
			return fmt.Errorf("%w after %s: %s", ErrReferrerNotIndexed, referrerVisibilityTimeout, attachment.Digest)
		case <-timer.C:
			interval = min(interval*2, maxReferrerVisibilityInterval)
		}
	}
}

func permanentReferrerVisibilityError(err error) bool {
	return errors.Is(err, ErrArtifactLimit) ||
		errors.Is(err, ErrUnexpectedArtifactType) ||
		errors.Is(err, errdef.ErrTooManyPages)
}

// Referrers lists attachments using this client's transport.
func (c *Client) Referrers(
	ctx context.Context,
	subjectRef,
	artifactType string,
) ([]ocispec.Descriptor, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	ref, err := ParseRef(subjectRef)
	if err != nil {
		return nil, err
	}
	repository, err := c.repository(ref)
	if err != nil {
		return nil, err
	}
	subject, err := repository.Resolve(ctx, ref.Selector())
	if err != nil {
		return nil, fmt.Errorf("resolve referrer subject %s: %w", ref, err)
	}
	referrers, err := listDiscoverableReferrers(
		ctx,
		repository,
		subject,
		artifactType,
		c.options.Limits,
	)
	if err != nil {
		return nil, fmt.Errorf("list referrers for %s: %w", ref, err)
	}
	return referrers, nil
}

// AttachTo pushes an OCI manifest whose subject is subject and gives it a
// content-derived discovery tag that concurrent writers cannot overwrite.
func AttachTo(
	ctx context.Context,
	target oras.Target,
	subject ocispec.Descriptor,
	fsys fs.FS,
	opts AttachmentOptions,
) (PushResult, error) {
	if err := validateAttachmentSubject(subject); err != nil {
		return PushResult{}, err
	}
	result, err := PushTo(ctx, target, fsys, PushOptions{
		ArtifactType:   opts.ArtifactType,
		LayerMediaType: opts.LayerMediaType,
		Annotations:    opts.Annotations,
		Subject:        &subject,
		Limits:         opts.Limits,
	})
	if err != nil {
		return result, err
	}
	if !opts.Policy.WritesDurableTag() {
		return result, nil
	}
	tag, err := durableReferrerTag(subject, result.Descriptor)
	if err != nil {
		return result, err
	}
	if err := target.Tag(ctx, result.Descriptor, tag); err != nil {
		return result, &PartialPushError{
			Descriptor:  result.Descriptor,
			AppliedTags: slices.Clone(result.Tags),
			FailedTag:   tag,
			Err:         err,
		}
	}
	result.Tags = append(result.Tags, tag)
	return result, nil
}

func validateAttachmentSubject(subject ocispec.Descriptor) error {
	if subject.Digest == "" {
		return fmt.Errorf("attachment subject digest is required")
	}
	if err := validateLayerDigest(subject); err != nil {
		return fmt.Errorf("validate attachment subject: %w", err)
	}
	if subject.MediaType != ocispec.MediaTypeImageManifest {
		return fmt.Errorf("attachment subject has unsupported media type %q", subject.MediaType)
	}
	if subject.Size <= 0 {
		return fmt.Errorf("attachment subject size must be positive")
	}
	return nil
}

func durableReferrerTag(
	subject,
	attachment ocispec.Descriptor,
) (string, error) {
	if err := validateLayerDigest(subject); err != nil {
		return "", fmt.Errorf("validate durable referrer subject: %w", err)
	}
	if err := validateLayerDigest(attachment); err != nil {
		return "", fmt.Errorf("validate durable referrer attachment: %w", err)
	}
	return durableReferrerTagPrefix +
		subject.Digest.Encoded()[:32] + "-" +
		attachment.Digest.Encoded(), nil
}

func listDiscoverableReferrers(
	ctx context.Context,
	repository referrerRepository,
	subject ocispec.Descriptor,
	artifactType string,
	limits Limits,
) ([]ocispec.Descriptor, error) {
	limits = limits.normalized()
	standard, err := listReferrersFrom(ctx, repository, subject, artifactType, limits.Referrers)
	if err != nil {
		return nil, err
	}
	durable, err := listDurableReferrers(ctx, repository, subject, artifactType, limits)
	if err != nil {
		return nil, err
	}
	return mergeReferrers(standard, durable, limits.Referrers)
}

func listDurableReferrers(
	ctx context.Context,
	repository referrerRepository,
	subject ocispec.Descriptor,
	artifactType string,
	limits Limits,
) ([]ocispec.Descriptor, error) {
	if err := validateLayerDigest(subject); err != nil {
		return nil, fmt.Errorf("validate durable referrer subject: %w", err)
	}
	prefix := durableReferrerTagPrefix + subject.Digest.Encoded()[:32] + "-"
	var tags []string
	err := repository.Tags(ctx, "", func(page []string) error {
		for _, tag := range page {
			if !strings.HasPrefix(tag, prefix) {
				continue
			}
			if len(tags) >= limits.Referrers {
				return fmt.Errorf("%w: more than %d durable referrers", ErrArtifactLimit, limits.Referrers)
			}
			tags = append(tags, tag)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list durable OCI referrer tags: %w", err)
	}

	referrers := make([]ocispec.Descriptor, 0, len(tags))
	for _, tag := range tags {
		descriptor, manifestBytes, err := oras.FetchBytes(ctx, repository, tag, oras.FetchBytesOptions{
			MaxBytes: limits.ManifestBytes,
		})
		if err != nil {
			return nil, fmt.Errorf("fetch durable OCI referrer %q: %w", tag, err)
		}
		referrer, err := validateDurableReferrer(subject, descriptor, manifestBytes, tag)
		if err != nil {
			return nil, err
		}
		if artifactType == "" || referrer.ArtifactType == artifactType {
			referrers = append(referrers, referrer)
		}
	}
	return referrers, nil
}

func validateDurableReferrer(
	subject,
	descriptor ocispec.Descriptor,
	manifestBytes []byte,
	tag string,
) (ocispec.Descriptor, error) {
	if descriptor.MediaType != ocispec.MediaTypeImageManifest {
		return ocispec.Descriptor{}, fmt.Errorf(
			"%w: durable OCI referrer %q has unsupported media type %q",
			ErrUnexpectedArtifactType,
			tag,
			descriptor.MediaType,
		)
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf(
			"%w: decode durable OCI referrer %q: %v",
			ErrUnexpectedArtifactType,
			tag,
			err,
		)
	}
	if manifest.SchemaVersion != 2 ||
		manifest.MediaType != ocispec.MediaTypeImageManifest ||
		manifest.Subject == nil ||
		!sameDescriptor(*manifest.Subject, subject) {
		return ocispec.Descriptor{}, fmt.Errorf(
			"%w: durable OCI referrer %q does not reference the requested subject",
			ErrUnexpectedArtifactType,
			tag,
		)
	}
	expectedTag, err := durableReferrerTag(subject, descriptor)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	if tag != expectedTag {
		return ocispec.Descriptor{}, fmt.Errorf(
			"%w: durable OCI referrer tag %q does not match manifest digest %s",
			ErrUnexpectedArtifactType,
			tag,
			descriptor.Digest,
		)
	}
	descriptor.ArtifactType = manifest.ArtifactType
	descriptor.Annotations = maps.Clone(manifest.Annotations)
	return descriptor, nil
}

func sameDescriptor(left, right ocispec.Descriptor) bool {
	return left.Digest == right.Digest &&
		left.MediaType == right.MediaType &&
		left.Size == right.Size
}

func mergeReferrers(
	groups,
	additional []ocispec.Descriptor,
	limit int,
) ([]ocispec.Descriptor, error) {
	result := make([]ocispec.Descriptor, 0, len(groups)+len(additional))
	seen := make(map[string]struct{}, len(groups)+len(additional))
	for _, descriptor := range append(groups, additional...) {
		key := descriptor.Digest.String()
		if _, exists := seen[key]; exists {
			continue
		}
		if len(result) >= limit {
			return nil, fmt.Errorf("%w: more than %d referrers", ErrArtifactLimit, limit)
		}
		seen[key] = struct{}{}
		descriptor.Annotations = maps.Clone(descriptor.Annotations)
		result = append(result, descriptor)
	}
	return result, nil
}

// ListReferrersFrom lists and defensively clones referrer descriptors.
func ListReferrersFrom(
	ctx context.Context,
	lister registry.ReferrerLister,
	subject ocispec.Descriptor,
	artifactType string,
) ([]ocispec.Descriptor, error) {
	return listReferrersFrom(
		ctx,
		lister,
		subject,
		artifactType,
		Limits{}.normalized().Referrers,
	)
}

func listReferrersFrom(
	ctx context.Context,
	lister registry.ReferrerLister,
	subject ocispec.Descriptor,
	artifactType string,
	limit int,
) ([]ocispec.Descriptor, error) {
	if lister == nil {
		return nil, fmt.Errorf("OCI referrer lister is required")
	}
	if subject.Digest == "" {
		return nil, fmt.Errorf("referrer subject digest is required")
	}
	var result []ocispec.Descriptor
	err := lister.Referrers(ctx, subject, artifactType, func(page []ocispec.Descriptor) error {
		if len(page) > limit-len(result) {
			return fmt.Errorf("%w: more than %d referrers", ErrArtifactLimit, limit)
		}
		for _, descriptor := range page {
			descriptor.Annotations = maps.Clone(descriptor.Annotations)
			result = append(result, descriptor)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// indexProbe asks this client's registry whether it serves the referrers index.
//
// The question is put on a repository client of its own rather than the one the
// attachment uses, because pinning the capability is a one-shot operation and
// the attachment still needs its own auto-detection intact afterwards.
func (c *Client) indexProbe(subjectRef string) indexProbe {
	return func(ctx context.Context) (bool, string, error) {
		capabilities, err := c.Capabilities(ctx, subjectRef)
		if err != nil {
			return false, "", err
		}
		return capabilities.ReferrersAPI, capabilities.Detail, nil
	}
}

// effectiveReferrerPolicy lets an attachment override the client's policy and
// falls back to it otherwise, so a caller that names nothing inherits the
// decision the run was configured with rather than a hardcoded default.
func (c *Client) effectiveReferrerPolicy(attachment ReferrerPolicy) ReferrerPolicy {
	if attachment != "" {
		return attachment
	}
	return c.options.ReferrerPolicy.normalized()
}
