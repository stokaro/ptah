package ociartifact

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/errdef"
)

// ErrTagRequired reports a promotion asked for without naming the alias to move.
var ErrTagRequired = errors.New("a tag to move is required")

// RegistryCapabilities is what a registry was observed to support.
//
// Ptah writes referrers two ways and merges them on read, which makes its own
// discovery robust and says nothing about anyone else's. An operator running an
// audit pipeline has to be able to ask the registry directly, because the
// answer decides whether a referrer this repository published is discoverable
// by a client that is not Ptah.
type RegistryCapabilities struct {
	Reference Reference
	// ReferrersAPI is true when the registry answered the referrers endpoint
	// defined by the distribution specification.
	ReferrersAPI bool
	// Detail carries the registry's own words when the API is absent, so a
	// refusal is distinguishable from a network failure.
	Detail string
}

// Tags lists the tags of a reference's repository, in registry order.
func (c *Client) Tags(ctx context.Context, rawRef string) ([]string, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	ref, err := ParseRef(rawRef)
	if err != nil {
		return nil, err
	}
	repository, err := c.repository(ref)
	if err != nil {
		return nil, err
	}
	var tags []string
	limit := c.options.Limits.normalized().Referrers
	err = repository.Tags(ctx, "", func(page []string) error {
		for _, tag := range page {
			if len(tags) >= limit {
				return fmt.Errorf("%w: more than %d tags", ErrArtifactLimit, limit)
			}
			tags = append(tags, tag)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list tags of %s: %w", ref, err)
	}
	return tags, nil
}

// Retag moves an alias onto the manifest a reference already resolves to,
// without rebuilding or re-uploading anything.
//
// This is the operation `build -> staging -> production` needs and the one a
// push cannot provide: a push creates an immutable artifact AND moves aliases,
// so promoting through it re-derives content that was already reviewed. Moving
// the alias alone keeps the digest identical by construction, which is what
// makes the promoted artifact the same artifact rather than an equal one.
func (c *Client) Retag(ctx context.Context, rawRef string, tags []string) (ocispec.Descriptor, []string, error) {
	tags, err := validatedTags(tags)
	if err != nil {
		return ocispec.Descriptor{}, nil, err
	}
	if len(tags) == 0 {
		return ocispec.Descriptor{}, nil, ErrTagRequired
	}

	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	ref, err := ParseRef(rawRef)
	if err != nil {
		return ocispec.Descriptor{}, nil, err
	}
	repository, err := c.repository(ref)
	if err != nil {
		return ocispec.Descriptor{}, nil, err
	}
	descriptor, err := repository.Resolve(ctx, ref.Selector())
	if err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("resolve %s: %w", ref, err)
	}
	// Aliases move one at a time and the applied ones are reported even when a
	// later one fails, for the same reason a partial push reports what it did:
	// an operator who is told only "it failed" has to go and look at the
	// registry to learn which environment now points at the new build.
	var applied []string
	for _, tag := range tags {
		if err := repository.Tag(ctx, descriptor, tag); err != nil {
			return descriptor, applied, &PartialPushError{
				Descriptor:  descriptor,
				AppliedTags: applied,
				FailedTag:   tag,
				Err:         err,
			}
		}
		applied = append(applied, tag)
	}
	return descriptor, applied, nil
}

// ArtifactCopyOptions configures a copy between repositories.
type ArtifactCopyOptions struct {
	// Recursive carries the artifact's referrers with it. Without it a promoted
	// artifact arrives with its lint results, plans, deployment reports and
	// signatures left behind in the source repository, which is the shape that
	// makes a promotion silently lose the evidence it was promoted on.
	Recursive bool
	// Tags are additional aliases to apply at the destination.
	Tags []string
}

// copyEndpoint is one side of a copy, resolved to something oras can read or
// write and the selector that names the artifact within it.
type copyEndpoint struct {
	target   oras.Target
	selector string
	display  string
}

// copySource opens the side a copy reads from, registry or image layout.
//
// Both kinds resolve the same way on purpose. An export to a directory and an
// import from one are the same operation as a registry-to-registry copy, and
// making them a separate verb would give an air-gapped environment a second
// code path to trust.
func (c *Client) copySource(raw string) (copyEndpoint, error) {
	if IsLayoutRef(raw) {
		return openLayoutEndpoint(raw)
	}
	ref, err := ParseRef(raw)
	if err != nil {
		return copyEndpoint{}, err
	}
	repository, err := c.repository(ref)
	if err != nil {
		return copyEndpoint{}, err
	}
	return copyEndpoint{target: repository, selector: ref.Selector(), display: ref.String()}, nil
}

// copyDestination opens the side a copy writes to, and refuses a digest.
//
// A digest names content that already exists, so there is nothing for a copy to
// create at that address.
func (c *Client) copyDestination(raw string) (copyEndpoint, error) {
	if IsLayoutRef(raw) {
		return openLayoutEndpoint(raw)
	}
	ref, err := ParseRef(raw)
	if err != nil {
		return copyEndpoint{}, err
	}
	if ref.IsDigest() {
		return copyEndpoint{}, fmt.Errorf("%w: %s", ErrDigestPush, ref)
	}
	repository, err := c.repository(ref)
	if err != nil {
		return copyEndpoint{}, err
	}
	return copyEndpoint{target: repository, selector: ref.Selector(), display: ref.String()}, nil
}

func openLayoutEndpoint(raw string) (copyEndpoint, error) {
	target, tag, err := OpenLayout(raw)
	if err != nil {
		return copyEndpoint{}, err
	}
	return copyEndpoint{target: target, selector: tag, display: strings.TrimSpace(raw)}, nil
}

// CopyArtifact copies one artifact between repositories, preserving its digest.
//
// Either side may be an oci-layout:// directory, which is what an air-gapped
// environment has instead of a network: export on one side of the gap, carry
// the directory across, import on the other.
//
// The destination selector is used as the destination tag, so a copy names what
// it is creating rather than inheriting the source's alias silently.
func (c *Client) CopyArtifact(
	ctx context.Context,
	srcRef string,
	dstRef string,
	opts ArtifactCopyOptions,
) (ocispec.Descriptor, error) {
	extra, err := validatedTags(opts.Tags)
	if err != nil {
		return ocispec.Descriptor{}, err
	}

	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	source, err := c.copySource(srcRef)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("parse source: %w", err)
	}
	destination, err := c.copyDestination(dstRef)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("parse destination: %w", err)
	}

	var descriptor ocispec.Descriptor
	if opts.Recursive {
		graph, ok := source.target.(oras.ReadOnlyGraphTarget)
		if !ok {
			return ocispec.Descriptor{}, fmt.Errorf(
				"the source %s cannot enumerate referrers, so a recursive copy would silently carry none",
				source.display)
		}
		descriptor, err = oras.ExtendedCopy(ctx, graph, source.selector, destination.target,
			destination.selector, oras.ExtendedCopyOptions{})
	} else {
		descriptor, err = oras.Copy(ctx, source.target, source.selector, destination.target,
			destination.selector, oras.CopyOptions{})
	}
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("copy %s to %s: %w", source.display, destination.display, err)
	}
	for _, tag := range extra {
		if err := destination.target.Tag(ctx, descriptor, tag); err != nil {
			return descriptor, &PartialPushError{
				Descriptor: descriptor,
				FailedTag:  tag,
				Err:        err,
			}
		}
	}
	return descriptor, nil
}

// Capabilities asks the registry what it supports, rather than inferring it.
//
// The referrers question is answered by making the request and reading the
// answer: the repository client is pinned to the API so it cannot quietly fall
// back to the tag schema, and a refusal that names the API as unsupported is
// the registry saying no. Anything else is a failure to ask, which is reported
// as such rather than folded into a no.
func (c *Client) Capabilities(ctx context.Context, rawRef string) (RegistryCapabilities, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	ref, err := ParseRef(rawRef)
	if err != nil {
		return RegistryCapabilities{}, err
	}
	repository, err := c.repository(ref)
	if err != nil {
		return RegistryCapabilities{}, err
	}
	subject, err := repository.Resolve(ctx, ref.Selector())
	if err != nil {
		return RegistryCapabilities{}, fmt.Errorf("resolve %s: %w", ref, err)
	}
	if err := repository.SetReferrersCapability(true); err != nil {
		return RegistryCapabilities{}, fmt.Errorf("pin the referrers capability of %s: %w", ref, err)
	}
	capabilities := RegistryCapabilities{Reference: ref}
	err = repository.Referrers(ctx, subject, "", func([]ocispec.Descriptor) error { return nil })
	switch {
	case err == nil:
		capabilities.ReferrersAPI = true
	case errors.Is(err, errdef.ErrUnsupported):
		capabilities.Detail = strings.TrimSpace(err.Error())
	default:
		return RegistryCapabilities{}, fmt.Errorf("ask %s for the referrers API: %w", ref, err)
	}
	return capabilities, nil
}

// SortedTags returns tags in a stable order for output.
func SortedTags(tags []string) []string {
	out := slices.Clone(tags)
	slices.SortFunc(out, strings.Compare)
	return out
}
