// Package ociartifact stores Ptah artifacts in OCI-compliant registries.
package ociartifact

import (
	"crypto/rand"
	_ "crypto/sha256" // Register SHA-256 for OCI digest validation.
	"errors"
	"fmt"
	"strings"
	"time"

	"oras.land/oras-go/v2/registry"
)

const (
	// Scheme is the URL scheme accepted by Ptah for OCI artifacts.
	Scheme = "oci://"
	// DefaultTag is used when an OCI reference does not name a tag or digest.
	DefaultTag = "latest"
)

var (
	// ErrInvalidReference reports a malformed or unsupported OCI reference.
	ErrInvalidReference = errors.New("invalid OCI reference")
	// ErrDigestPush reports an attempt to push to an immutable digest selector.
	ErrDigestPush = errors.New("cannot push to a digest reference")
	// ErrUnexpectedArtifactType reports an artifact whose type is not accepted.
	ErrUnexpectedArtifactType = errors.New("unexpected OCI artifact type")
	// ErrUnsafeArtifactPath reports a file-layer title that is unsafe to extract.
	ErrUnsafeArtifactPath = errors.New("unsafe OCI artifact path")
	// ErrArtifactLimit reports an artifact that exceeds configured resource limits.
	ErrArtifactLimit = errors.New("OCI artifact limit exceeded")
	// ErrTagConflict reports a write-once tag that already names different content.
	ErrTagConflict = errors.New("OCI write-once tag already exists")
	// ErrReferrerNotIndexed reports an attachment missing from referrer discovery.
	ErrReferrerNotIndexed = errors.New("OCI attachment is not discoverable as a referrer")
)

// Reference is a parsed oci:// registry reference.
type Reference struct {
	registry   string
	repository string
	selector   string
	tag        string
	digest     bool
}

// ParseRef parses an oci://registry/repository[:tag][@digest] reference.
// An omitted selector resolves to :latest.
//
// A reference may name a tag and a digest together, which is the shape a
// promotion pipeline emits: "the artifact we call :release, resolved to these
// bytes". The digest always decides which bytes are fetched, exactly as every
// other OCI client behaves when the two disagree; the tag is kept for display
// only, because dropping what the author wrote is itself a silent discard.
func ParseRef(raw string) (Reference, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return Reference{}, fmt.Errorf("%w: reference must not be empty or contain surrounding whitespace", ErrInvalidReference)
	}
	value, ok := strings.CutPrefix(raw, Scheme)
	if !ok {
		return Reference{}, fmt.Errorf("%w: expected %s prefix", ErrInvalidReference, Scheme)
	}
	if err := validateReferenceText(value); err != nil {
		return Reference{}, err
	}
	parsed, err := registry.ParseReference(value)
	if err != nil {
		return Reference{}, fmt.Errorf("%w: %v", ErrInvalidReference, err)
	}
	selector := parsed.ReferenceOrDefault()
	isDigest := strings.Contains(selector, ":")
	tag := selector
	if isDigest {
		// oras-go resolves tag@digest by the digest and drops the tag from the
		// parsed value, so recover the author's tag from the raw text.
		tag = tagBeforeDigest(value)
	}
	return Reference{
		registry:   parsed.Registry,
		repository: parsed.Repository,
		selector:   selector,
		tag:        tag,
		digest:     isDigest,
	}, nil
}

func validateReferenceText(value string) error {
	switch {
	case strings.ContainsAny(value, "?#"):
		return fmt.Errorf("%w: query strings and fragments are not supported", ErrInvalidReference)
	case strings.ContainsAny(value, "\\\x00"):
		return fmt.Errorf("%w: backslashes and NUL bytes are not supported", ErrInvalidReference)
	case containsEscapedSeparator(value):
		return fmt.Errorf("%w: escaped path separators are not supported", ErrInvalidReference)
	case hasUserInfo(value):
		return fmt.Errorf("%w: embedded credentials are not supported", ErrInvalidReference)
	default:
		return nil
	}
}

func containsEscapedSeparator(value string) bool {
	lower := strings.ToLower(value)
	return strings.Contains(lower, "%2f") || strings.Contains(lower, "%5c")
}

func hasUserInfo(value string) bool {
	firstSlash := strings.IndexByte(value, '/')
	firstAt := strings.IndexByte(value, '@')
	return firstAt >= 0 && (firstSlash < 0 || firstAt < firstSlash)
}

// tagBeforeDigest returns the tag written alongside a digest, or the empty
// string when the reference named no tag. hasUserInfo already rejected an "@"
// before the first "/", so the first "@" here starts the digest.
func tagBeforeDigest(value string) string {
	beforeDigest, _, hasDigest := strings.Cut(value, "@")
	if !hasDigest {
		return ""
	}
	lastSlash := strings.LastIndexByte(beforeDigest, '/')
	_, tag, ok := strings.Cut(beforeDigest[lastSlash+1:], ":")
	if !ok {
		return ""
	}
	return tag
}

// Registry returns the registry host, with an optional port.
func (r Reference) Registry() string {
	return r.registry
}

// Repository returns the repository path within the registry.
func (r Reference) Repository() string {
	return r.repository
}

// Selector returns the tag or digest used to resolve the artifact.
func (r Reference) Selector() string {
	return r.selector
}

// IsDigest reports whether the reference is pinned by content digest.
func (r Reference) IsDigest() bool {
	return r.digest
}

// Tag returns the registry tag the author named, or the empty string when the
// reference named none. A tag is a movable registry pointer: whoever can push
// to the repository can repoint it at other bytes. A reference that names both
// a tag and a digest keeps its tag here while resolving by the digest, so the
// tag is a label, not a selector.
func (r Reference) Tag() string {
	return r.tag
}

// PinnedString renders the canonical oci:// form that selects digest exactly,
// which is the reference an operator should adopt to stop depending on a tag.
func (r Reference) PinnedString(digest string) string {
	return Scheme + r.repositoryName() + "@" + digest
}

// String returns the canonical oci:// form. A reference that named both a tag
// and a digest renders both, so round-tripping through String preserves what
// the author wrote rather than silently narrowing it to the digest.
func (r Reference) String() string {
	if !r.digest {
		return Scheme + r.repositoryName() + ":" + r.selector
	}
	if r.tag == "" {
		return Scheme + r.repositoryName() + "@" + r.selector
	}
	return Scheme + r.repositoryName() + ":" + r.tag + "@" + r.selector
}

func (r Reference) repositoryName() string {
	return r.registry + "/" + r.repository
}

// VersionTag returns a collision-resistant write-once version tag containing a
// UTC timestamp and a cryptographically random suffix.
func VersionTag(timestamp time.Time) string {
	return "v" + timestamp.UTC().Format("20060102150405") + "-" + rand.Text()
}
