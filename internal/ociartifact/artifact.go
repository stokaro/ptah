package ociartifact

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"path"
	"slices"
	"strings"

	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry"

	"github.com/stokaro/ptah/internal/fsnapshot"
)

const (
	// MigrationArtifactType identifies a Ptah migration-directory artifact.
	MigrationArtifactType = "application/vnd.stokaro.ptah.migrations.v1"
	// SchemaArtifactType identifies a Ptah desired-schema artifact.
	SchemaArtifactType = "application/vnd.stokaro.ptah.schema.v1"
	// LintArtifactType identifies lint or analysis results attached to an artifact.
	LintArtifactType = "application/vnd.stokaro.ptah.lint.v1"
	// PlanArtifactType identifies a pre-planned migration attached to an artifact.
	PlanArtifactType = "application/vnd.stokaro.ptah.plan.v1"
	// DeploymentArtifactType identifies a migration deployment report.
	DeploymentArtifactType = "application/vnd.stokaro.ptah.deployment.v1"
	// FileMediaType identifies an individual file layer.
	FileMediaType = "application/vnd.stokaro.ptah.file.v1"
)

const (
	defaultMaxFiles        = 10_000
	defaultMaxFileBytes    = 64 << 20
	defaultMaxTotalBytes   = 512 << 20
	defaultManifestBytes   = 4 << 20
	defaultReferrerPages   = 100
	defaultMaxPathBytes    = 1024
	defaultMaxPathDepth    = 32
	defaultMaxReferrers    = 1000
	annotationArtifactKind = "io.stokaro.ptah.kind"
)

// Limits bounds memory and network use while packaging untrusted artifacts.
type Limits struct {
	Files         int
	FileBytes     int64
	TotalBytes    int64
	ManifestBytes int64
	ReferrerPages int
	PathBytes     int
	PathDepth     int
	Referrers     int
}

func (l Limits) normalized() Limits {
	if l.Files <= 0 {
		l.Files = defaultMaxFiles
	}
	if l.FileBytes <= 0 {
		l.FileBytes = defaultMaxFileBytes
	}
	if l.TotalBytes <= 0 {
		l.TotalBytes = defaultMaxTotalBytes
	}
	if l.ManifestBytes <= 0 {
		l.ManifestBytes = defaultManifestBytes
	}
	if l.ReferrerPages <= 0 {
		l.ReferrerPages = defaultReferrerPages
	}
	if l.PathBytes <= 0 {
		l.PathBytes = defaultMaxPathBytes
	}
	if l.PathDepth <= 0 {
		l.PathDepth = defaultMaxPathDepth
	}
	if l.Referrers <= 0 {
		l.Referrers = defaultMaxReferrers
	}
	return l
}

// PushOptions describes an OCI artifact to create.
type PushOptions struct {
	ArtifactType   string
	LayerMediaType string
	Tags           []string
	// WriteOnceTags are checked before any tags move. A tag already resolving
	// to different content fails the push. Registry-side immutable-tag policy
	// is still required to protect against concurrent writers.
	WriteOnceTags []string
	Annotations   map[string]string
	Subject       *ocispec.Descriptor
	Limits        Limits
}

// PullOptions constrains the artifact accepted from a registry.
type PullOptions struct {
	ExpectedArtifactTypes []string
	LayerMediaType        string
	Limits                Limits
}

// PushResult identifies a pushed artifact and all tags applied to it.
type PushResult struct {
	Reference  Reference
	Descriptor ocispec.Descriptor
	Tags       []string
}

// PartialPushError reports that an immutable manifest was published and some
// tags moved before a later tag operation failed.
type PartialPushError struct {
	Descriptor  ocispec.Descriptor
	AppliedTags []string
	FailedTag   string
	Err         error
}

// Error describes the externally visible state left by a partial push.
func (e *PartialPushError) Error() string {
	return fmt.Sprintf(
		"OCI manifest %s was published and tags %v were applied before tag %q failed: %v",
		e.Descriptor.Digest,
		e.AppliedTags,
		e.FailedTag,
		e.Err,
	)
}

// Unwrap returns the tag operation error.
func (e *PartialPushError) Unwrap() error {
	return e.Err
}

// Artifact is a validated OCI artifact reconstructed as an immutable filesystem.
type Artifact struct {
	Reference    Reference
	Descriptor   ocispec.Descriptor
	ArtifactType string
	Annotations  map[string]string
	FileSystem   fs.FS
}

// PushTo packages fsys and pushes it to target.
func PushTo(ctx context.Context, target oras.Target, fsys fs.FS, opts PushOptions) (PushResult, error) {
	if target == nil {
		return PushResult{}, fmt.Errorf("OCI target is required")
	}
	if fsys == nil {
		return PushResult{}, fmt.Errorf("artifact filesystem is required")
	}
	if strings.TrimSpace(opts.ArtifactType) == "" {
		return PushResult{}, fmt.Errorf("artifact type is required")
	}
	opts.Limits = opts.Limits.normalized()
	if opts.LayerMediaType == "" {
		opts.LayerMediaType = FileMediaType
	}
	tags, err := validatedTags(opts.Tags)
	if err != nil {
		return PushResult{}, err
	}
	writeOnceTags, err := validatedTags(opts.WriteOnceTags)
	if err != nil {
		return PushResult{}, err
	}
	if slices.Contains(writeOnceTags, DefaultTag) {
		return PushResult{}, fmt.Errorf("write-once OCI tag %q is reserved for the movable latest pointer", DefaultTag)
	}
	tags, err = validatedTags(append(writeOnceTags, tags...))
	if err != nil {
		return PushResult{}, err
	}
	tags = moveTagLast(tags, DefaultTag)

	layers, err := pushFileLayers(ctx, target, fsys, opts.LayerMediaType, opts.Limits)
	if err != nil {
		return PushResult{}, err
	}
	annotations := maps.Clone(opts.Annotations)
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[annotationArtifactKind] = opts.ArtifactType
	descriptor, err := oras.PackManifest(ctx, target, oras.PackManifestVersion1_1, opts.ArtifactType, oras.PackManifestOptions{
		Subject:             opts.Subject,
		Layers:              layers,
		ManifestAnnotations: annotations,
	})
	if err != nil {
		return PushResult{}, fmt.Errorf("pack OCI manifest: %w", err)
	}
	if err := verifyWriteOnceTags(ctx, target, descriptor, writeOnceTags); err != nil {
		return PushResult{}, err
	}
	return applyTags(ctx, target, descriptor, tags)
}

// PullFrom validates and reconstructs an artifact from target.
func PullFrom(ctx context.Context, target oras.ReadOnlyTarget, selector string, opts PullOptions) (Artifact, error) {
	if target == nil {
		return Artifact{}, fmt.Errorf("OCI target is required")
	}
	if selector == "" {
		selector = DefaultTag
	}
	opts.Limits = opts.Limits.normalized()
	if opts.LayerMediaType == "" {
		opts.LayerMediaType = FileMediaType
	}

	descriptor, manifestBytes, err := oras.FetchBytes(ctx, target, selector, oras.FetchBytesOptions{
		MaxBytes: opts.Limits.ManifestBytes,
	})
	if err != nil {
		return Artifact{}, fmt.Errorf("fetch OCI manifest: %w", err)
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return Artifact{}, fmt.Errorf("decode OCI manifest: %w", err)
	}
	if manifest.SchemaVersion != 2 || manifest.MediaType != ocispec.MediaTypeImageManifest {
		return Artifact{}, fmt.Errorf("%w: unsupported manifest media type %q", ErrUnexpectedArtifactType, manifest.MediaType)
	}
	if descriptor.MediaType != ocispec.MediaTypeImageManifest {
		return Artifact{}, fmt.Errorf("%w: descriptor media type %q", ErrUnexpectedArtifactType, descriptor.MediaType)
	}
	if !acceptedArtifactType(manifest.ArtifactType, opts.ExpectedArtifactTypes) {
		return Artifact{}, fmt.Errorf("%w: %q", ErrUnexpectedArtifactType, manifest.ArtifactType)
	}
	if manifest.Config.Digest != ocispec.DescriptorEmptyJSON.Digest ||
		manifest.Config.Size != ocispec.DescriptorEmptyJSON.Size ||
		manifest.Config.MediaType != ocispec.MediaTypeEmptyJSON {
		return Artifact{}, fmt.Errorf("%w: unsupported config descriptor", ErrUnexpectedArtifactType)
	}
	if kind := manifest.Annotations[annotationArtifactKind]; kind != "" && kind != manifest.ArtifactType {
		return Artifact{}, fmt.Errorf("%w: artifact annotation %q does not match %q", ErrUnexpectedArtifactType, kind, manifest.ArtifactType)
	}
	files, err := fetchFileLayers(ctx, target, manifest.Layers, opts.LayerMediaType, opts.Limits)
	if err != nil {
		return Artifact{}, err
	}
	snapshot, err := fsnapshot.TakeFiles(files)
	if err != nil {
		return Artifact{}, fmt.Errorf("build artifact filesystem: %w", err)
	}
	return Artifact{
		Descriptor:   descriptor,
		ArtifactType: manifest.ArtifactType,
		Annotations:  maps.Clone(manifest.Annotations),
		FileSystem:   snapshot,
	}, nil
}

func applyTags(
	ctx context.Context,
	target oras.Target,
	descriptor ocispec.Descriptor,
	tags []string,
) (PushResult, error) {
	result := PushResult{Descriptor: descriptor}
	for _, tag := range tags {
		if err := target.Tag(ctx, descriptor, tag); err != nil {
			return result, &PartialPushError{
				Descriptor:  descriptor,
				AppliedTags: slices.Clone(result.Tags),
				FailedTag:   tag,
				Err:         err,
			}
		}
		result.Tags = append(result.Tags, tag)
	}
	return result, nil
}

func pushFileLayers(
	ctx context.Context,
	target oras.Target,
	fsys fs.FS,
	mediaType string,
	limits Limits,
) ([]ocispec.Descriptor, error) {
	var layers []ocispec.Descriptor
	var total int64
	err := fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("%w: symbolic link %q", ErrUnsafeArtifactPath, name)
		}
		if err := validateArtifactPath(name, limits); err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("stat artifact file %q: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%w: non-regular file %q", ErrUnsafeArtifactPath, name)
		}
		if len(layers) >= limits.Files {
			return fmt.Errorf("%w: more than %d files", ErrArtifactLimit, limits.Files)
		}
		if info.Size() < 0 || info.Size() > limits.FileBytes {
			return fmt.Errorf("%w: file %q is %d bytes, maximum is %d", ErrArtifactLimit, name, info.Size(), limits.FileBytes)
		}
		contents, err := readArtifactFile(fsys, name, limits.FileBytes)
		if err != nil {
			return fmt.Errorf("read artifact file %q: %w", name, err)
		}
		size := int64(len(contents))
		if size > limits.FileBytes {
			return fmt.Errorf("%w: file %q is %d bytes, maximum is %d", ErrArtifactLimit, name, size, limits.FileBytes)
		}
		if size > limits.TotalBytes-total {
			return fmt.Errorf("%w: files exceed %d total bytes", ErrArtifactLimit, limits.TotalBytes)
		}
		total += size
		layer := content.NewDescriptorFromBytes(mediaType, contents)
		layer.Annotations = map[string]string{ocispec.AnnotationTitle: name}
		if err := target.Push(ctx, layer, bytes.NewReader(contents)); err != nil &&
			!errors.Is(err, errdef.ErrAlreadyExists) {
			return fmt.Errorf("push artifact file %q: %w", name, err)
		}
		layers = append(layers, layer)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("package artifact files: %w", err)
	}
	if len(layers) == 0 {
		return nil, fmt.Errorf("artifact must contain at least one file")
	}
	return layers, nil
}

func readArtifactFile(fsys fs.FS, name string, maxBytes int64) ([]byte, error) {
	file, err := fsys.Open(name)
	if err != nil {
		return nil, err
	}
	limit := maxBytes
	if limit < math.MaxInt64 {
		limit++
	}
	contents, readErr := io.ReadAll(io.LimitReader(file, limit))
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if int64(len(contents)) > maxBytes {
		return nil, fmt.Errorf("%w: file %q exceeds %d bytes while being read", ErrArtifactLimit, name, maxBytes)
	}
	return contents, nil
}

func fetchFileLayers(
	ctx context.Context,
	target oras.ReadOnlyTarget,
	layers []ocispec.Descriptor,
	mediaType string,
	limits Limits,
) (map[string][]byte, error) {
	if len(layers) > limits.Files {
		return nil, fmt.Errorf("%w: manifest contains %d files, maximum is %d", ErrArtifactLimit, len(layers), limits.Files)
	}
	files := make(map[string][]byte, len(layers))
	var total int64
	for _, layer := range layers {
		name := layer.Annotations[ocispec.AnnotationTitle]
		if err := validateArtifactPath(name, limits); err != nil {
			return nil, err
		}
		if _, exists := files[name]; exists {
			return nil, fmt.Errorf("%w: duplicate file %q", ErrUnsafeArtifactPath, name)
		}
		if layer.MediaType != mediaType {
			return nil, fmt.Errorf("%w: file %q has media type %q", ErrUnexpectedArtifactType, name, layer.MediaType)
		}
		if layer.Size < 0 || layer.Size > limits.FileBytes {
			return nil, fmt.Errorf("%w: file %q is %d bytes, maximum is %d", ErrArtifactLimit, name, layer.Size, limits.FileBytes)
		}
		if err := validateLayerDigest(layer); err != nil {
			return nil, fmt.Errorf("validate artifact file %q: %w", name, err)
		}
		if layer.Size > limits.TotalBytes-total {
			return nil, fmt.Errorf("%w: files exceed %d total bytes", ErrArtifactLimit, limits.TotalBytes)
		}
		total += layer.Size
		contents, err := content.FetchAll(ctx, target, layer)
		if err != nil {
			return nil, fmt.Errorf("fetch artifact file %q: %w", name, err)
		}
		files[name] = contents
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("artifact must contain at least one file")
	}
	return files, nil
}

func validateArtifactPath(name string, limits Limits) error {
	if name == "" || name == "." || !fs.ValidPath(name) || path.Clean(name) != name || strings.Contains(name, `\`) {
		return fmt.Errorf("%w: %q", ErrUnsafeArtifactPath, name)
	}
	if len(name) > limits.PathBytes {
		return fmt.Errorf("%w: path %q is %d bytes, maximum is %d", ErrArtifactLimit, name, len(name), limits.PathBytes)
	}
	if strings.Count(name, "/")+1 > limits.PathDepth {
		return fmt.Errorf("%w: path %q exceeds maximum depth %d", ErrArtifactLimit, name, limits.PathDepth)
	}
	return nil
}

func validateLayerDigest(layer ocispec.Descriptor) error {
	if err := layer.Digest.Validate(); err != nil {
		return fmt.Errorf("%w: invalid digest: %v", ErrUnexpectedArtifactType, err)
	}
	if layer.Digest.Algorithm() != digest.SHA256 {
		return fmt.Errorf("%w: unsupported digest algorithm %q", ErrUnexpectedArtifactType, layer.Digest.Algorithm())
	}
	return nil
}

func acceptedArtifactType(got string, expected []string) bool {
	return len(expected) == 0 || slices.Contains(expected, got)
}

func validatedTags(values []string) ([]string, error) {
	seen := make(map[string]struct{}, len(values))
	tags := make([]string, 0, len(values))
	for _, tag := range values {
		if _, exists := seen[tag]; exists {
			continue
		}
		ref := registry.Reference{Registry: "example.invalid", Repository: "ptah", Reference: tag}
		if err := ref.ValidateReferenceAsTag(); err != nil {
			return nil, fmt.Errorf("invalid OCI tag %q: %w", tag, err)
		}
		seen[tag] = struct{}{}
		tags = append(tags, tag)
	}
	return tags, nil
}

func moveTagLast(tags []string, tag string) []string {
	index := slices.Index(tags, tag)
	if index < 0 || index == len(tags)-1 {
		return tags
	}
	return append(slices.Delete(tags, index, index+1), tag)
}

func verifyWriteOnceTags(
	ctx context.Context,
	target oras.Target,
	descriptor ocispec.Descriptor,
	tags []string,
) error {
	for _, tag := range tags {
		existing, err := target.Resolve(ctx, tag)
		if errors.Is(err, errdef.ErrNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("resolve write-once OCI tag %q: %w", tag, err)
		}
		if existing.Digest != descriptor.Digest {
			return fmt.Errorf(
				"%w: %q resolves to %s instead of %s",
				ErrTagConflict,
				tag,
				existing.Digest,
				descriptor.Digest,
			)
		}
	}
	return nil
}
