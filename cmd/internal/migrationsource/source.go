// Package migrationsource resolves local and OCI-backed migration directories.
package migrationsource

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"go.5x5.cz/ptah/internal/migrationartifact"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/migration/migrator"
)

// Options controls migration source resolution.
type Options struct {
	DirFormat migrator.MigrationDirFormat
	PlainHTTP bool
}

// OCI identifies the immutable registry artifact behind a source.
type OCI struct {
	Client     *ociartifact.Client
	Reference  string
	Descriptor ocispec.Descriptor
	// Tag is the registry tag the operator named, or the empty string when the
	// reference named none. A tag is a movable pointer, so it says nothing
	// about which bytes it will select on the next pull.
	Tag string
	// PinnedByDigest reports whether the reference selected exact content by
	// digest. This is the provenance bit callers need to tell an artifact whose
	// bytes the operator chose from one whoever last moved Tag chose for them.
	PinnedByDigest bool
	// DigestReference is the canonical oci:// form pinning Descriptor's exact
	// bytes, so a caller can print the reference that removes the tag's say.
	DigestReference string
}

// Source is an immutable migration filesystem and its display metadata.
type Source struct {
	FileSystem fs.FS
	Display    string
	DirFormat  migrator.MigrationDirFormat
	OCI        *OCI
}

// LocalOptions controls rooted local migration-directory capture.
type LocalOptions struct {
	// AllowedRoot constrains raw even when raw is absolute. Leave empty for CLI
	// semantics: relative paths are constrained to the working directory and
	// explicit absolute paths remain allowed.
	AllowedRoot string
	// Root, when set, opens raw through an already-bound directory handle.
	// The caller retains ownership of Root.
	Root *pathguard.OpenedDirectory
}

// LocalSource is one immutable local migration-directory snapshot.
type LocalSource struct {
	FileSystem fs.FS
	Display    string
}

// LocalDirectory is one rooted local directory handle. The caller must close
// it after finishing any path-bound preparation.
type LocalDirectory struct {
	opened  *pathguard.OpenedDirectory
	fsys    fs.FS
	display string
}

// FS returns the escape-resistant filesystem rooted at the opened directory.
func (d *LocalDirectory) FS() fs.FS {
	if d.fsys != nil {
		return d.fsys
	}
	return d.opened.FS()
}

// Display returns the stable lexical path selected for the directory.
func (d *LocalDirectory) Display() string {
	if d.display != "" {
		return d.display
	}
	return d.opened.Path()
}

// Close releases the underlying directory handle.
func (d *LocalDirectory) Close() error {
	if d == nil || d.opened == nil {
		return nil
	}
	return d.opened.Close()
}

type localRootContextKey struct{}

type virtualContextKey struct{}

type localRootContextValue struct {
	raw     string
	options LocalOptions
}

type virtualContextSource struct {
	fsys    fs.FS
	display string
}

type virtualContextValue struct {
	sources map[string]virtualContextSource
}

// WithLocalRoot returns a context that preserves the allowed-root provenance
// for one forwarded local migration path.
func WithLocalRoot(ctx context.Context, raw, allowedRoot string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, localRootContextKey{}, localRootContextValue{
		raw:     raw,
		options: LocalOptions{AllowedRoot: allowedRoot},
	})
}

// WithRootedLocal returns a context that preserves a borrowed rooted handle
// for one forwarded local migration path.
func WithRootedLocal(
	ctx context.Context,
	raw string,
	root *pathguard.OpenedDirectory,
) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, localRootContextKey{}, localRootContextValue{
		raw:     raw,
		options: LocalOptions{Root: root},
	})
}

// WithVirtual returns a context that binds raw to one immutable in-memory
// migration directory produced during project-config evaluation.
func WithVirtual(ctx context.Context, raw string, fsys fs.FS, display string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	sources := make(map[string]virtualContextSource)
	if current, ok := ctx.Value(virtualContextKey{}).(virtualContextValue); ok {
		maps.Copy(sources, current.sources)
	}
	sources[raw] = virtualContextSource{fsys: fsys, display: display}
	return context.WithValue(ctx, virtualContextKey{}, virtualContextValue{
		sources: sources,
	})
}

// Resolve loads raw as a local directory or an oci:// artifact.
func Resolve(ctx context.Context, raw string, opts Options) (Source, error) {
	if virtual, ok := virtualFromContext(ctx, raw); ok {
		return resolveVirtual(virtual, opts)
	}
	if strings.HasPrefix(raw, ociartifact.Scheme) {
		return resolveOCI(ctx, raw, opts)
	}
	return resolveLocal(raw, opts, localOptionsFromContext(ctx, raw))
}

func resolveVirtual(virtual virtualContextSource, opts Options) (Source, error) {
	local, err := CaptureVirtual(virtual.fsys, virtual.display)
	if err != nil {
		return Source{}, err
	}
	format, err := migrator.ParseMigrationDirFormat(string(opts.DirFormat))
	if err != nil {
		return Source{}, err
	}
	return Source{
		FileSystem: local.FileSystem,
		Display:    local.Display,
		DirFormat:  format,
	}, nil
}

func virtualFromContext(ctx context.Context, raw string) (virtualContextSource, bool) {
	if ctx == nil {
		return virtualContextSource{}, false
	}
	value, ok := ctx.Value(virtualContextKey{}).(virtualContextValue)
	if !ok {
		return virtualContextSource{}, false
	}
	source, ok := value.sources[raw]
	return source, ok && source.fsys != nil
}

func resolveLocal(raw string, opts Options, localOpts LocalOptions) (Source, error) {
	local, err := CaptureLocal(raw, localOpts)
	if err != nil {
		return Source{}, err
	}
	format, err := migrator.ParseMigrationDirFormat(string(opts.DirFormat))
	if err != nil {
		return Source{}, err
	}
	return Source{
		FileSystem: local.FileSystem,
		Display:    local.Display,
		DirFormat:  format,
	}, nil
}

func localOptionsFromContext(ctx context.Context, raw string) LocalOptions {
	if ctx == nil {
		return LocalOptions{}
	}
	value, ok := ctx.Value(localRootContextKey{}).(localRootContextValue)
	if !ok || value.raw != raw {
		return LocalOptions{}
	}
	return value.options
}

// CaptureLocal validates and opens raw through its allowed root, then captures
// one stable immutable snapshot before returning.
func CaptureLocal(raw string, opts LocalOptions) (LocalSource, error) {
	opened, err := OpenLocal(raw, opts)
	if err != nil {
		return LocalSource{}, err
	}
	return captureAndCloseLocal(opened)
}

// CaptureVirtual captures one immutable snapshot from an in-memory project
// data source.
func CaptureVirtual(fsys fs.FS, display string) (LocalSource, error) {
	if fsys == nil {
		return LocalSource{}, fmt.Errorf("open migrations directory: in-memory filesystem is unavailable")
	}
	snapshot, err := migrationsnapshot.CaptureStable(fsys)
	if err != nil {
		return LocalSource{}, fmt.Errorf("capture migrations directory: %w", err)
	}
	return LocalSource{FileSystem: snapshot, Display: display}, nil
}

// OpenVirtual returns a non-owning directory view over an immutable in-memory
// project data source.
func OpenVirtual(fsys fs.FS, display string) *LocalDirectory {
	return &LocalDirectory{fsys: fsys, display: display}
}

// OpenLocal opens raw through its configured rooted boundary without capturing
// it. The caller owns the returned handle.
func OpenLocal(raw string, opts LocalOptions) (*LocalDirectory, error) {
	var opened *pathguard.OpenedDirectory
	var err error
	switch {
	case opts.Root != nil:
		opened, err = opts.Root.OpenDirectory(raw)
	case opts.AllowedRoot == "":
		opened, err = pathguard.OpenCLIDirectory(raw)
	default:
		opened, err = pathguard.OpenDirectoryWithinRoot(raw, opts.AllowedRoot)
	}
	if err != nil {
		if errors.Is(err, pathguard.ErrOutsideRoot) {
			return nil, fmt.Errorf("invalid migrations directory: %w", err)
		}
		return nil, fmt.Errorf("open migrations directory: %w", err)
	}
	return &LocalDirectory{opened: opened}, nil
}

func captureAndCloseLocal(opened *LocalDirectory) (LocalSource, error) {
	snapshot, captureErr := migrationsnapshot.CaptureStable(opened.FS())
	closeErr := opened.Close()
	if captureErr != nil {
		if closeErr != nil {
			captureErr = errors.Join(captureErr, fmt.Errorf("close migrations directory: %w", closeErr))
		}
		return LocalSource{}, fmt.Errorf("capture migrations directory: %w", captureErr)
	}
	if closeErr != nil {
		return LocalSource{}, fmt.Errorf("close migrations directory: %w", closeErr)
	}
	return LocalSource{FileSystem: snapshot, Display: opened.Display()}, nil
}

func resolveOCI(ctx context.Context, raw string, opts Options) (Source, error) {
	ref, err := ociartifact.ParseRef(raw)
	if err != nil {
		return Source{}, err
	}
	requestedFormat, err := migrator.ParseMigrationDirFormat(string(opts.DirFormat))
	if err != nil {
		return Source{}, err
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.PlainHTTP})
	if err != nil {
		return Source{}, err
	}
	artifact, err := migrationartifact.Pull(ctx, client, ref.String())
	if err != nil {
		return Source{}, fmt.Errorf("resolve migrations artifact: %w", err)
	}
	if requestedFormat != migrator.MigrationDirFormatAuto && requestedFormat != artifact.DirFormat {
		return Source{}, fmt.Errorf(
			"migration artifact format %q does not match requested format %q",
			artifact.DirFormat,
			requestedFormat,
		)
	}
	return Source{
		FileSystem: artifact.FileSystem,
		Display:    ref.String(),
		DirFormat:  artifact.DirFormat,
		OCI: &OCI{
			Client:          client,
			Reference:       ref.String(),
			Descriptor:      artifact.Descriptor,
			Tag:             ref.Tag(),
			PinnedByDigest:  ref.IsDigest(),
			DigestReference: ref.PinnedString(artifact.Descriptor.Digest.String()),
		},
	}, nil
}
