// Package migrationsource resolves local and OCI-backed migration directories.
package migrationsource

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/stokaro/ptah/internal/migrationartifact"
	"github.com/stokaro/ptah/internal/migrationsnapshot"
	"github.com/stokaro/ptah/internal/ociartifact"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/migration/migrator"
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
	created bool
}

// FS returns the escape-resistant filesystem rooted at the opened directory.
func (d *LocalDirectory) FS() fs.FS {
	return d.opened.FS()
}

// Display returns the stable lexical path selected for the directory.
func (d *LocalDirectory) Display() string {
	return d.opened.Path()
}

// VerifyPath checks that Display still identifies the opened directory.
func (d *LocalDirectory) VerifyPath() error {
	return d.opened.VerifyPath()
}

// Created reports whether OpenOrCreateLocal observed the directory as missing
// before creating it.
func (d *LocalDirectory) Created() bool {
	return d.created
}

// Close releases the underlying directory handle.
func (d *LocalDirectory) Close() error {
	return d.opened.Close()
}

// DiscardIfCreatedEmpty closes the directory and removes it when this opener
// created it and it remains the same empty filesystem object. Nonempty
// directories are closed and preserved.
func (d *LocalDirectory) DiscardIfCreatedEmpty() error {
	if !d.created {
		return d.Close()
	}
	entries, err := fs.ReadDir(d.FS(), ".")
	if err != nil {
		return errors.Join(err, d.Close())
	}
	if len(entries) != 0 {
		return d.Close()
	}
	if err := d.VerifyPath(); err != nil {
		return errors.Join(err, d.Close())
	}
	path := d.Display()
	if err := d.Close(); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove empty migrations directory: %w", err)
	}
	return nil
}

type localRootContextKey struct{}

type localRootContextValue struct {
	raw     string
	options LocalOptions
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

// Resolve loads raw as a local directory or an oci:// artifact.
func Resolve(ctx context.Context, raw string, opts Options) (Source, error) {
	if strings.HasPrefix(raw, ociartifact.Scheme) {
		return resolveOCI(ctx, raw, opts)
	}
	return resolveLocal(raw, opts, localOptionsFromContext(ctx, raw))
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

// OpenOrCreateLocal creates a missing local directory through its configured
// rooted boundary and returns an owned handle.
func OpenOrCreateLocal(
	raw string,
	opts LocalOptions,
	perm fs.FileMode,
) (*LocalDirectory, error) {
	existing, err := OpenLocal(raw, opts)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	var opened *pathguard.OpenedDirectory
	switch {
	case opts.Root != nil:
		opened, err = opts.Root.OpenOrCreateDirectory(raw, perm)
	case opts.AllowedRoot == "":
		opened, err = pathguard.OpenOrCreateCLIDirectory(raw, perm)
	default:
		opened, err = pathguard.OpenOrCreateDirectoryWithinRoot(raw, opts.AllowedRoot, perm)
	}
	if err != nil {
		if errors.Is(err, pathguard.ErrOutsideRoot) {
			return nil, fmt.Errorf("invalid migrations directory: %w", err)
		}
		return nil, fmt.Errorf("open migrations directory: %w", err)
	}
	return &LocalDirectory{opened: opened, created: true}, nil
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
			Client:     client,
			Reference:  ref.String(),
			Descriptor: artifact.Descriptor,
		},
	}, nil
}
