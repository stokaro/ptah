// Package migrationsource resolves local and OCI-backed migration directories.
package migrationsource

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
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
}

// LocalSource is one immutable local migration-directory snapshot.
type LocalSource struct {
	FileSystem fs.FS
	Display    string
}

type localRootContextKey struct{}

type localRootContextValue struct {
	raw         string
	allowedRoot string
}

// WithLocalRoot returns a context that preserves the allowed-root provenance
// for one forwarded local migration path.
func WithLocalRoot(ctx context.Context, raw, allowedRoot string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, localRootContextKey{}, localRootContextValue{
		raw:         raw,
		allowedRoot: allowedRoot,
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
	return LocalOptions{AllowedRoot: value.allowedRoot}
}

// CaptureLocal validates and opens raw through its allowed root, then captures
// one stable immutable snapshot before returning.
func CaptureLocal(raw string, opts LocalOptions) (LocalSource, error) {
	var opened *pathguard.OpenedDirectory
	var err error
	if opts.AllowedRoot == "" {
		opened, err = pathguard.OpenCLIDirectory(raw)
	} else {
		opened, err = pathguard.OpenDirectoryWithinRoot(raw, opts.AllowedRoot)
	}
	if err != nil {
		if errors.Is(err, pathguard.ErrOutsideRoot) {
			return LocalSource{}, fmt.Errorf("invalid migrations directory: %w", err)
		}
		return LocalSource{}, fmt.Errorf("open migrations directory: %w", err)
	}
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
	return LocalSource{FileSystem: snapshot, Display: opened.Path()}, nil
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
