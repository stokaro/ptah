// Package migrationsource resolves local and OCI-backed migration directories.
package migrationsource

import (
	"context"
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

// Resolve loads raw as a local directory or an oci:// artifact.
func Resolve(ctx context.Context, raw string, opts Options) (Source, error) {
	if strings.HasPrefix(raw, ociartifact.Scheme) {
		return resolveOCI(ctx, raw, opts)
	}
	return resolveLocal(raw, opts)
}

func resolveLocal(raw string, opts Options) (Source, error) {
	resolved, err := pathguard.ResolveCLIPath(raw)
	if err != nil {
		return Source{}, fmt.Errorf("invalid migrations directory: %w", err)
	}
	root, err := os.OpenRoot(resolved)
	if err != nil {
		return Source{}, fmt.Errorf("open migrations directory: %w", err)
	}
	defer root.Close()
	snapshot, err := migrationsnapshot.Capture(root.FS())
	if err != nil {
		return Source{}, fmt.Errorf("capture migrations directory: %w", err)
	}
	format, err := migrator.ParseMigrationDirFormat(string(opts.DirFormat))
	if err != nil {
		return Source{}, err
	}
	return Source{
		FileSystem: snapshot,
		Display:    resolved,
		DirFormat:  format,
	}, nil
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
