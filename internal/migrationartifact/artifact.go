// Package migrationartifact maps Ptah migration directories to OCI artifacts.
package migrationartifact

import (
	"context"
	"fmt"
	"io/fs"
	"maps"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"

	"github.com/stokaro/ptah/internal/migrationsnapshot"
	"github.com/stokaro/ptah/internal/ociartifact"
	"github.com/stokaro/ptah/migration/migrator"
)

const (
	// LayerMediaType identifies files contained in a migration artifact.
	LayerMediaType = "application/vnd.stokaro.ptah.migration.file.v1"

	annotationDirFormat = "io.stokaro.ptah.migration-format"
)

// PushOptions controls migration artifact metadata and tags.
type PushOptions struct {
	Tags          []string
	WriteOnceTags []string
	DirFormat     migrator.MigrationDirFormat
	Annotations   map[string]string
}

// Artifact is a validated migration directory retrieved from OCI storage.
type Artifact struct {
	FileSystem fs.FS
	Descriptor ocispec.Descriptor
	Reference  ociartifact.Reference
	DirFormat  migrator.MigrationDirFormat
}

type preparedArtifact struct {
	FileSystem  fs.FS
	DirFormat   migrator.MigrationDirFormat
	Annotations map[string]string
}

// WriteToDir reconstructs the migration directory under dir.
func (a Artifact) WriteToDir(dir string) error {
	return (ociartifact.Artifact{FileSystem: a.FileSystem}).WriteToDir(dir)
}

// VersionTag returns the conventional write-once version tag for timestamp.
func VersionTag(timestamp time.Time) string {
	return ociartifact.VersionTag(timestamp)
}

// Capture returns the immutable migration inputs accepted by Ptah.
func Capture(fsys fs.FS, format migrator.MigrationDirFormat) (fs.FS, error) {
	normalized, err := migrator.ParseMigrationDirFormat(string(format))
	if err != nil {
		return nil, err
	}
	if err := validateEntries(fsys); err != nil {
		return nil, err
	}
	snapshot, err := migrationsnapshot.Capture(fsys)
	if err != nil {
		return nil, fmt.Errorf("capture migration artifact: %w", err)
	}
	files, err := migrator.DiscoverMigrationFiles(snapshot, normalized)
	if err != nil {
		return nil, fmt.Errorf("validate migration artifact: %w", err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("migration artifact contains no migration files")
	}
	return snapshot, nil
}

func validateEntries(fsys fs.FS) error {
	return fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("%w: symbolic link %q", ociartifact.ErrUnsafeArtifactPath, name)
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("stat migration artifact input %q: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%w: non-regular file %q", ociartifact.ErrUnsafeArtifactPath, name)
		}
		return nil
	})
}

// Push stores a migration directory through client.
func Push(
	ctx context.Context,
	client *ociartifact.Client,
	ref string,
	fsys fs.FS,
	opts PushOptions,
) (ociartifact.PushResult, error) {
	if client == nil {
		return ociartifact.PushResult{}, fmt.Errorf("OCI client is required")
	}
	prepared, err := prepare(fsys, opts)
	if err != nil {
		return ociartifact.PushResult{}, err
	}
	return client.Push(ctx, ref, prepared.FileSystem, ociartifact.PushOptions{
		ArtifactType:   ociartifact.MigrationArtifactType,
		LayerMediaType: LayerMediaType,
		Tags:           opts.Tags,
		WriteOnceTags:  opts.WriteOnceTags,
		Annotations:    prepared.Annotations,
	})
}

// PushTo stores a migration directory in target.
func PushTo(
	ctx context.Context,
	target oras.Target,
	fsys fs.FS,
	opts PushOptions,
) (ociartifact.PushResult, error) {
	prepared, err := prepare(fsys, opts)
	if err != nil {
		return ociartifact.PushResult{}, err
	}
	return ociartifact.PushTo(ctx, target, prepared.FileSystem, ociartifact.PushOptions{
		ArtifactType:   ociartifact.MigrationArtifactType,
		LayerMediaType: LayerMediaType,
		Tags:           opts.Tags,
		WriteOnceTags:  opts.WriteOnceTags,
		Annotations:    prepared.Annotations,
	})
}

// Pull retrieves and validates a migration directory through client.
func Pull(ctx context.Context, client *ociartifact.Client, ref string) (Artifact, error) {
	if client == nil {
		return Artifact{}, fmt.Errorf("OCI client is required")
	}
	pulled, err := client.Pull(ctx, ref, ociartifact.PullOptions{
		ExpectedArtifactTypes: []string{ociartifact.MigrationArtifactType},
		LayerMediaType:        LayerMediaType,
	})
	if err != nil {
		return Artifact{}, err
	}
	return validatePulled(pulled)
}

// PullFrom retrieves and validates a migration directory from target.
func PullFrom(ctx context.Context, target oras.ReadOnlyTarget, selector string) (Artifact, error) {
	pulled, err := ociartifact.PullFrom(ctx, target, selector, ociartifact.PullOptions{
		ExpectedArtifactTypes: []string{ociartifact.MigrationArtifactType},
		LayerMediaType:        LayerMediaType,
	})
	if err != nil {
		return Artifact{}, err
	}
	return validatePulled(pulled)
}

func prepare(
	fsys fs.FS,
	opts PushOptions,
) (preparedArtifact, error) {
	format, err := migrator.ParseMigrationDirFormat(string(opts.DirFormat))
	if err != nil {
		return preparedArtifact{}, err
	}
	snapshot, err := Capture(fsys, format)
	if err != nil {
		return preparedArtifact{}, err
	}
	annotations := maps.Clone(opts.Annotations)
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[annotationDirFormat] = string(format)
	return preparedArtifact{
		FileSystem:  snapshot,
		DirFormat:   format,
		Annotations: annotations,
	}, nil
}

func validatePulled(pulled ociartifact.Artifact) (Artifact, error) {
	format, err := migrator.ParseMigrationDirFormat(pulled.Annotations[annotationDirFormat])
	if err != nil {
		return Artifact{}, fmt.Errorf("invalid migration artifact format: %w", err)
	}
	snapshot, err := Capture(pulled.FileSystem, format)
	if err != nil {
		return Artifact{}, err
	}
	return Artifact{
		FileSystem: snapshot,
		Descriptor: pulled.Descriptor,
		Reference:  pulled.Reference,
		DirFormat:  format,
	}, nil
}
