package migrationartifact

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"oras.land/oras-go/v2"

	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/internal/ociartifact"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/migration/migrator"
)

// DirectoryPushOptions configures a migration-directory registry push.
type DirectoryPushOptions struct {
	Reference string
	Directory string
	Tags      []string
	Version   string
	DirFormat migrator.MigrationDirFormat
	PlainHTTP bool
	VerifySum bool
	Now       func() time.Time
}

// DirectoryPushResult describes the immutable version written by a push.
type DirectoryPushResult struct {
	ociartifact.PushResult
	Directory string
	Version   string
}

// DirectoryPullOptions configures a migration-directory registry pull.
type DirectoryPullOptions struct {
	Reference string
	Output    string
	PlainHTTP bool
}

// DirectoryPullResult describes a reconstructed migration directory.
type DirectoryPullResult struct {
	Artifact Artifact
	Output   string
}

type preparedDirectory struct {
	FileSystem fs.FS
	Directory  string
	Version    string
	Tags       []string
}

// PushDirectory captures and stores a local migration directory.
func PushDirectory(ctx context.Context, opts DirectoryPushOptions) (DirectoryPushResult, error) {
	prepared, err := prepareDirectory(opts)
	if err != nil {
		return DirectoryPushResult{}, err
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.PlainHTTP})
	if err != nil {
		return DirectoryPushResult{}, err
	}
	result, err := Push(ctx, client, opts.Reference, prepared.FileSystem, PushOptions{
		Tags:          prepared.Tags,
		WriteOnceTags: []string{prepared.Version},
		DirFormat:     opts.DirFormat,
	})
	if err != nil {
		return DirectoryPushResult{}, err
	}
	return DirectoryPushResult{
		PushResult: result,
		Directory:  prepared.Directory,
		Version:    prepared.Version,
	}, nil
}

// PushDirectoryTo captures a local migration directory and stores it in target.
// It is used by the local-registry test harness and other internal transports.
func PushDirectoryTo(
	ctx context.Context,
	target oras.Target,
	opts DirectoryPushOptions,
) (DirectoryPushResult, error) {
	prepared, err := prepareDirectory(opts)
	if err != nil {
		return DirectoryPushResult{}, err
	}
	result, err := PushTo(ctx, target, prepared.FileSystem, PushOptions{
		Tags:          prepared.Tags,
		WriteOnceTags: []string{prepared.Version},
		DirFormat:     opts.DirFormat,
	})
	if err != nil {
		return DirectoryPushResult{}, err
	}
	return DirectoryPushResult{
		PushResult: result,
		Directory:  prepared.Directory,
		Version:    prepared.Version,
	}, nil
}

// PullDirectory retrieves and reconstructs a migration directory.
func PullDirectory(ctx context.Context, opts DirectoryPullOptions) (DirectoryPullResult, error) {
	if opts.Output == "" {
		return DirectoryPullResult{}, fmt.Errorf("migration artifact output directory is required")
	}
	output, err := pathguard.ResolveCLIPath(opts.Output)
	if err != nil {
		return DirectoryPullResult{}, fmt.Errorf("resolve migration artifact output: %w", err)
	}
	if err := validateAbsentOutput(output); err != nil {
		return DirectoryPullResult{}, err
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.PlainHTTP})
	if err != nil {
		return DirectoryPullResult{}, err
	}
	artifact, err := Pull(ctx, client, opts.Reference)
	if err != nil {
		return DirectoryPullResult{}, err
	}
	if err := artifact.WriteToDir(output); err != nil {
		return DirectoryPullResult{}, err
	}
	return DirectoryPullResult{Artifact: artifact, Output: output}, nil
}

func validateAbsentOutput(output string) error {
	_, err := os.Lstat(output)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect migration artifact output path: %w", err)
	}
	return fmt.Errorf("migration artifact output path already exists: %s", output)
}

func prepareDirectory(
	opts DirectoryPushOptions,
) (preparedDirectory, error) {
	if opts.Reference == "" {
		return preparedDirectory{}, fmt.Errorf("OCI reference is required")
	}
	if opts.Directory == "" {
		return preparedDirectory{}, fmt.Errorf("migrations directory is required")
	}
	directory, err := pathguard.ResolveCLIPath(opts.Directory)
	if err != nil {
		return preparedDirectory{}, fmt.Errorf("resolve migrations directory: %w", err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return preparedDirectory{}, fmt.Errorf("open migrations directory: %w", err)
	}
	defer root.Close()
	fsys, err := Capture(root.FS(), opts.DirFormat)
	if err != nil {
		return preparedDirectory{}, err
	}
	if opts.VerifySum {
		result, verifyErr := migratesum.VerifyWithFormat(fsys, opts.DirFormat)
		if verifyErr != nil {
			return preparedDirectory{}, fmt.Errorf("verify migrations directory: %w", verifyErr)
		}
		if !result.OK() {
			return preparedDirectory{}, fmt.Errorf("verify migrations directory:\n%s", result.Describe())
		}
	}
	version := opts.Version
	if version == "" {
		now := opts.Now
		if now == nil {
			now = time.Now
		}
		version = VersionTag(now())
	}
	return preparedDirectory{
		FileSystem: fsys,
		Directory:  directory,
		Version:    version,
		Tags:       append(append([]string{}, opts.Tags...), ociartifact.DefaultTag),
	}, nil
}
