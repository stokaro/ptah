package migrationartifact

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"oras.land/oras-go/v2"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/migration/migrator"
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
	// Latest moves the latest alias onto this push. It is opt-in because a
	// publish and an alias move are two operations, and doing both by default
	// makes every publish a promotion nobody asked for.
	Latest bool
	// GeneratedVersion writes a timestamped version tag when Version names
	// none. It is opt-in for the same reason, and because a tag that exists
	// only because a default created it is one nothing refers to.
	GeneratedVersion bool
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
		WriteOnceTags: prepared.writeOnceTags(),
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
		WriteOnceTags: prepared.writeOnceTags(),
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
	if version == "" && opts.GeneratedVersion {
		now := opts.Now
		if now == nil {
			now = time.Now
		}
		version = VersionTag(now())
	}
	tags := append(make([]string, 0), opts.Tags...)
	if opts.Latest {
		tags = append(tags, ociartifact.DefaultTag)
	}
	// The tag the reference named is the tag that was asked for, and it
	// belongs on the artifact whichever path published it. Client.Push adds it
	// too and the duplicate collapses; a push straight to a target has no
	// other way to learn it, and without this it would write an artifact
	// nothing but a digest can address.
	if ref, err := ociartifact.ParseRef(opts.Reference); err == nil && !ref.IsDigest() {
		tags = append(tags, ref.Selector())
	}
	return preparedDirectory{
		FileSystem: fsys,
		Directory:  directory,
		Version:    version,
		Tags:       tags,
	}, nil
}

// writeOnceTags is the version tag when there is one. A push that generated no
// version has nothing to protect from being moved, and passing an empty string
// would make the write-once check refuse a tag nobody named.
func (p preparedDirectory) writeOnceTags() []string {
	if p.Version == "" {
		return nil
	}
	return []string{p.Version}
}
