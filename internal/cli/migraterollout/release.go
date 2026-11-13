package migraterollout

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"ptah.run/internal/cli/internal/migrationsource"
	"ptah.run/internal/migrationartifact"
	"ptah.run/internal/migrationrollout"
	"ptah.run/internal/ociartifact"
	"ptah.run/migration/migrationfile"
)

// boundRelease is a release resolved once for one invocation: its identity,
// and the --migrations-dir value every target run of the invocation reads.
type boundRelease struct {
	release  migrationrollout.Release
	location string
	cleanup  func() error
}

// bindRelease resolves raw to an immutable release.
//
// An oci:// reference is pulled once to learn the manifest digest it names now,
// and every target run is handed the digest-pinned reference, so a tag moved
// during the rollout changes nothing it runs. A local directory is captured
// once, and every target run reads a private copy of that capture, so an edit
// to the directory during the rollout changes nothing it runs either. A
// resumed invocation binds again and [migrationrollout.State.Bind] refuses
// content other than the recorded.
func bindRelease(ctx context.Context, raw string, plainHTTP bool) (boundRelease, error) {
	if strings.HasPrefix(raw, ociartifact.Scheme) {
		return bindOCIRelease(ctx, raw, plainHTTP)
	}
	return bindDirectoryRelease(raw)
}

func bindOCIRelease(ctx context.Context, raw string, plainHTTP bool) (boundRelease, error) {
	source, err := migrationsource.Resolve(ctx, raw, migrationsource.Options{
		DirFormat: migrationfile.DirFormatAuto,
		PlainHTTP: plainHTTP,
	})
	if err != nil {
		return boundRelease{}, err
	}
	return boundRelease{
		release: migrationrollout.Release{
			Kind:      migrationrollout.ReleaseOCI,
			Source:    raw,
			Digest:    source.OCI.Descriptor.Digest.String(),
			Reference: source.OCI.DigestReference,
		},
		location: source.OCI.DigestReference,
		cleanup:  func() error { return nil },
	}, nil
}

func bindDirectoryRelease(raw string) (boundRelease, error) {
	local, err := migrationsource.CaptureLocal(raw, migrationsource.LocalOptions{})
	if err != nil {
		return boundRelease{}, err
	}
	digest, err := migrationrollout.DirectoryDigest(local.FileSystem)
	if err != nil {
		return boundRelease{}, err
	}
	staging, err := os.MkdirTemp("", "ptah-rollout-release-")
	if err != nil {
		return boundRelease{}, fmt.Errorf("stage the migration release: %w", err)
	}
	cleanup := func() error { return os.RemoveAll(staging) }
	location := filepath.Join(staging, "migrations")
	if err := (migrationartifact.Artifact{FileSystem: local.FileSystem}).WriteToDir(location); err != nil {
		_ = cleanup()
		return boundRelease{}, fmt.Errorf("stage the migration release: %w", err)
	}
	// The copy is what the targets read, so it is what the digest has to
	// describe: a write the copy did not reproduce would roll out content the
	// state file does not name.
	staged, err := migrationsource.CaptureLocal(location, migrationsource.LocalOptions{})
	if err == nil {
		var stagedDigest string
		stagedDigest, err = migrationrollout.DirectoryDigest(staged.FileSystem)
		if err == nil && stagedDigest != digest {
			err = fmt.Errorf("the staged copy holds %s where %s holds %s", stagedDigest, raw, digest)
		}
	}
	if err != nil {
		_ = cleanup()
		return boundRelease{}, fmt.Errorf("stage the migration release: %w", err)
	}
	return boundRelease{
		release: migrationrollout.Release{
			Kind:   migrationrollout.ReleaseDirectory,
			Source: raw,
			Digest: digest,
		},
		location: location,
		cleanup:  cleanup,
	}, nil
}
