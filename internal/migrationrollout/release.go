package migrationrollout

import (
	"errors"
	"fmt"
	"io/fs"

	"ptah.run/internal/sourcecontent"
)

// ErrReleaseChanged is the refusal of a resumed run whose release is not the
// one the run was bound to.
var ErrReleaseChanged = errors.New("the migration release is not the one this rollout is bound to")

// ReleaseKind says where a release came from.
type ReleaseKind string

const (
	// ReleaseDirectory is a local migration directory, identified by the
	// content digest of the files a run reads from it.
	ReleaseDirectory ReleaseKind = "directory"
	// ReleaseOCI is an OCI artifact, identified by its manifest digest.
	ReleaseOCI ReleaseKind = "oci"
)

// Release is the immutable migration release a rollout is bound to.
type Release struct {
	// Kind says how Digest was computed.
	Kind ReleaseKind `json:"kind"`
	// Source is what the operator named: a directory path or an oci://
	// reference, which may be a tag.
	Source string `json:"source"`
	// Digest identifies the content. For a directory it is the content digest
	// of [sourcecontent], the one Ptah Operator binds a plan to; for an OCI
	// artifact it is the manifest digest.
	Digest string `json:"digest"`
	// Reference is the form each target run is handed: the digest-pinned
	// oci:// reference for an artifact, and empty for a directory, whose
	// captured copy lives only as long as one invocation.
	Reference string `json:"reference,omitempty"`
}

// DirectoryDigest is the content digest of a captured migration directory:
// every regular file in fsys, by path and bytes. The digest does not depend on
// where the directory lives or on file times.
func DirectoryDigest(fsys fs.FS) (string, error) {
	var files []sourcecontent.File
	err := fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		data, err := fs.ReadFile(fsys, name)
		if err != nil {
			return err
		}
		files = append(files, sourcecontent.File{Path: name, Data: data})
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("read migration release: %w", err)
	}
	digest, err := sourcecontent.Digest(files)
	if err != nil {
		return "", fmt.Errorf("identify migration release: %w", err)
	}
	return digest, nil
}

// Matches refuses a release that is not the one recorded. The kind and the
// digest decide; the source spelling does not, so a resumed run may name the
// same artifact by its digest where the first run named a tag.
func (r Release) Matches(recorded Release) error {
	if r.Kind != recorded.Kind {
		return fmt.Errorf("%w: the run is bound to %s %s (%s), and this invocation names %s %s",
			ErrReleaseChanged, recorded.Kind, recorded.Source, recorded.Digest, r.Kind, r.Source)
	}
	if r.Digest != recorded.Digest {
		return fmt.Errorf("%w: the run is bound to %s, and %s now holds %s; "+
			"start a new rollout with a new state file to roll out the new content",
			ErrReleaseChanged, recorded.Digest, r.Source, r.Digest)
	}
	return nil
}
