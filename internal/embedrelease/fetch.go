package embedrelease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"

	"ptah.run/internal/ociartifact"
)

// ErrIncompleteRelease reports a release artifact that is missing a part a
// promotion needs.
var ErrIncompleteRelease = errors.New("the release does not carry what it takes to run")

// Fetched is a release read back out of a registry or an image layout.
type Fetched struct {
	// Release is what the change proposed.
	Release Release
	// Specification is the document it was built from, exactly as published.
	//
	// Unparsed here on purpose: this package models evidence, and the grammar
	// of a specification belongs to the package that defines it. Checking these
	// bytes against Release.SpecDigest is [ptah.run/internal/embedspec.ParsePublished].
	Specification []byte
	// Reference is what the operator asked for, and Digest what it resolved to.
	//
	// Both, because they differ exactly when it matters. A mutable tag is what
	// a person types and what a pipeline configures; the digest is what ran,
	// and a record of a promotion that kept only the tag says the environments
	// agreed without establishing that they did.
	Reference string
	Digest    string
}

// FetchOptions are how to reach the release.
type FetchOptions struct {
	// PlainHTTP permits an unencrypted connection to the registry.
	PlainHTTP bool
}

// Fetch reads a release artifact and everything it carries.
//
// An oci-layout:// directory is accepted alongside a registry reference, and
// the two resolve the same way deliberately: an air-gapped environment
// promotes a release by carrying a directory across the gap, and giving that
// case its own code path would give it a second set of checks to trust.
//
// What comes back is not yet a specification. The bytes are handed on for the
// package that defines the format to parse and to check against the digest this
// release recorded, which is the one place that can compare them without
// restating how a document is addressed.
func Fetch(ctx context.Context, reference string, opts FetchOptions) (Fetched, error) {
	artifact, err := pullRelease(ctx, reference, opts)
	if err != nil {
		return Fetched{}, err
	}

	recordBody, err := fs.ReadFile(artifact.FileSystem, ReleaseFileName)
	if err != nil {
		return Fetched{}, fmt.Errorf("%w: %s holds no %s",
			ErrIncompleteRelease, reference, ReleaseFileName)
	}
	var release Release
	if err := json.Unmarshal(recordBody, &release); err != nil {
		return Fetched{}, fmt.Errorf("decode %s from %s: %w", ReleaseFileName, reference, err)
	}
	if release.Version != RecordVersion {
		// Refused rather than read on a best-effort basis. A record whose
		// schema this build does not know is one whose fields it would be
		// guessing at, and the guess lands in an approval chain.
		return Fetched{}, fmt.Errorf(
			"%s was written as a version %d release and this build reads version %d",
			reference, release.Version, RecordVersion)
	}

	specification, err := fs.ReadFile(artifact.FileSystem, SpecificationFileName)
	if err != nil {
		// The likely reader of this is somebody promoting a release published
		// by a build that carried none, so the sentence says what to do rather
		// than only what is absent.
		return Fetched{}, fmt.Errorf(
			"%w: %s holds no %s, so there is nothing here to run; publish the release again from the specification",
			ErrIncompleteRelease, reference, SpecificationFileName)
	}

	return Fetched{
		Release: release, Specification: specification,
		Reference: reference, Digest: artifact.Descriptor.Digest.String(),
	}, nil
}

// pullRelease reads the release at whichever kind of address the reference is.
//
// It used to open an image layout itself, because [ociartifact.Client.Pull]
// could only address a registry. That made this function the second place that
// had to recognize a layout reference, and the two were not equivalent: a
// registry pull went through the client's limits and options and a layout pull
// did not. The client resolves both kinds now, so there is one recognition
// again (stokaro/ptah#2623).
func pullRelease(
	ctx context.Context, reference string, opts FetchOptions,
) (ociartifact.Artifact, error) {
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: opts.PlainHTTP})
	if err != nil {
		return ociartifact.Artifact{}, fmt.Errorf("fetch the release %s: %w", reference, err)
	}
	artifact, err := client.Pull(ctx, reference, ociartifact.PullOptions{
		ExpectedArtifactTypes: []string{ReleaseArtifactType},
	})
	if err != nil {
		return ociartifact.Artifact{}, fmt.Errorf("fetch the release %s: %w", reference, err)
	}
	return artifact, nil
}
