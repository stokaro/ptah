package ociartifact

import (
	"fmt"
	"path/filepath"
	"strings"

	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content/oci"
)

// LayoutScheme addresses an OCI image layout on disk rather than a registry.
//
// It is what an air-gapped environment has instead of a network: an artifact is
// copied into a directory on one side of the gap, carried across, and copied
// out on the other. The same directory serves as a release bundle, a registry
// backup, and a deterministic fixture for a test that must not depend on a
// server being up.
const LayoutScheme = "oci-layout://"

// IsLayoutRef reports whether a reference addresses a local image layout.
func IsLayoutRef(raw string) bool {
	return strings.HasPrefix(strings.TrimSpace(raw), LayoutScheme)
}

// LayoutPath returns the directory a layout reference names, and the tag it
// selects within that layout.
//
// The tag is optional and is separated by a colon, the way a registry reference
// separates one. A Windows drive letter is not a tag, so only a colon after the
// last path separator is read as one.
func LayoutPath(raw string) (path, tag string, err error) {
	trimmed := strings.TrimSpace(raw)
	if !IsLayoutRef(trimmed) {
		return "", "", fmt.Errorf("%q is not an %s reference", raw, LayoutScheme)
	}
	rest := strings.TrimPrefix(trimmed, LayoutScheme)
	if rest == "" {
		return "", "", fmt.Errorf("%s reference names no directory", LayoutScheme)
	}
	base := filepath.Base(rest)
	if index := strings.LastIndex(base, ":"); index > 0 {
		tag = base[index+1:]
		rest = rest[:len(rest)-len(base)+index]
	}
	if tag == "" {
		tag = DefaultTag
	}
	return rest, tag, nil
}

// OpenLayout opens or creates the image layout a reference names.
//
// Creating it is deliberate: the export half of an air-gap workflow is the
// first thing anyone runs, and refusing to create the directory would make the
// operator do by hand what the command is about to do anyway.
func OpenLayout(raw string) (oras.Target, string, error) {
	path, tag, err := LayoutPath(raw)
	if err != nil {
		return nil, "", err
	}
	store, err := oci.New(path)
	if err != nil {
		return nil, "", fmt.Errorf("open the OCI image layout %s: %w", path, err)
	}
	return store, tag, nil
}
