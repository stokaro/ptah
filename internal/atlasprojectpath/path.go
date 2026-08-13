// Package atlasprojectpath resolves local paths declared in atlas.hcl.
package atlasprojectpath

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"go.5x5.cz/ptah/internal/pathguard"
)

// LocalDir resolves a local Atlas migration directory URL inside baseDir.
func LocalDir(rawURL, baseDir string) (string, error) {
	path, query, err := LocalDirWithQuery(rawURL, baseDir)
	if err != nil {
		return "", err
	}
	if len(query) > 0 {
		return "", fmt.Errorf("migration directory URL query parameters are not supported yet")
	}
	return path, nil
}

// LocalDirWithQuery resolves a local Atlas migration directory URL inside
// baseDir and preserves query parameters for command-specific validation.
// Absolute paths must also stay inside baseDir. Query semantics apply only to
// file:// URLs, not plain filesystem paths.
func LocalDirWithQuery(rawURL, baseDir string) (string, url.Values, error) {
	path, query, err := localPath(rawURL, "migration directories")
	if err != nil {
		return "", nil, err
	}
	resolved, err := resolveProjectPath(path, baseDir)
	if err != nil {
		return "", nil, err
	}
	return resolved, query, nil
}

// SchemaFileURLs resolves local Atlas schema file URLs relative to baseDir.
func SchemaFileURLs(rawURLs []string, baseDir string) ([]string, error) {
	resolved := make([]string, 0, len(rawURLs))
	for _, rawURL := range rawURLs {
		value, err := SchemaFileURL(rawURL, baseDir)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, value)
	}
	return resolved, nil
}

// SchemaFileURL resolves one local Atlas schema file URL relative to baseDir.
func SchemaFileURL(rawURL, baseDir string) (string, error) {
	path, query, err := localPath(rawURL, "schema files")
	if err != nil {
		return "", err
	}
	if len(query) > 0 {
		return "", fmt.Errorf("schema file URL query parameters are not supported yet")
	}
	resolved, err := resolveUnrootedPath(path, baseDir)
	if err != nil {
		return "", err
	}
	return "file://" + filepath.ToSlash(resolved), nil
}

func localPath(rawURL, resource string) (path string, query url.Values, err error) {
	value := strings.TrimSpace(rawURL)
	if value == "" {
		return "", nil, fmt.Errorf("%s URL is required", resource)
	}
	if !strings.HasPrefix(value, "file://") {
		if strings.Contains(value, "://") {
			return "", nil, fmt.Errorf("only local file:// %s are supported", resource)
		}
		return filepath.Clean(filepath.FromSlash(value)), nil, nil
	}
	base, rawQuery, _ := strings.Cut(value, "?")
	if rawQuery != "" {
		query, err = url.ParseQuery(rawQuery)
		if err != nil {
			return "", nil, fmt.Errorf("parse %s URL query: %w", resource, err)
		}
	}
	path = strings.TrimPrefix(base, "file://")
	if path == "" {
		path = "."
	}
	path, err = url.PathUnescape(path)
	if err != nil {
		return "", nil, fmt.Errorf("decode %s URL path: %w", resource, err)
	}
	return filepath.Clean(filepath.FromSlash(path)), query, nil
}

func resolveProjectPath(path, baseDir string) (string, error) {
	if strings.TrimSpace(baseDir) == "" {
		return "", fmt.Errorf("atlas.hcl base directory is required")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(baseDir, path)
	}
	return pathguard.ResolveWithinRoot(path, baseDir)
}

func resolveUnrootedPath(path, baseDir string) (string, error) {
	if filepath.IsAbs(path) {
		return pathguard.ResolveWithinRoot(path, "")
	}
	if strings.TrimSpace(baseDir) == "" {
		return "", fmt.Errorf("atlas.hcl base directory is required")
	}
	return pathguard.ResolveWithinRoot(filepath.Join(baseDir, path), "")
}
