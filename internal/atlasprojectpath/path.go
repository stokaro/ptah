// Package atlasprojectpath resolves local paths declared in atlas.hcl.
package atlasprojectpath

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/stokaro/ptah/internal/pathguard"
)

// LocalDir resolves a local Atlas migration directory URL relative to baseDir.
func LocalDir(rawURL, baseDir string) (string, error) {
	path, rawQuery, err := localPath(rawURL, "migration directories")
	if err != nil {
		return "", err
	}
	if rawQuery != "" {
		return "", fmt.Errorf("migration directory URL query parameters are not supported yet")
	}
	return resolvePath(path, baseDir)
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
	path, rawQuery, err := localPath(rawURL, "schema files")
	if err != nil {
		return "", err
	}
	if rawQuery != "" {
		return "", fmt.Errorf("schema file URL query parameters are not supported yet")
	}
	resolved, err := resolvePath(path, baseDir)
	if err != nil {
		return "", err
	}
	return "file://" + filepath.ToSlash(resolved), nil
}

func localPath(rawURL, resource string) (path string, rawQuery string, err error) {
	base, rawQuery, _ := strings.Cut(strings.TrimSpace(rawURL), "?")
	if base == "" {
		return "", "", fmt.Errorf("%s URL is required", resource)
	}
	if rawQuery != "" {
		if _, err := url.ParseQuery(rawQuery); err != nil {
			return "", "", fmt.Errorf("parse %s URL query: %w", resource, err)
		}
	}
	if strings.Contains(base, "://") && !strings.HasPrefix(base, "file://") {
		return "", "", fmt.Errorf("only local file:// %s are supported", resource)
	}
	path = strings.TrimPrefix(base, "file://")
	if path == "" {
		path = "."
	}
	path, err = url.PathUnescape(path)
	if err != nil {
		return "", "", fmt.Errorf("decode %s URL path: %w", resource, err)
	}
	return filepath.Clean(filepath.FromSlash(path)), rawQuery, nil
}

func resolvePath(path, baseDir string) (string, error) {
	if filepath.IsAbs(path) {
		return pathguard.ResolveWithinRoot(path, "")
	}
	if strings.TrimSpace(baseDir) == "" {
		return "", fmt.Errorf("atlas.hcl base directory is required")
	}
	return pathguard.ResolveWithinRoot(filepath.Join(baseDir, path), "")
}
