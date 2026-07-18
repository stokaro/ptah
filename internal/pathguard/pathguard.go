package pathguard

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ResolveWithinRoot returns an absolute, cleaned path and, when allowedRoot is
// set, verifies that the path stays inside that root after resolving existing
// symlinks. Missing final path components are allowed so callers can validate
// directories before creating them.
func ResolveWithinRoot(path, allowedRoot string) (string, error) {
	resolved, err := resolvePath(path)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(allowedRoot) == "" {
		return resolved, nil
	}

	root, err := resolvePath(allowedRoot)
	if err != nil {
		return "", fmt.Errorf("resolve allowed root: %w", err)
	}
	if !isSubpath(root, resolved) {
		return "", fmt.Errorf("%q is outside allowed root %q", resolved, root)
	}
	return resolved, nil
}

// ResolveCLIPath applies a conservative boundary to relative CLI paths while
// preserving historical support for explicit absolute paths.
func ResolveCLIPath(path string) (string, error) {
	if filepath.IsAbs(path) {
		return ResolveWithinRoot(path, "")
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve working directory: %w", err)
	}
	return ResolveWithinRoot(path, cwd)
}

func resolvePath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("path is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve absolute path: %w", err)
	}
	return resolveExistingPrefix(filepath.Clean(abs))
}

func resolveExistingPrefix(path string) (string, error) {
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return filepath.Clean(resolved), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	var suffix []string
	for current := path; ; current = filepath.Dir(current) {
		parent := filepath.Dir(current)
		if parent == current {
			return filepath.Clean(filepath.Join(append([]string{current}, suffix...)...)), nil
		}

		suffix = append([]string{filepath.Base(current)}, suffix...)
		resolved, err := filepath.EvalSymlinks(parent)
		if err == nil {
			parts := append([]string{filepath.Clean(resolved)}, suffix...)
			return filepath.Clean(filepath.Join(parts...)), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
	}
}

func isSubpath(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (!strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != ".." && !filepath.IsAbs(rel))
}
