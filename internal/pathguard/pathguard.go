// Package pathguard validates user-supplied filesystem paths, resolving them
// against an allowed root and rejecting escapes so CLI flags cannot traverse
// outside the intended directory.
package pathguard

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// OpenedDirectory is a directory handle anchored to the filesystem object
// opened after path validation. Renaming or replacing the pathname does not
// retarget its filesystem view.
type OpenedDirectory struct {
	root *os.Root
	path string
}

// Path returns the absolute path resolved during validation.
func (d *OpenedDirectory) Path() string {
	return d.path
}

// FS returns an escape-resistant filesystem rooted at the opened directory.
func (d *OpenedDirectory) FS() fs.FS {
	return d.root.FS()
}

// Close releases the underlying directory handle.
func (d *OpenedDirectory) Close() error {
	return d.root.Close()
}

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

// OpenCLIDirectory validates and opens a CLI directory path in one operation.
// Relative paths are opened through the current working directory as their
// allowed root. Explicit absolute paths preserve their unbounded CLI behavior.
func OpenCLIDirectory(path string) (*OpenedDirectory, error) {
	if filepath.IsAbs(path) {
		return openUnboundedDirectory(path)
	}
	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("resolve working directory: %w", err)
	}
	return OpenDirectoryWithinRoot(path, cwd)
}

// OpenDirectoryWithinRoot validates and opens path through an already opened
// allowed-root handle. A pathname swap after validation therefore cannot
// redirect the opened directory outside allowedRoot.
func OpenDirectoryWithinRoot(path, allowedRoot string) (*OpenedDirectory, error) {
	if strings.TrimSpace(allowedRoot) == "" {
		return nil, fmt.Errorf("allowed root is required")
	}
	resolvedRoot, err := resolvePath(allowedRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve allowed root: %w", err)
	}
	root, err := os.OpenRoot(resolvedRoot)
	if err != nil {
		return nil, fmt.Errorf("open allowed root: %w", err)
	}

	candidate := path
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(resolvedRoot, candidate)
	}
	resolved, resolveErr := resolvePath(candidate)
	if resolveErr == nil && !isSubpath(resolvedRoot, resolved) {
		resolveErr = fmt.Errorf("%q is outside allowed root %q", resolved, resolvedRoot)
	}
	if resolveErr != nil {
		closeErr := root.Close()
		if closeErr != nil {
			return nil, errors.Join(resolveErr, fmt.Errorf("close allowed root: %w", closeErr))
		}
		return nil, resolveErr
	}

	relative, err := filepath.Rel(resolvedRoot, resolved)
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("resolve path relative to allowed root: %w", err)
	}
	opened, openErr := root.OpenRoot(relative)
	closeErr := root.Close()
	if openErr != nil {
		if closeErr != nil {
			return nil, errors.Join(openErr, fmt.Errorf("close allowed root: %w", closeErr))
		}
		return nil, openErr
	}
	if closeErr != nil {
		openedCloseErr := opened.Close()
		return nil, errors.Join(
			fmt.Errorf("close allowed root: %w", closeErr),
			openedCloseErr,
		)
	}
	return &OpenedDirectory{root: opened, path: resolved}, nil
}

func openUnboundedDirectory(path string) (*OpenedDirectory, error) {
	resolved, err := ResolveWithinRoot(path, "")
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(resolved)
	if err != nil {
		return nil, err
	}
	return &OpenedDirectory{root: root, path: resolved}, nil
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
