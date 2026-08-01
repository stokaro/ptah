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

var (
	// ErrOutsideRoot reports that a path escapes its configured filesystem root.
	ErrOutsideRoot = errors.New("outside allowed root")
	// ErrDirectoryChanged reports that an opened directory's lexical path no
	// longer identifies the filesystem object selected when it was opened.
	ErrDirectoryChanged = errors.New("opened directory path changed")
)

// OpenedDirectory is a directory handle anchored to the filesystem object
// selected by one rooted open. Renaming or replacing the pathname does not
// retarget its filesystem view.
type OpenedDirectory struct {
	root *os.Root
	path string
	info fs.FileInfo
}

type rootedPath struct {
	relative string
	display  string
}

// Path returns the stable absolute lexical path selected for the directory.
// It is intentionally not canonicalized after opening because the pathname
// could then describe a different filesystem object than the rooted handle.
func (d *OpenedDirectory) Path() string {
	return d.path
}

// FS returns an escape-resistant filesystem rooted at the opened directory.
func (d *OpenedDirectory) FS() fs.FS {
	return d.root.FS()
}

// OpenDirectory opens path through this directory handle. The returned handle
// remains valid after the receiver is closed.
func (d *OpenedDirectory) OpenDirectory(path string) (*OpenedDirectory, error) {
	target, err := rootedRelativePath(path, d.path)
	if err != nil {
		return nil, err
	}
	opened, err := d.root.OpenRoot(target.relative)
	if err != nil {
		_, containmentErr := ResolveWithinRoot(target.display, d.path)
		if errors.Is(containmentErr, ErrOutsideRoot) {
			return nil, containmentErr
		}
		return nil, err
	}
	return newOpenedDirectory(opened, target.display)
}

// Stat returns information about name through the rooted handle.
func (d *OpenedDirectory) Stat(name string) (fs.FileInfo, error) {
	return d.root.Stat(name)
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
		return "", outsideRootError(resolved, root)
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
		return OpenDirectory(path)
	}
	root, err := OpenCurrentDirectory()
	if err != nil {
		return nil, err
	}
	return openDirectoryAndCloseRoot(root, path)
}

// OpenCurrentDirectory anchors the process working directory before resolving
// its pathname. The caller owns the returned handle.
func OpenCurrentDirectory() (*OpenedDirectory, error) {
	root, err := os.OpenRoot(".")
	if err != nil {
		return nil, fmt.Errorf("open working directory: %w", err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		closeErr := root.Close()
		return nil, errors.Join(
			fmt.Errorf("resolve working directory display path: %w", err),
			closeErr,
		)
	}
	return newOpenedDirectory(root, filepath.Clean(cwd))
}

// OpenDirectoryWithinRoot binds allowedRoot first, then opens path only through
// that handle. Pathname replacement cannot redirect either rooted open.
func OpenDirectoryWithinRoot(path, allowedRoot string) (*OpenedDirectory, error) {
	if strings.TrimSpace(allowedRoot) == "" {
		return nil, fmt.Errorf("allowed root is required")
	}
	root, err := OpenDirectory(allowedRoot)
	if err != nil {
		return nil, fmt.Errorf("open allowed root: %w", err)
	}
	return openDirectoryAndCloseRoot(root, path)
}

func openDirectoryAndCloseRoot(
	root *OpenedDirectory,
	path string,
) (*OpenedDirectory, error) {
	opened, openErr := root.OpenDirectory(path)
	return closeParentAfterOpen(root, opened, openErr)
}

func closeParentAfterOpen(
	root, opened *OpenedDirectory,
	openErr error,
) (*OpenedDirectory, error) {
	closeErr := root.Close()
	if openErr != nil {
		if closeErr != nil {
			return nil, errors.Join(openErr, fmt.Errorf("close directory root: %w", closeErr))
		}
		return nil, openErr
	}
	if closeErr != nil {
		openedCloseErr := opened.Close()
		return nil, errors.Join(
			fmt.Errorf("close directory root: %w", closeErr),
			openedCloseErr,
		)
	}
	return opened, nil
}

// OpenDirectory opens path as a rooted directory handle without imposing an
// enclosing path boundary. The caller owns the returned handle.
func OpenDirectory(path string) (*OpenedDirectory, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("path is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve absolute path: %w", err)
	}
	absolute = filepath.Clean(absolute)
	root, err := os.OpenRoot(absolute)
	if err != nil {
		return nil, err
	}
	return newOpenedDirectory(root, absolute)
}

func newOpenedDirectory(root *os.Root, path string) (*OpenedDirectory, error) {
	info, err := root.Stat(".")
	if err != nil {
		return nil, errors.Join(err, root.Close())
	}
	if !info.IsDir() {
		return nil, errors.Join(
			fmt.Errorf("opened path is not a directory: %s", path),
			root.Close(),
		)
	}
	return &OpenedDirectory{root: root, path: path, info: info}, nil
}

func rootedRelativePath(path, root string) (rootedPath, error) {
	if strings.TrimSpace(path) == "" {
		return rootedPath{}, fmt.Errorf("path is required")
	}
	if !filepath.IsAbs(path) {
		relative := filepath.Clean(path)
		absolute := filepath.Clean(filepath.Join(root, relative))
		if !isSubpath(root, absolute) {
			return rootedPath{}, outsideRootError(absolute, root)
		}
		return rootedPath{relative: relative, display: absolute}, nil
	}

	absolute := filepath.Clean(path)
	if isSubpath(root, absolute) {
		relative, err := filepath.Rel(root, absolute)
		if err != nil {
			return rootedPath{}, fmt.Errorf("resolve path relative to allowed root: %w", err)
		}
		return rootedPath{relative: relative, display: absolute}, nil
	}

	resolvedRoot, err := resolvePath(root)
	if err != nil {
		return rootedPath{}, fmt.Errorf("resolve allowed root display path: %w", err)
	}
	resolved, err := resolvePath(absolute)
	if err != nil {
		return rootedPath{}, err
	}
	if !isSubpath(resolvedRoot, resolved) {
		return rootedPath{}, outsideRootError(resolved, resolvedRoot)
	}
	relative, err := filepath.Rel(resolvedRoot, resolved)
	if err != nil {
		return rootedPath{}, fmt.Errorf("resolve path relative to allowed root: %w", err)
	}
	return rootedPath{relative: relative, display: absolute}, nil
}

func outsideRootError(path, root string) error {
	return fmt.Errorf("%q is %w %q", path, ErrOutsideRoot, root)
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
