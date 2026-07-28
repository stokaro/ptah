// Package fileplan applies planned multi-file outputs to the local
// filesystem. Producers (for example the Atlas schema inspect split/write
// template pipeline) stay pure and only describe files as root-relative
// path/content pairs; this package is the single place that touches the
// filesystem, so path safety is enforced once:
//
//   - every path must stay inside its output root after cleaning and
//     resolving existing symlinks (no traversal, no absolute paths),
//   - duplicate output paths fail explicitly,
//   - a planned file that collides with another planned file's directory
//     fails explicitly,
//   - a destination that already exists as a directory (or other non-regular
//     file) fails explicitly instead of being clobbered.
//
// Apply validates the whole plan before writing anything, so a rejected plan
// leaves the filesystem untouched apart from pre-existing state.
package fileplan

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/stokaro/ptah/internal/pathguard"
)

// File is one planned output file. Path is slash-separated and relative to
// Root; Root is the output directory exactly as the operator supplied it
// (relative roots resolve against the working directory).
type File struct {
	Root string
	Path string
	Data string
}

// resolvedFile pairs a planned file with its validated absolute destination.
type resolvedFile struct {
	file File
	abs  string
}

// Apply validates every planned file and then writes all of them. Validation
// failures abort before the first write.
func Apply(files []File) error {
	resolved, err := resolve(files)
	if err != nil {
		return err
	}
	for _, entry := range resolved {
		if err := os.MkdirAll(filepath.Dir(entry.abs), 0o755); err != nil {
			return fmt.Errorf("create output directory for %q: %w", entry.file.Path, err)
		}
		if err := os.WriteFile(entry.abs, []byte(entry.file.Data), 0o600); err != nil {
			return fmt.Errorf("write %q: %w", entry.file.Path, err)
		}
	}
	return nil
}

func resolve(files []File) ([]resolvedFile, error) {
	resolved := make([]resolvedFile, 0, len(files))
	seen := make(map[string]File, len(files))
	roots := make(map[string]string)
	for _, file := range files {
		root, err := resolveRoot(roots, file.Root)
		if err != nil {
			return nil, err
		}
		abs, err := resolveFilePath(root, file.Path)
		if err != nil {
			return nil, err
		}
		if previous, ok := seen[abs]; ok {
			return nil, fmt.Errorf("duplicate output path %q in output directory %q (also produced as %q)",
				file.Path, file.Root, previous.Path)
		}
		seen[abs] = file
		resolved = append(resolved, resolvedFile{file: file, abs: abs})
	}
	if err := checkDirectoryCollisions(resolved, seen); err != nil {
		return nil, err
	}
	for _, entry := range resolved {
		if err := checkOverwriteHazard(entry); err != nil {
			return nil, err
		}
	}
	return resolved, nil
}

func resolveRoot(cache map[string]string, root string) (string, error) {
	if strings.TrimSpace(root) == "" {
		return "", fmt.Errorf("output directory must not be empty")
	}
	if resolved, ok := cache[root]; ok {
		return resolved, nil
	}
	resolved, err := pathguard.ResolveCLIPath(root)
	if err != nil {
		return "", fmt.Errorf("resolve output directory: %w", err)
	}
	cache[root] = resolved
	return resolved, nil
}

// resolveFilePath validates one root-relative output path lexically, then
// resolves it inside the root with symlink awareness.
func resolveFilePath(root, relative string) (string, error) {
	if strings.TrimSpace(relative) == "" {
		return "", fmt.Errorf("output path must not be empty")
	}
	if strings.ContainsRune(relative, '\\') {
		return "", fmt.Errorf("unsafe output path %q", relative)
	}
	clean := path.Clean(relative)
	if clean == "." || clean == ".." || path.IsAbs(clean) || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("unsafe output path %q", relative)
	}
	abs, err := pathguard.ResolveWithinRoot(filepath.Join(root, filepath.FromSlash(clean)), root)
	if err != nil {
		return "", fmt.Errorf("unsafe output path %q: %w", relative, err)
	}
	return abs, nil
}

// checkDirectoryCollisions rejects plans where one planned file's path is a
// directory of another planned file (for example "tables" and
// "tables/users.sql"): the writer could only satisfy one of them.
func checkDirectoryCollisions(resolved []resolvedFile, seen map[string]File) error {
	for _, entry := range resolved {
		for dir := filepath.Dir(entry.abs); ; dir = filepath.Dir(dir) {
			if parent, ok := seen[dir]; ok {
				return fmt.Errorf("output path %q needs directory %q, which is also a planned output file",
					entry.file.Path, parent.Path)
			}
			next := filepath.Dir(dir)
			if next == dir {
				break
			}
		}
	}
	return nil
}

// checkOverwriteHazard refuses destinations that already exist as something
// other than a regular file. Overwriting a regular file is allowed: repeated
// exports of the same schema are expected to regenerate their output.
func checkOverwriteHazard(entry resolvedFile) error {
	info, err := os.Lstat(entry.abs)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("inspect output path %q: %w", entry.file.Path, err)
	}
	if info.IsDir() {
		return fmt.Errorf("output path %q already exists as a directory", entry.file.Path)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("output path %q already exists and is not a regular file", entry.file.Path)
	}
	return nil
}
