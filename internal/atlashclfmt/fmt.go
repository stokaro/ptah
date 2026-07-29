// Package atlashclfmt formats Atlas HCL schema files with canonical hclwrite
// formatting. It backs the "schema fmt" commands of both the ptah and
// ptah-compat binaries, in rewrite (FormatPaths) and check-only (CheckPaths)
// modes.
package atlashclfmt

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclwrite"
)

// fileFunc processes one file and reports whether its content changed (or,
// for the check variant, would change).
type fileFunc func(path string) (bool, error)

// FormatPaths formats HCL schema files under every provided path and returns
// the files whose content changed.
func FormatPaths(paths []string) ([]string, error) {
	return processPaths(paths, FormatFile)
}

// CheckPaths reports the HCL schema files under every provided path whose
// content is not in canonical format, without rewriting anything. It is the
// no-write CI companion of [FormatPaths].
func CheckPaths(paths []string) ([]string, error) {
	return processPaths(paths, checkFile)
}

func processPaths(paths []string, process fileFunc) ([]string, error) {
	changed := make([]string, 0)
	for _, path := range paths {
		pathChanged, err := processPath(path, process)
		if err != nil {
			return nil, err
		}
		changed = append(changed, pathChanged...)
	}
	return changed, nil
}

// FormatPath formats one file or recursively formats every .hcl file under a
// directory.
func FormatPath(path string) ([]string, error) {
	return processPath(path, FormatFile)
}

func processPath(path string, process fileFunc) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("schema fmt %s: %w", path, err)
	}
	if !info.IsDir() {
		changed, err := process(path)
		if err != nil || !changed {
			return nil, err
		}
		return []string{path}, nil
	}

	changed := make([]string, 0)
	err = filepath.WalkDir(path, func(file string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(file) != ".hcl" {
			return nil
		}
		fileChanged, err := process(file)
		if err != nil {
			return err
		}
		if fileChanged {
			changed = append(changed, file)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("schema fmt %s: %w", path, err)
	}
	return changed, nil
}

// FormatFile formats a single .hcl file. Non-HCL files are ignored.
func FormatFile(path string) (bool, error) {
	canonical, err := canonicalizeFile(path)
	if err != nil || !canonical.changed {
		return false, err
	}
	if err := os.WriteFile(path, canonical.formatted, canonical.info.Mode().Perm()); err != nil {
		return false, fmt.Errorf("schema fmt %s: %w", path, err)
	}
	return true, nil
}

// checkFile reports whether a single .hcl file would change under formatting,
// without rewriting it.
func checkFile(path string) (bool, error) {
	canonical, err := canonicalizeFile(path)
	if err != nil {
		return false, err
	}
	return canonical.changed, nil
}

// canonicalFile is the in-memory canonical form of one .hcl file.
type canonicalFile struct {
	info      os.FileInfo
	formatted []byte
	changed   bool
}

// canonicalizeFile reads and canonically formats one .hcl file in memory,
// reporting whether the content differs. Non-HCL paths and directories report
// no change.
func canonicalizeFile(path string) (canonicalFile, error) {
	if filepath.Ext(path) != ".hcl" {
		return canonicalFile{}, nil
	}

	info, err := os.Stat(path)
	if err != nil {
		return canonicalFile{}, fmt.Errorf("schema fmt %s: %w", path, err)
	}
	if info.IsDir() {
		return canonicalFile{}, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return canonicalFile{}, fmt.Errorf("schema fmt %s: %w", path, err)
	}
	if _, diags := hclwrite.ParseConfig(data, path, hcl.Pos{Line: 1, Column: 1}); diags.HasErrors() {
		return canonicalFile{}, fmt.Errorf("schema fmt %s: %s", path, diags.Error())
	}

	formatted := hclwrite.Format(data)
	return canonicalFile{
		info:      info,
		formatted: formatted,
		changed:   !bytes.Equal(data, formatted),
	}, nil
}
