package atlashclfmt

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclwrite"
)

// FormatPaths formats HCL schema files under every provided path and returns
// the files whose content changed.
func FormatPaths(paths []string) ([]string, error) {
	changed := make([]string, 0)
	for _, path := range paths {
		pathChanged, err := FormatPath(path)
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
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("schema fmt %s: %w", path, err)
	}
	if !info.IsDir() {
		changed, err := FormatFile(path)
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
		fileChanged, err := FormatFile(file)
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
	if filepath.Ext(path) != ".hcl" {
		return false, nil
	}

	info, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("schema fmt %s: %w", path, err)
	}
	if info.IsDir() {
		return false, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return false, fmt.Errorf("schema fmt %s: %w", path, err)
	}
	if _, diags := hclwrite.ParseConfig(data, path, hcl.Pos{Line: 1, Column: 1}); diags.HasErrors() {
		return false, fmt.Errorf("schema fmt %s: %s", path, diags.Error())
	}

	formatted := hclwrite.Format(data)
	if bytes.Equal(data, formatted) {
		return false, nil
	}
	//nolint:gosec // schema fmt intentionally rewrites the user-selected local HCL file after stat/read validation.
	if err := os.WriteFile(path, formatted, info.Mode().Perm()); err != nil {
		return false, fmt.Errorf("schema fmt %s: %w", path, err)
	}
	return true, nil
}
