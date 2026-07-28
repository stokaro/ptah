package goschema

import (
	"fmt"
	"os"
	"path/filepath"

	"go.yaml.in/yaml/v3"
)

// LoadManagedRows reads the YAML row-data file referenced by md and returns its
// rows as an ordered slice of column maps.
//
// ParseDir and ParseDirRaw record an absolute md.SourceDir, preserving the
// originating root when several Go roots are merged. ParseFS and ParseSource
// cannot provide a host-filesystem root, so their relative SourceDir is resolved
// against rootDir. md.File is always resolved relative to that source directory.
// The file must be a top-level YAML list of mappings, one mapping per row:
//
//   - code: US
//     name: United States
//   - code: CZ
//     name: Czechia
//
// Each mapping becomes one map[string]any whose keys are the column names. This
// makes the declarative data model testable; the later data-diff phase consumes
// the returned rows to compute row-level changes.
//
// An empty, null, or whitespace-only file yields no rows and no error. A missing
// or unreadable file, or malformed YAML, is returned as a wrapped error that
// names the resolved path and target table.
func LoadManagedRows(rootDir string, md ManagedData) ([]map[string]any, error) {
	sourceDir := md.SourceDir
	if !filepath.IsAbs(sourceDir) {
		sourceDir = filepath.Join(rootDir, sourceDir)
	}
	path := filepath.Join(sourceDir, md.File)

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read managed data file %q for table %q: %w", path, md.Table, err)
	}

	var rows []map[string]any
	if err := yaml.Unmarshal(content, &rows); err != nil {
		return nil, fmt.Errorf("parse managed data file %q for table %q: %w", path, md.Table, err)
	}

	return rows, nil
}
