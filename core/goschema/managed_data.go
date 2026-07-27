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
// The file path is resolved as filepath.Join(baseDir, md.File): md.File is the
// value written verbatim in the //migrator:schema:data annotation and is
// interpreted relative to baseDir, which callers set to the directory of the Go
// source file that carried the annotation. The file must be a top-level YAML
// list of mappings, one mapping per row:
//
//   - code: US
//     name: United States
//   - code: CZ
//     name: Czechia
//
// Each mapping becomes one map[string]any whose keys are the column names. This
// makes the declarative data model self-contained and testable; the later
// data-diff phase consumes the returned rows to compute row-level changes.
//
// A missing or unreadable file, or malformed YAML, is returned as a wrapped
// error that names the resolved path and target table.
func LoadManagedRows(baseDir string, md ManagedData) ([]map[string]any, error) {
	path := filepath.Join(baseDir, md.File)

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
