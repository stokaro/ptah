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
// The path is resolved as filepath.Join(rootDir, md.SourceDir, md.File): rootDir
// is the root the schema was parsed from (the argument passed to ParseDir, or ""
// for ParseFile), md.SourceDir is the parse-root-relative directory of the Go
// source file that carried the annotation (recorded at parse time), and md.File
// is the value written verbatim in the annotation. Because ParseDir abstracts the
// OS root away via os.DirFS, SourceDir alone cannot be resolved — but a caller
// only needs the single parse root, and SourceDir disambiguates which
// subdirectory each entry came from. The file must be a top-level YAML list of
// mappings, one mapping per row:
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
	path := filepath.Join(rootDir, md.SourceDir, md.File)

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
