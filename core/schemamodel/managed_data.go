package schemamodel

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.yaml.in/yaml/v3"
)

const (
	// nullTag and stringTag are the resolved YAML tags this loader tests
	// against. yaml.Node reports a resolved tag for every scalar, so a plain
	// `null` and a quoted "null" differ here and nowhere else.
	nullTag   = "!!null"
	stringTag = "!!str"
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

// ManagedValue is one declared cell, kept as the YAML scalar that declared it
// rather than as the Go value that scalar resolves to.
//
// Resolution is lossy for the values a reference table is made of. Measured
// with go.yaml.in/yaml/v3: `007` resolves to int 7, `1.0` to float64 1, and
// `2020-01-01` to a time.Time. A published artifact has to carry what the
// author wrote, so Tag keeps the resolved YAML tag and Text the scalar's exact
// source text. The pair separates `007` from "007", which resolve to different
// SQL literals in the same column.
type ManagedValue struct {
	// Tag is the resolved YAML tag without its "!!" prefix: str, int, float,
	// bool, timestamp, or null.
	Tag string
	// Text is the scalar's source text. It is empty for a null.
	Text string
	// Null records a value the declaration spelled out as null, which is not
	// the same as a column the row never names.
	Null bool
}

// ManagedRow is one declared row: column name to declared value.
//
// A column absent from the map was not declared. A column mapped to a
// [ManagedValue] with Null set was declared null. The two are different
// statements about a row, and nothing downstream may collapse them.
type ManagedRow map[string]ManagedValue

// LoadManagedRowValues reads the YAML row-data file referenced by md and
// returns its rows with every value preserved as the scalar that declared it.
//
// Paths resolve exactly as [LoadManagedRows] resolves them. The two loaders
// read the same file and differ in what they keep: this one preserves the
// declaration, so it is what publication and planning read, while
// LoadManagedRows resolves to Go values for callers that compare against a
// live row set.
//
// An empty, null, or whitespace-only file yields no rows and no error. A
// nested structure, an anchor alias, a non-scalar column name and a repeated
// column are refused by name: each of them would publish a row whose meaning
// depends on who read it.
func LoadManagedRowValues(rootDir string, md ManagedData) ([]ManagedRow, error) {
	sourceDir := md.SourceDir
	if !filepath.IsAbs(sourceDir) {
		sourceDir = filepath.Join(rootDir, sourceDir)
	}
	path := filepath.Join(sourceDir, md.File)

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read managed data file %q for table %q: %w", path, md.Table, err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(content, &document); err != nil {
		return nil, fmt.Errorf("parse managed data file %q for table %q: %w", path, md.Table, err)
	}
	if document.Kind == 0 || len(document.Content) == 0 {
		return nil, nil
	}
	root := document.Content[0]
	if root.Kind == yaml.ScalarNode && root.Tag == nullTag {
		return nil, nil
	}
	if root.Kind != yaml.SequenceNode {
		return nil, fmt.Errorf(
			"managed data file %q for table %q must be a YAML list of mappings",
			path, md.Table,
		)
	}
	rows := make([]ManagedRow, 0, len(root.Content))
	for index, item := range root.Content {
		row, err := managedRowFromNode(item)
		if err != nil {
			return nil, fmt.Errorf(
				"managed data file %q for table %q, row %d: %w",
				path, md.Table, index+1, err,
			)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func managedRowFromNode(item *yaml.Node) (ManagedRow, error) {
	if item.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("is not a mapping")
	}
	row := make(ManagedRow, len(item.Content)/2)
	for index := 0; index+1 < len(item.Content); index += 2 {
		name, value := item.Content[index], item.Content[index+1]
		if name.Kind != yaml.ScalarNode || name.Tag != stringTag {
			return nil, fmt.Errorf("has a column name that is not a plain string")
		}
		if _, exists := row[name.Value]; exists {
			return nil, fmt.Errorf("names column %q twice", name.Value)
		}
		if value.Kind == yaml.AliasNode {
			return nil, fmt.Errorf("column %q is a YAML alias", name.Value)
		}
		if value.Kind != yaml.ScalarNode {
			return nil, fmt.Errorf("column %q is not a scalar", name.Value)
		}
		if value.Tag == nullTag {
			row[name.Value] = ManagedValue{Tag: "null", Null: true}
			continue
		}
		row[name.Value] = ManagedValue{Tag: strings.TrimPrefix(value.Tag, "!!"), Text: value.Value}
	}
	return row, nil
}
