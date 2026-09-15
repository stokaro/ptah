package schemamodel

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

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

// ResolveManagedRows returns the rows md carries as the column maps a row
// comparison consumes, the shape [LoadManagedRows] returns for the same
// declaration read from its file.
//
// A declaration that carries its rows is the only form a published artifact
// has: the artifact travels without the working copy the annotation pointed
// into, so md.File and md.SourceDir there name no file this process can read.
// Callers that accept both forms resolve the carried rows through this function
// and read the file through LoadManagedRows, so the two answer alike about the
// same declaration.
//
// Each value resolves the way the YAML scalar that declared it resolves, which
// is what makes the two loaders agree: `007` under an int tag is the number 7
// and under a string tag is the three characters, and a timestamp is a
// time.Time either way. A nil md.Rows returns no rows and no error — nothing
// has read the declaration, and it is the file that holds the answer. A value
// whose text does not resolve under its own tag is an error naming the table,
// the row and the column.
func ResolveManagedRows(md ManagedData) ([]map[string]any, error) {
	if md.Rows == nil {
		return nil, nil
	}
	rows := make([]map[string]any, 0, len(md.Rows))
	for index, declared := range md.Rows {
		row := make(map[string]any, len(declared))
		for column, value := range declared {
			resolved, err := resolveManagedValue(value)
			if err != nil {
				return nil, fmt.Errorf(
					"managed data for table %q, row %d, column %q: %w",
					QualifyTableName(md.Schema, md.Table), index+1, column, err,
				)
			}
			row[column] = resolved
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// resolveManagedValue turns one declared scalar into the Go value the YAML
// resolver would have produced for it.
//
// A string keeps its text, because that is what quoting it said. Every other
// tag is resolved by handing the text back to the YAML resolver rather than by
// parsing it here: the resolver owns which texts are integers and which layouts
// are timestamps, and a second list of those rules would answer differently the
// first time the first one moved.
func resolveManagedValue(value ManagedValue) (any, error) {
	if value.Null {
		return nil, nil
	}
	switch value.Tag {
	case "", "str":
		return value.Text, nil
	case "int", "float", "bool", "timestamp":
		var resolved any
		if err := yaml.Unmarshal([]byte(value.Text), &resolved); err != nil {
			return nil, fmt.Errorf("%q is tagged %q and does not parse as one: %w", value.Text, value.Tag, err)
		}
		if got := managedValueTag(resolved); got != value.Tag {
			return nil, fmt.Errorf("%q is tagged %q and resolves as %q", value.Text, value.Tag, got)
		}
		return resolved, nil
	default:
		return nil, fmt.Errorf("carries the unsupported YAML tag %q", value.Tag)
	}
}

// managedValueTag names the tag a resolved value carries, so a declared tag can
// be held to what its own text resolves to. An unrecognized Go type answers the
// empty string, which matches no declared tag and is reported as a mismatch.
func managedValueTag(value any) string {
	switch value.(type) {
	case int, int64, uint64:
		return "int"
	case float64:
		return "float"
	case bool:
		return "bool"
	case time.Time:
		return "timestamp"
	case string:
		return "str"
	default:
		return ""
	}
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
