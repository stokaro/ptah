package dbtest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	yaml "go.yaml.in/yaml/v3"
)

// caseFile is the top-level YAML document shape: a single cases: list.
type caseFile struct {
	Cases []Case `yaml:"cases"`
}

// ParseCases parses YAML containing a top-level cases: list and validates the
// result. Every ---separated document in the input is decoded and its cases are
// concatenated, so a multi-document file contributes all of its cases rather
// than silently only the first. Unknown fields are rejected so typos in step or
// assertion keys surface as errors rather than being silently ignored. Empty
// input yields no cases and no error.
func ParseCases(data []byte) ([]Case, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)

	var cases []Case
	for {
		var file caseFile
		err := dec.Decode(&file)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse test cases: %w", err)
		}
		cases = append(cases, file.Cases...)
	}
	if err := validateCases(cases); err != nil {
		return nil, err
	}
	return cases, nil
}

// LoadCases reads every *.yaml and *.yml file in dir (non-recursively), parses
// each as a [ParseCases] document, and returns the concatenated cases. Files are
// processed in lexical order by name so runs are deterministic.
func LoadCases(dir string) ([]Case, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read test-case directory %s: %w", dir, err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !isCaseFile(entry.Name()) {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)

	var cases []Case
	var origins []string
	for _, name := range names {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read test-case file %s: %w", path, err)
		}
		parsed, err := ParseCases(data)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		cases = append(cases, parsed...)
		for range parsed {
			origins = append(origins, name)
		}
	}
	// Deliberately not wrapped with the per-file prefix above: the union error
	// already names both colliding files, and a prefix would read as
	// `b.yaml: duplicate test case "dup" in a.yaml and b.yaml`.
	if err := validateUniqueCaseNames(cases, origins); err != nil {
		return nil, err
	}
	return cases, nil
}

func isCaseFile(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".yaml", ".yml":
		return true
	default:
		return false
	}
}

// LoadCasesOfKind reads the test cases of one kind from dir, accepting both the
// native YAML documents [LoadCases] reads and Atlas-format `*.test.hcl` files.
//
// The kind is explicit rather than inferred because Atlas labels each case with
// it and the two are not interchangeable: a `test "migrate"` case drives the
// migration directory to a version, which a schema test run must not do. Native
// YAML cases carry no kind and are returned for either.
func LoadCasesOfKind(dir string, kind AtlasTestKind) ([]Case, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read test-case directory %s: %w", dir, err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !isCaseFile(entry.Name()) && !isAtlasCaseFile(entry.Name()) {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)

	var cases []Case
	var origins []string
	for _, name := range names {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read test-case file %s: %w", path, err)
		}
		var parsed []Case
		if isAtlasCaseFile(name) {
			parsed, err = ParseAtlasTestCases(data, name, kind)
		} else {
			parsed, err = ParseCases(data)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		cases = append(cases, parsed...)
		for range parsed {
			origins = append(origins, name)
		}
	}
	// Checked over the post-filter set, which makes directory validity depend on
	// kind. One directory holding `a.yaml: dup` and `b.test.hcl: test "migrate"
	// "dup"` loads clean for schema and is rejected for migrate:
	//
	//	LoadCasesOfKind(dir, AtlasTestKindSchema)  -> err=<nil>, 1 case
	//	LoadCasesOfKind(dir, AtlasTestKindMigrate) -> duplicate test case "dup" in a.yaml and b.test.hcl
	//
	// That asymmetry is the point rather than an oversight. ParseAtlasTestCases
	// drops the blocks of the other kind, so a schema run never loads the
	// migrate case: --run selects one case, the report shows one row, and there
	// is nothing to disambiguate. The migrate run does load both and is exactly
	// the ambiguity this check exists to reject. Both directions are pinned by
	// tests. Emitted unwrapped for the reason given in LoadCases.
	if err := validateUniqueCaseNames(cases, origins); err != nil {
		return nil, err
	}
	return cases, nil
}

// isAtlasCaseFile reports whether name is an Atlas-format test document.
//
// The suffix is `.test.hcl`, not `.hcl`: a bare .hcl file in the same directory
// is a schema definition, and reading one as a test document would fail on
// every table block. Matching the compound suffix is what separates the two,
// and it is why filepath.Ext alone -- which yields ".hcl" for both -- will not
// do.
func isAtlasCaseFile(name string) bool {
	return strings.HasSuffix(strings.ToLower(name), ".test.hcl")
}
