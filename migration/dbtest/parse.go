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
