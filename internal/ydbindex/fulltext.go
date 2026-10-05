package ydbindex

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"ptah.run/internal/ydbtype"
)

// FullTextPlainMethod and FullTextRelevanceMethod are YDB's full-text access
// methods. Both index text synchronously; the latter also stores relevance data.
const (
	FullTextPlainMethod     = "fulltext_plain"
	FullTextRelevanceMethod = "fulltext_relevance"
)

// IsFullText reports whether kind is a YDB full-text index.
func (k Kind) IsFullText() bool { return k == FullTextPlain || k == FullTextRelevance }

// fullTextOptions names the options accepted by the measured YDB 26.2 grammar.
// Values name their scalar grammar. The reader uses the same resolver as the
// renderer and comparator, so an option cannot disappear during introspection.
var fullTextOptions = map[string]string{
	"tokenizer": "tokenizer", "language": "language",
	"use_filter_lowercase": "bool", "use_filter_stopwords": "bool",
	"use_filter_ngram": "bool", "use_filter_edge_ngram": "bool",
	"filter_ngram_min_length": "integer", "filter_ngram_max_length": "integer",
	"use_filter_length": "bool", "filter_length_min": "integer", "filter_length_max": "integer",
	"use_filter_snowball": "bool",
}

// ResolveFullText validates and normalizes the WITH options of a full-text
// index. Settings use the shared index StorageParams map, which carries WITH
// options through Go, YAML, catalog and AST conversions. A missing option stays
// missing; YDB 26.2 does not supply the n-gram bounds claimed by newer docs.
func ResolveFullText(options map[string]string) (map[string]string, error) {
	resolved := make(map[string]string, len(options))
	for _, key := range slices.Sorted(maps.Keys(options)) {
		grammar, known := fullTextOptions[key]
		if !known {
			return nil, fmt.Errorf("unknown YDB full-text index option %q", key)
		}
		value, err := fullTextValue(grammar, options[key])
		if err != nil {
			return nil, fmt.Errorf("YDB full-text index option %s: %w", key, err)
		}
		resolved[key] = value
	}
	if len(resolved) == 0 {
		return nil, fmt.Errorf("a YDB full-text index requires analyzer settings in WITH")
	}
	if resolved["tokenizer"] == "" {
		return nil, fmt.Errorf("a YDB full-text index requires tokenizer")
	}
	if resolved["use_filter_ngram"] == "true" || resolved["use_filter_edge_ngram"] == "true" {
		for _, name := range []string{"filter_ngram_min_length", "filter_ngram_max_length"} {
			if _, present := resolved[name]; !present {
				return nil, fmt.Errorf("a YDB n-gram filter requires %s", name)
			}
		}
	}
	return resolved, nil
}

// fullTextValue validates one scalar without allowing it to add SQL tokens.
func fullTextValue(grammar, value string) (string, error) {
	value = strings.TrimSpace(value)
	switch grammar {
	case "bool":
		if value == "true" || value == "false" {
			return value, nil
		}
		return "", fmt.Errorf("%q is not true or false", value)
	case "integer":
		number, err := strconv.ParseUint(value, 10, 31)
		if err != nil {
			return "", fmt.Errorf("%q is not a nonnegative 32-bit integer", value)
		}
		return strconv.FormatUint(number, 10), nil
	case "tokenizer":
		switch value {
		case "standard", "whitespace", "keyword":
			return value, nil
		default:
			return "", fmt.Errorf("unknown tokenizer %q", value)
		}
	case "language":
		if value == "" {
			return "", fmt.Errorf("the language is empty")
		}
		return value, nil
	default:
		return "", fmt.Errorf("unknown option grammar %q", grammar)
	}
}

// FullTextClause renders validated options in stable order. String values use
// YQL escaping; boolean and integer values are already scalar literals.
func FullTextClause(options map[string]string) string {
	parts := make([]string, 0, len(options))
	for _, key := range slices.Sorted(maps.Keys(options)) {
		value := options[key]
		if key == "language" {
			value = ydbtype.StringLiteral(value)
		}
		parts = append(parts, key+"="+value)
	}
	return "WITH (" + strings.Join(parts, ", ") + ")"
}

// FullTextEqual compares declared and catalog options. Omitted boolean filters
// are disabled, so an explicit false has the same behavior as omission.
func FullTextEqual(declared, held map[string]string) bool {
	left, leftErr := ResolveFullText(declared)
	right, rightErr := ResolveFullText(held)
	if leftErr != nil || rightErr != nil {
		return false
	}
	for key, grammar := range fullTextOptions {
		if grammar != "bool" {
			continue
		}
		if left[key] == "false" {
			delete(left, key)
		}
		if right[key] == "false" {
			delete(right, key)
		}
	}
	return maps.Equal(left, right)
}

// FullTextAttributes lists the annotation and YAML keys that declare text analysis.
func FullTextAttributes() []string { return slices.Sorted(maps.Keys(fullTextOptions)) }

// ParseFullTextDeclaration extracts analyzer options from an index declaration.
// Without an analyzer attribute it returns nil, leaving other index families alone.
func ParseFullTextDeclaration(values map[string]string) (map[string]string, error) {
	options := make(map[string]string)
	for _, name := range FullTextAttributes() {
		if value, present := values[name]; present {
			options[name] = value
		}
	}
	if len(options) == 0 {
		return nil, nil
	}
	return ResolveFullText(options)
}
