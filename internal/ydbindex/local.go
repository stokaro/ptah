package ydbindex

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/platform/capability"
)

// LocalAttributes lists the local-index declaration attributes in stable order.
func LocalAttributes() []string {
	return []string{"false_positive_probability", "ngram_size", "case_sensitive"}
}

// ParseOptionsDeclaration reads full-text or local-index options from annotations
// and YAML. Mixing the families is refused before a renderer sees the schema.
func ParseOptionsDeclaration(values map[string]string) (map[string]string, error) {
	text, err := ParseFullTextDeclaration(values)
	if err != nil {
		return nil, err
	}
	local := make(map[string]string)
	for _, name := range LocalAttributes() {
		if value, ok := values[name]; ok {
			local[name] = value
		}
	}
	if len(local) == 0 {
		return text, nil
	}
	if len(text) > 0 {
		return nil, fmt.Errorf("local and full-text index options cannot be combined")
	}
	kind, err := KindOf(values["type"])
	if err != nil {
		return nil, err
	}
	return ResolveLocal(kind, local)
}

// IsLocal reports an index stored with a column table's data portions.
func (k Kind) IsLocal() bool { return k == LocalBloom || k == LocalNgram || k == LocalMinMax }

// LocalCapability is the feature gate for a local-index method.
func (k Kind) LocalCapability() capability.Capability {
	switch k {
	case LocalBloom:
		return capability.LocalBloomIndexes
	case LocalNgram:
		return capability.LocalNgramIndexes
	case LocalMinMax:
		return capability.LocalMinMaxIndexes
	default:
		return ""
	}
}

// ResolveLocal validates local-index settings and supplies the release defaults.
// YDB 26.2 omits unset n-gram parameters from its description: the constructor
// defaults are probability 0.1, n-gram size 3 and case-sensitive matching.
func ResolveLocal(kind Kind, settings map[string]string) (map[string]string, error) {
	if !kind.IsLocal() {
		return nil, fmt.Errorf("index method %s is not local", kind)
	}
	out := make(map[string]string)
	if kind != LocalMinMax {
		out["false_positive_probability"] = "0.1"
	}
	if kind == LocalNgram {
		out["ngram_size"] = "3"
		out["case_sensitive"] = "true"
	}
	for _, key := range slices.Sorted(maps.Keys(settings)) {
		value := strings.TrimSpace(settings[key])
		if _, allowed := out[key]; !allowed {
			return nil, fmt.Errorf("setting %q is not supported by %s", key, kind)
		}
		switch key {
		case "false_positive_probability":
			v, err := strconv.ParseFloat(value, 64)
			if err != nil || math.IsNaN(v) || v <= 0 || v >= 1 {
				return nil, fmt.Errorf("false_positive_probability must be between 0 and 1, exclusively")
			}
			out[key] = strconv.FormatFloat(v, 'g', -1, 64)
		case "ngram_size":
			v, err := strconv.ParseUint(value, 10, 32)
			if err != nil || v < 3 || v > 8 {
				return nil, fmt.Errorf("ngram_size must be between 3 and 8")
			}
			out[key] = strconv.FormatUint(v, 10)
		case "case_sensitive":
			if value != "true" && value != "false" {
				return nil, fmt.Errorf("case_sensitive must be true or false")
			}
			out[key] = value
		}
	}
	if kind == LocalNgram {
		probability, _ := strconv.ParseFloat(out["false_positive_probability"], 64)
		// YDB 26.2 aborts in TOlapSchemaUpdate instead of returning a YQL error
		// when the constructor rejects a size or probability-derived hash count.
		if math.Floor(-math.Log2(probability)) > 8 {
			return nil, fmt.Errorf("false_positive_probability must produce at most 8 n-gram hashes (greater than 1/512)")
		}
	}
	return out, nil
}

// LocalEqual compares the effective settings, including server defaults.
func LocalEqual(kind Kind, a, b map[string]string) bool {
	left, leftErr := ResolveLocal(kind, a)
	right, rightErr := ResolveLocal(kind, b)
	return leftErr == nil && rightErr == nil && maps.Equal(left, right)
}

// LocalClause renders validated numeric and boolean settings in stable order.
func LocalClause(settings map[string]string) string {
	if len(settings) == 0 {
		return ""
	}
	parts := make([]string, 0, len(settings))
	for _, name := range slices.Sorted(maps.Keys(settings)) {
		parts = append(parts, name+" = "+settings[name])
	}
	return "WITH (" + strings.Join(parts, ", ") + ")"
}
