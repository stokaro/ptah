// Package annotationschema renders JSON Schema for Ptah Go annotations.
package annotationschema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"go.5x5.cz/ptah/internal/annotationmeta"
)

const SchemaPath = "schemas/ptah-annotations.schema.json"

// Generate renders the JSON Schema document.
func Generate() ([]byte, error) {
	doc := map[string]any{
		"$schema":     "https://json-schema.org/draft/2020-12/schema",
		"$id":         "https://stokaro.github.io/ptah/schemas/ptah-annotations.schema.json",
		"title":       "Ptah Go Annotation Directives",
		"description": "Schema for parsed //ptah Go annotation directives.",
		"oneOf":       directiveRefs(),
		"$defs":       directiveDefs(),
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(doc); err != nil {
		return nil, fmt.Errorf("encode annotation JSON Schema: %w", err)
	}
	return buf.Bytes(), nil
}

func directiveRefs() []map[string]string {
	directives := annotationmeta.Directives()
	refs := make([]map[string]string, 0, len(directives))
	for _, directive := range directives {
		refs = append(refs, map[string]string{
			"$ref": "#/$defs/" + defName(directive.Name),
		})
	}
	return refs
}

func directiveDefs() map[string]any {
	defs := make(map[string]any)
	for _, directive := range annotationmeta.Directives() {
		defs[defName(directive.Name)] = directiveDef(directive)
	}
	return defs
}

func directiveDef(directive annotationmeta.Directive) map[string]any {
	requiredAttrs := make([]string, 0)
	properties := make(map[string]any)
	for _, attr := range directive.Attributes {
		properties[attr.Name] = attrSchema(attr)
		if attr.Required {
			requiredAttrs = append(requiredAttrs, attr.Name)
		}
	}
	sort.Strings(requiredAttrs)

	attrs := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if len(requiredAttrs) > 0 {
		attrs["required"] = requiredAttrs
	}
	if directive.AllowPlatform {
		attrs["patternProperties"] = map[string]any{
			annotationmeta.PlatformAttributePattern: map[string]any{
				"type":        "string",
				"description": "Dialect-specific platform override.",
			},
		}
	}

	return map[string]any{
		"type":        "object",
		"description": directive.Description,
		"properties": map[string]any{
			"directive":  map[string]any{"const": directive.Name},
			"attributes": attrs,
		},
		"required":             []string{"directive", "attributes"},
		"additionalProperties": false,
	}
}

func attrSchema(attr annotationmeta.Attribute) map[string]any {
	if attr.Retired != "" {
		// A retired attribute keeps its place in properties rather than
		// leaving it. Removing the key would still be an error, because
		// additionalProperties is false -- but a generic "additional property
		// is not allowed" is what an editor says about a typo, and this
		// attribute was spelled correctly and meant something once. An empty
		// "not" never validates, so any value fails, and the reason travels
		// with the failure (stokaro/ptah#1625).
		// An empty schema object, which validates everything, negated into one
		// that validates nothing.
		alwaysFails := make(map[string]any)
		return map[string]any{
			"description":    attr.Description + " " + attr.Retired + ".",
			"not":            alwaysFails,
			"x-ptah-retired": attr.Retired,
		}
	}

	schema := map[string]any{
		"description": attr.Description,
	}
	switch attr.Value {
	case "boolean":
		schema["type"] = "string"
		schema["enum"] = []string{"true", "false"}
	case "comma-list", "sql", "string":
		schema["type"] = "string"
	default:
		schema["type"] = "string"
	}
	if attr.AliasFor != "" {
		schema["x-ptah-alias-for"] = attr.AliasFor
	}
	if attr.Boolean {
		schema["x-ptah-bare-boolean"] = true
	}
	return schema
}

func defName(directive string) string {
	return strings.ReplaceAll(directive, ":", ".")
}
