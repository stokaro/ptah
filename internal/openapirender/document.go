package openapirender

import (
	yaml "go.yaml.in/yaml/v3"
)

// document is the minimal OpenAPI 3.0 envelope. Field order here is the emitted
// key order.
type document struct {
	OpenAPI    string         `yaml:"openapi"`
	Info       info           `yaml:"info"`
	Servers    []server       `yaml:"servers"`
	Paths      map[string]any `yaml:"paths"`
	Components components     `yaml:"components"`
}

type info struct {
	Title   string `yaml:"title"`
	Version string `yaml:"version"`
}

type server struct {
	URL string `yaml:"url"`
}

type components struct {
	Schemas *orderedMap `yaml:"schemas"`
}

// schemaObject is an OpenAPI 3.0 Schema Object. Field order is the emitted key
// order; omitempty drops the keys that do not apply to a given column or table.
type schemaObject struct {
	Type        string        `yaml:"type,omitempty"`
	Format      string        `yaml:"format,omitempty"`
	Items       *schemaObject `yaml:"items,omitempty"`
	Description string        `yaml:"description,omitempty"`
	Enum        []any         `yaml:"enum,omitempty"`
	MaxLength   *int          `yaml:"maxLength,omitempty"`
	Minimum     *int          `yaml:"minimum,omitempty"`
	Nullable    bool          `yaml:"nullable,omitempty"`
	Required    []string      `yaml:"required,omitempty"`
	Properties  *orderedMap   `yaml:"properties,omitempty"`
}

// orderedMap is a string-keyed map that marshals to YAML preserving insertion
// order, so schemas follow table-definition order and properties follow
// column order instead of being alphabetized.
type orderedMap struct {
	keys []string
	vals []any
}

func newOrderedMap() *orderedMap { return &orderedMap{} }

func (m *orderedMap) set(key string, value any) {
	m.keys = append(m.keys, key)
	m.vals = append(m.vals, value)
}

func (m *orderedMap) len() int { return len(m.keys) }

// MarshalYAML builds a mapping node with entries in insertion order.
func (m *orderedMap) MarshalYAML() (any, error) {
	node := &yaml.Node{Kind: yaml.MappingNode}
	for i, key := range m.keys {
		keyNode := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}
		valNode := &yaml.Node{}
		if err := valNode.Encode(m.vals[i]); err != nil {
			return nil, err
		}
		node.Content = append(node.Content, keyNode, valNode)
	}
	return node, nil
}
