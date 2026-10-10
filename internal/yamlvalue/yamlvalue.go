// Package yamlvalue holds the value types Ptah's YAML schema format reads its
// keys with: a scalar read as its text, a list written as a sequence or a
// comma-separated scalar, and a mapping that keeps the order its keys were
// written in. The frontend, ptah.run/core/yamlschema, and the feature owners
// that read keys of their own share them, so a key reads alike whoever owns
// it.
package yamlvalue

import (
	"bytes"
	"fmt"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Scalar is a scalar read as its text, whatever its YAML type: `10`, `true`
// and `PT1H` all read as written. A null reads as the empty string.
type Scalar string

// UnmarshalYAML reads a scalar node, and refuses any other.
func (s *Scalar) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("expected scalar, got %s", value.ShortTag())
	}
	if value.Tag == "!!null" {
		*s = ""
		return nil
	}
	*s = Scalar(value.Value)
	return nil
}

// List is a list of scalars, written as a sequence or as one comma-separated
// scalar. A null or an empty scalar reads as no list.
type List []string

// UnmarshalYAML reads a sequence or a scalar node, and refuses any other.
func (s *List) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.SequenceNode:
		values := make([]string, 0, len(value.Content))
		for _, item := range value.Content {
			var scalar Scalar
			if err := item.Decode(&scalar); err != nil {
				return err
			}
			values = append(values, string(scalar))
		}
		*s = values
	case yaml.ScalarNode:
		if value.Tag == "!!null" || value.Value == "" {
			*s = nil
			return nil
		}
		*s = strings.Split(value.Value, ",")
	default:
		return fmt.Errorf("expected scalar or sequence, got %s", value.ShortTag())
	}
	return nil
}

// OrderedMap is a mapping read in the order its keys were written, which a Go
// map does not keep. Each value is read strictly: a key its type does not
// declare is refused.
type OrderedMap[V any] []Entry[V]

// Entry is one key of an [OrderedMap] and its value.
type Entry[V any] struct {
	Name  string
	Value V
}

// UnmarshalYAML reads a mapping node, refusing a key written twice. A null
// reads as no entries.
func (m *OrderedMap[V]) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode && value.Tag == "!!null" {
		*m = nil
		return nil
	}
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("expected mapping, got %s", value.ShortTag())
	}

	entries := make([]Entry[V], 0, len(value.Content)/2)
	seen := make(map[string]bool, len(value.Content)/2)
	for i := 0; i < len(value.Content); i += 2 {
		keyNode := value.Content[i]
		valueNode := value.Content[i+1]

		if seen[keyNode.Value] {
			return fmt.Errorf("duplicate key %q", keyNode.Value)
		}
		seen[keyNode.Value] = true

		var entryValue V
		if err := DecodeKnownFields(valueNode, &entryValue); err != nil {
			return err
		}
		entries = append(entries, Entry[V]{Name: keyNode.Value, Value: entryValue})
	}

	*m = entries
	return nil
}

// DecodeKnownFields decodes node into target, refusing a key target's type
// does not declare, which yaml.Node.Decode alone does not.
func DecodeKnownFields[V any](node *yaml.Node, target *V) error {
	var buffer bytes.Buffer
	encoder := yaml.NewEncoder(&buffer)
	if err := encoder.Encode(node); err != nil {
		return err
	}
	if err := encoder.Close(); err != nil {
		return err
	}

	decoder := yaml.NewDecoder(&buffer)
	decoder.KnownFields(true)
	return decoder.Decode(target)
}
