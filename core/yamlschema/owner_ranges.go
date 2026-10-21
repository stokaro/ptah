package yamlschema

import (
	"fmt"

	rangedyaml "github.com/oasdiff/yaml3"
	"go.yaml.in/yaml/v3"

	"ptah.run/core/yamlext"
)

type yamlNodeOrigin struct {
	kind         yaml.Kind
	line, column int
}

type yamlRangeRecord struct {
	tag, value, anchor string
	children           int
	span               yamlext.SourceRange
}

type ownerCapture struct {
	data   []byte
	nodes  map[yamlNodeOrigin]*yamlext.Node
	ranges map[yamlNodeOrigin]yamlRangeRecord
}

// indexRanges uses only the fork's AST parser, which exposes end marks. It must
// not decode Go values: typed decoding and structural validation stay with the
// maintained frontend decoder. Every captured node is checked against this
// index before a position is accepted, so parser drift cannot invent a span.
func (c *ownerCapture) indexRanges() error {
	positions, err := indexYAMLPositions(c.data)
	if err != nil {
		return err
	}
	var root rangedyaml.Node
	if err := rangedyaml.Unmarshal(c.data, &root); err != nil {
		return fmt.Errorf("index YAML source ranges: %w", err)
	}
	c.ranges = make(map[yamlNodeOrigin]yamlRangeRecord)
	return c.indexRangeNode(&root, positions)
}

func (c *ownerCapture) indexRangeNode(node *rangedyaml.Node, positions yamlPositions) error {
	if node.Kind != rangedyaml.DocumentNode {
		start, err := positions.position(node.Line, node.Column)
		if err != nil {
			return err
		}
		end, err := positions.position(node.EndLine, node.EndColumn)
		if err != nil {
			return err
		}
		if end.ByteOffset < start.ByteOffset {
			return fmt.Errorf("YAML source range ends before its start at %d:%d", node.Line, node.Column)
		}
		key := yamlNodeOrigin{yaml.Kind(node.Kind), node.Line, node.Column}
		record := yamlRangeRecord{node.Tag, node.Value, node.Anchor, len(node.Content), yamlext.SourceRange{Start: start, End: end}}
		if previous, exists := c.ranges[key]; exists && previous != record {
			return fmt.Errorf("ambiguous YAML source range at %d:%d", node.Line, node.Column)
		}
		c.ranges[key] = record
	}
	for _, child := range node.Content {
		if err := c.indexRangeNode(child, positions); err != nil {
			return err
		}
	}
	return nil
}

func (c *ownerCapture) sourceRange(node *yaml.Node) (yamlext.SourceRange, error) {
	if c.ranges == nil {
		if err := c.indexRanges(); err != nil {
			return yamlext.SourceRange{}, err
		}
	}
	record, found := c.ranges[yamlNodeOrigin{node.Kind, node.Line, node.Column}]
	if !found || record.tag != node.Tag || record.value != node.Value || record.anchor != node.Anchor || record.children != len(node.Content) {
		return yamlext.SourceRange{}, fmt.Errorf("YAML source parsers disagree at %d:%d", node.Line, node.Column)
	}
	return record.span, nil
}
