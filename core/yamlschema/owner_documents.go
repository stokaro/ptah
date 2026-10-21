package yamlschema

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"go.yaml.in/yaml/v3"

	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlext"
)

// addOwnerDocuments interprets an owner's keys together after all common table
// bindings exist. Nodes remain uncoerced, and absent keys produce no fragment.
func (d document) addOwnerDocuments(ctx context.Context, db *schemamodel.Database, owners yamlext.Set, data []byte, filename string) error {
	input := yamlext.Document{Filename: filename, Source: data, Tables: d.documentTables(db)}
	capture := ownerCapture{data: data, nodes: make(map[yamlNodeOrigin]*yamlext.Node)}
	for _, key := range sortedKeys(d.Owned) {
		if owners.DocumentKey("", key) {
			node := d.Owned[key]
			fragment, err := capture.fragment([]string{key}, -1, &node)
			if err != nil {
				return err
			}
			input.Fragments = append(input.Fragments, fragment)
		}
	}
	for index, table := range input.Tables {
		for _, key := range sortedKeys(d.Tables[table.Key].Owned) {
			if owners.DocumentKey(yamlext.EntryTable, key) {
				node := d.Tables[table.Key].Owned[key]
				fragment, err := capture.fragment([]string{yamlext.EntryTable, table.Key, key}, index, &node)
				if err != nil {
					return err
				}
				input.Fragments = append(input.Fragments, fragment)
			}
		}
	}
	slices.SortStableFunc(input.Fragments, func(a, b yamlext.Fragment) int {
		return cmp.Compare(a.Node.Range.Start.ByteOffset, b.Node.Range.Start.ByteOffset)
	})
	result, err := owners.DecodeDocuments(ctx, input)
	if err != nil {
		return err
	}
	for _, contribution := range result.Contributions {
		if err := contribute(db, "owner document", contribution); err != nil {
			return err
		}
	}
	db.FeatureCoverage, err = db.FeatureCoverage.Combine(result.Coverage)
	return err
}

// fragment validates extension nodes that bypass typed YAML decoding.
// Validation does not coerce numeric scalars to machine-sized Go values.
func (c *ownerCapture) fragment(path []string, table int, node *yaml.Node) (yamlext.Fragment, error) {
	if err := validateOwnerSyntax(node); err != nil {
		return yamlext.Fragment{}, fmt.Errorf("parse YAML schema: %w", err)
	}
	captured, err := c.node(node)
	if err != nil {
		return yamlext.Fragment{}, fmt.Errorf("parse YAML schema: %w", err)
	}
	return yamlext.Fragment{Path: path, Table: table, Node: captured}, nil
}

func (c *ownerCapture) node(node *yaml.Node) (*yamlext.Node, error) {
	if node == nil {
		return nil, nil
	}
	span, err := c.sourceRange(node)
	if err != nil {
		return nil, err
	}
	// Owned mapping values copy their root yaml.Node. Source identity keeps an
	// alias of such a root attached to the same captured node.
	origin := yamlNodeOrigin{node.Kind, node.Line, node.Column}
	if captured, exists := c.nodes[origin]; exists {
		return captured, nil
	}
	captured := &yamlext.Node{Kind: captureKind(node.Kind), Tag: node.Tag, Value: node.Value, Anchor: node.Anchor,
		Range: span, Content: make([]*yamlext.Node, len(node.Content))}
	c.nodes[origin] = captured
	for index, child := range node.Content {
		captured.Content[index], err = c.node(child)
		if err != nil {
			return nil, err
		}
	}
	captured.Alias, err = c.node(node.Alias)
	return captured, err
}

func captureKind(kind yaml.Kind) yamlext.NodeKind {
	switch kind {
	case yaml.ScalarNode:
		return yamlext.ScalarNode
	case yaml.MappingNode:
		return yamlext.MappingNode
	case yaml.SequenceNode:
		return yamlext.SequenceNode
	case yaml.AliasNode:
		return yamlext.AliasNode
	default:
		return ""
	}
}
