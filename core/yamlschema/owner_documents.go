package yamlschema

import (
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
	nodes := make(map[*yaml.Node]*yamlext.Node)
	for _, key := range sortedKeys(d.Owned) {
		if owners.DocumentKey("", key) {
			node := d.Owned[key]
			fragment, err := captureFragment([]string{key}, -1, &node, nodes)
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
				fragment, err := captureFragment([]string{yamlext.EntryTable, table.Key, key}, index, &node, nodes)
				if err != nil {
					return err
				}
				input.Fragments = append(input.Fragments, fragment)
			}
		}
	}
	slices.SortStableFunc(input.Fragments, func(a, b yamlext.Fragment) int {
		if a.Node.Line != b.Node.Line {
			return a.Node.Line - b.Node.Line
		}
		return a.Node.Column - b.Node.Column
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

// captureFragment validates extension nodes that bypass typed YAML decoding.
// Validation does not coerce numeric scalars to machine-sized Go values.
func captureFragment(path []string, table int, node *yaml.Node, nodes map[*yaml.Node]*yamlext.Node) (yamlext.Fragment, error) {
	if err := validateOwnerSyntax(node); err != nil {
		return yamlext.Fragment{}, fmt.Errorf("parse YAML schema: %w", err)
	}
	return yamlext.Fragment{Path: path, Table: table, Node: captureNode(node, nodes)}, nil
}

func captureNode(node *yaml.Node, nodes map[*yaml.Node]*yamlext.Node) *yamlext.Node {
	if node == nil {
		return nil
	}
	if captured, exists := nodes[node]; exists {
		return captured
	}
	captured := &yamlext.Node{Kind: captureKind(node.Kind), Tag: node.Tag, Value: node.Value, Anchor: node.Anchor,
		Line: node.Line, Column: node.Column, Content: make([]*yamlext.Node, len(node.Content))}
	nodes[node] = captured
	for index, child := range node.Content {
		captured.Content[index] = captureNode(child, nodes)
	}
	captured.Alias = captureNode(node.Alias, nodes)
	return captured
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
