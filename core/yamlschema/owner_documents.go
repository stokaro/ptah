package yamlschema

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"reflect"
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
	groups, err := ownerPlatformGroups(owners, data)
	if err != nil {
		return err
	}
	for index, table := range input.Tables {
		for _, group := range owners.PlatformGroups() {
			node, written := groups[table.Key][group]
			if !written {
				continue
			}
			fragment, err := capture.fragment([]string{yamlext.EntryTable, table.Key, yamlext.PlatformKeyPrefix + group}, index, &node)
			if err != nil {
				return err
			}
			input.Fragments = append(input.Fragments, fragment)
		}
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

// ownerPlatformGroups returns, for each table key, the groups of its platform
// key that a document owner claims, with the node each group is written as.
// The typed decoding of the platform key keeps only strings, and an owner reads
// the value with its source positions, so the document is decoded again for the
// groups. A document that claims none is not decoded again.
func ownerPlatformGroups(owners yamlext.Set, data []byte) (map[string]map[string]yaml.Node, error) {
	claimed := owners.PlatformGroups()
	if len(claimed) == 0 {
		return nil, nil
	}
	rest := reflect.StructField{Name: "Rest", Type: reflect.TypeFor[map[string]yaml.Node](), Tag: `yaml:",inline"`}
	entry := reflect.StructOf([]reflect.StructField{
		{Name: "Platform", Type: reflect.TypeFor[map[string]yaml.Node](), Tag: `yaml:"platform"`}, rest,
	})
	wrapper := reflect.New(reflect.StructOf([]reflect.StructField{
		{Name: "Tables", Type: reflect.MapOf(reflect.TypeFor[string](), entry), Tag: `yaml:"tables"`}, rest,
	}))
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(wrapper.Interface()); err != nil {
		return nil, fmt.Errorf("parse YAML schema: %w", err)
	}
	groups := make(map[string]map[string]yaml.Node)
	tables := wrapper.Elem().Field(0)
	for _, key := range tables.MapKeys() {
		platform, _ := reflect.TypeAssert[map[string]yaml.Node](tables.MapIndex(key).Field(0))
		for _, group := range claimed {
			if node, written := platform[group]; written {
				if groups[key.String()] == nil {
					groups[key.String()] = make(map[string]yaml.Node)
				}
				groups[key.String()][group] = node
			}
		}
	}
	return groups, nil
}
