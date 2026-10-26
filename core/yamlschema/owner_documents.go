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
	entries, err := d.ownerEntryFragments(owners, data, input.Tables, &capture)
	if err != nil {
		return err
	}
	input.Fragments = append(input.Fragments, entries...)
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

// ownerEntryFragments captures the keys document owners add to column and
// index entries, with the keys of the entry each owner reads with its own.
// The typed decoding keeps no source position of an entry's keys, so the
// document is decoded again as nodes, and only when an owner claims such a
// key.
func (d document) ownerEntryFragments(owners yamlext.Set, data []byte, tables yamlext.Tables, capture *ownerCapture) ([]yamlext.Fragment, error) {
	if len(owners.EntryKeys(yamlext.EntryColumn))+len(owners.EntryKeys(yamlext.EntryIndex)) == 0 {
		return nil, nil
	}
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("parse YAML schema: %w", err)
	}
	entries := entryCapture{owners: owners, root: &root, capture: capture}
	for index, table := range tables {
		if err := entries.tableEntries(index, table.Key, d.Tables[table.Key]); err != nil {
			return nil, err
		}
	}
	for _, key := range sortedKeys(d.Indexes) {
		if err := entries.topLevelIndex(tables, key, d.Indexes[key]); err != nil {
			return nil, err
		}
	}
	return entries.fragments, nil
}

// entryCapture collects the fragments of the owners' keys of column and index
// entries from the document decoded as nodes.
type entryCapture struct {
	owners    yamlext.Set
	root      *yaml.Node
	capture   *ownerCapture
	fragments []yamlext.Fragment
}

// tableEntries captures the owners' keys of a table entry's columns, fields
// and indexes.
func (e *entryCapture) tableEntries(index int, key string, spec tableSpec) error {
	for _, group := range []struct {
		key     string
		entries orderedMap[fieldSpec]
	}{{yamlext.EntryColumn, spec.Columns}, {"fields", spec.Fields}} {
		for _, column := range group.entries {
			path := []string{yamlext.EntryTable, key, group.key, column.Name}
			if err := e.add(yamlext.EntryColumn, path, index, valueOrDefault(column.Value.Name, column.Name), column.Value.Owned); err != nil {
				return err
			}
		}
	}
	for _, entry := range spec.Indexes {
		path := []string{yamlext.EntryTable, key, yamlext.EntryIndex, entry.Name}
		if err := e.add(yamlext.EntryIndex, path, index, valueOrDefault(entry.Value.Name, entry.Name), entry.Value.Owned); err != nil {
			return err
		}
	}
	return nil
}

// topLevelIndex captures the owners' keys of a top-level index entry, bound
// to the table the entry names, which the document has to declare.
func (e *entryCapture) topLevelIndex(tables yamlext.Tables, key string, spec indexSpec) error {
	if !slices.ContainsFunc(sortedKeys(spec.Owned), func(owned string) bool { return e.owners.DocumentKey(yamlext.EntryIndex, owned) }) {
		return nil
	}
	table, err := tables.Find("", string(spec.TableName))
	if err != nil {
		return fmt.Errorf("parse YAML schema: index %q: %w", key, err)
	}
	if table < 0 {
		return fmt.Errorf("parse YAML schema: index %q names table %q, which the document does not declare; an owner's key of an index needs its table", key, spec.TableName)
	}
	return e.add(yamlext.EntryIndex, []string{yamlext.EntryIndex, key}, table, valueOrDefault(spec.Name, key), spec.Owned)
}

// add captures each key of owned that a document owner reads, at path, with
// the keys of the entry its owner reads with it.
func (e *entryCapture) add(entry string, path []string, table int, name string, owned map[string]yaml.Node) error {
	node := entryNode(e.root, path...)
	for _, key := range sortedKeys(owned) {
		if !e.owners.DocumentKey(entry, key) {
			continue
		}
		value := mappingValue(node, key)
		if value == nil {
			return fmt.Errorf("parse YAML schema: key %q of %q has no value", key, path)
		}
		fragment, err := e.capture.fragment(append(slices.Clone(path), key), table, value)
		if err != nil {
			return err
		}
		fragment.Name = name
		if fragment.Inherited, err = e.inherited(node, e.owners.EntryReads(entry, key)); err != nil {
			return err
		}
		e.fragments = append(e.fragments, fragment)
	}
	return nil
}

// inherited captures the keys of reads an entry writes, or nil for none.
func (e *entryCapture) inherited(node *yaml.Node, reads []string) (map[string]*yamlext.Node, error) {
	var inherited map[string]*yamlext.Node
	for _, read := range reads {
		written := mappingValue(node, read)
		if written == nil {
			continue
		}
		captured, err := e.capture.node(written)
		if err != nil {
			return nil, fmt.Errorf("parse YAML schema: %w", err)
		}
		if inherited == nil {
			inherited = make(map[string]*yamlext.Node)
		}
		inherited[read] = captured
	}
	return inherited, nil
}

// entryNode follows path through the mappings of a decoded document, aliases
// included, and returns the node it ends at, or nil.
func entryNode(root *yaml.Node, path ...string) *yaml.Node {
	node := root
	if node.Kind == yaml.DocumentNode && len(node.Content) == 1 {
		node = node.Content[0]
	}
	for _, key := range path {
		node = mappingValue(node, key)
		if node == nil {
			return nil
		}
	}
	return node
}

// mappingValue returns the value of key in a mapping node, following an
// alias of the mapping and of the value, or nil.
func mappingValue(node *yaml.Node, key string) *yaml.Node {
	for node != nil && node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		if node.Content[index].Value == key {
			value := node.Content[index+1]
			for value.Kind == yaml.AliasNode && value.Alias != nil {
				value = value.Alias
			}
			return value
		}
	}
	return nil
}

// keyNodeLine returns the line key is written on in a mapping node, or 0.
func keyNodeLine(node *yaml.Node, key string) int {
	for node != nil && node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	if node == nil || node.Kind != yaml.MappingNode {
		return 0
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		if node.Content[index].Value == key {
			return node.Content[index].Line
		}
	}
	return 0
}
