package yamlext

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
)

// Fragment is one explicitly present owner key. Node retains YAML scalar
// spelling, tags, nulls, sequence order, aliases and source positions without
// coercion. Path identifies the key from the document root. Table is -1 for
// a top-level section, or an index in Document.Tables for a key of a table
// entry, of one of its column or index entries, or of a top-level index entry
// that names the table.
//
// The path takes one of these shapes, which name the entry the key belongs to:
//
//	[key]                                    a top-level section
//	[tables, table, key]                     a table entry
//	[tables, table, columns|fields, column, key]
//	                                         a column entry
//	[tables, table, indexes, index, key]     an index entry of a table
//	[indexes, index, key]                    a top-level index entry
type Fragment struct {
	// Path contains mapping keys from the document root.
	Path []string
	// Table identifies a common table, or is -1 for a top-level section.
	Table int
	// Name is the name of the column or the index the key of a column or an
	// index entry belongs to, as the entry declares it. It is empty for the
	// other entries.
	Name string
	// Node is the authored value, including aliases and source positions.
	Node *Node
	// Inherited holds the keys of the entry itself that the key's owner reads
	// with it (see [EntrySection.Reads]) and the entry writes, by name.
	Inherited map[string]*Node
}

// Entry returns the frontend's entry the fragment's key belongs to: empty for
// a top-level section, or [EntryTable], [EntryColumn] or [EntryIndex], with the
// key. It returns false for a path of no shape [Fragment] lists.
func (f Fragment) Entry() (entry, key string, valid bool) {
	path := f.Path
	switch {
	case len(path) == 1:
		return "", path[0], true
	case len(path) == 3 && path[0] == EntryTable:
		return EntryTable, path[2], true
	case len(path) == 3 && path[0] == EntryIndex:
		return EntryIndex, path[2], true
	case len(path) == 5 && path[0] == EntryTable && (path[2] == EntryColumn || path[2] == "fields"):
		return EntryColumn, path[4], true
	case len(path) == 5 && path[0] == EntryTable && path[2] == EntryIndex:
		return EntryIndex, path[4], true
	}
	return "", "", false
}

// Document is the complete input to one owner's YAML interpretation. Source
// holds the original bytes, including common declarations and comments.
// Fragments contains only that owner's claimed keys, in source order. Missing
// keys have no fragment; an explicit null has a fragment with a null node.
// Tables includes all common table bindings, including forward declarations,
// with their resolved names. Each invocation receives its own mutable copy.
type Document struct {
	// Filename is the input path, or empty when parsing unnamed bytes.
	Filename string
	// Source is a private copy of the complete authored document.
	Source []byte
	// Fragments are this owner's explicitly present keys in source order.
	Fragments []Fragment
	// Tables provides resolved common table names in key order.
	Tables Tables
}

// DocumentResult carries the contributions and explicit knowledge established
// by one successful document interpretation. Empty Contributions does not imply
// absence: only Coverage makes that claim. A zero result claims nothing.
type DocumentResult struct {
	// Contributions contains the owner's validated objects and table facets.
	Contributions []Contribution
	// Coverage records desired knowledge using only this owner's model kinds.
	Coverage schemaext.Coverage
}

func checkDocumentExtension(extension Extension, extensions []Extension) error {
	if extension.Document == nil {
		return nil
	}
	if extension.Coverage != nil || extension.Cover != nil || extension.Entries != nil ||
		len(extension.TargetScopes) != 0 || len(extension.EntryAttributes) != 0 {
		return fmt.Errorf("YAML document owner %s also declares local decoding or coverage", extension.Owner)
	}
	for _, section := range extension.Sections {
		if section.Decode != nil {
			return fmt.Errorf("YAML document owner %s also decodes section %q locally", extension.Owner, section.Key)
		}
	}
	for _, section := range extension.EntrySections {
		if section.Decode != nil {
			return fmt.Errorf("YAML document owner %s also decodes table key %q locally", extension.Owner, section.Key)
		}
	}
	count := 0
	for _, candidate := range extensions {
		if candidate.Owner == extension.Owner {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("YAML document owner %s is declared more than once", extension.Owner)
	}
	return nil
}

// DocumentKey reports whether a key is interpreted by a whole-document owner.
// entry is empty for a top-level section, or EntryTable, EntryColumn or
// EntryIndex for a key of such an entry. Other entries and unknown keys return
// false.
func (s Set) DocumentKey(entry, key string) bool {
	index, found := s.sections[key]
	if entry != "" {
		index, found = s.entryKeys[entry][key]
	}
	return found && s.extensions[index].Document != nil
}

// DecodeDocuments invokes each whole-document owner once, in registration
// order, even when none of its keys is present. document must contain every
// explicitly present key claimed by those owners and all common table bindings.
// Each owner receives only its fragments and a private copy of the input.
// Unknown, duplicate or misbound fragments are refused before any callback.
// Cancellation, a callback error or an invalid contribution/coverage claim
// discards the entire result; no successful prefix is returned.
func (s Set) DecodeDocuments(ctx context.Context, document Document) (DocumentResult, error) {
	if err := ctx.Err(); err != nil {
		return DocumentResult{}, err
	}
	if !s.selected {
		return DocumentResult{}, ErrUnselected
	}
	batches, err := s.documentBatches(document)
	if err != nil {
		return DocumentResult{}, err
	}
	var joined DocumentResult
	for index, extension := range s.extensions {
		if extension.Document == nil {
			continue
		}
		if err := ctx.Err(); err != nil {
			return DocumentResult{}, err
		}
		input := Document{Filename: document.Filename, Source: slices.Clone(document.Source), Tables: slices.Clone(document.Tables), Fragments: cloneFragments(batches[index])}
		result, err := extension.Document(ctx, input)
		if err != nil {
			return DocumentResult{}, fmt.Errorf("YAML document of %s: %w", extension.Owner, err)
		}
		if err := ctx.Err(); err != nil {
			return DocumentResult{}, err
		}
		if err := checkContributions(extension, "YAML document of "+extension.Owner, result.Contributions, len(document.Tables)); err != nil {
			return DocumentResult{}, err
		}
		if err := checkDocumentCoverage(extension, result.Coverage); err != nil {
			return DocumentResult{}, err
		}
		if joined.Coverage, err = joined.Coverage.Combine(result.Coverage); err != nil {
			return DocumentResult{}, err
		}
		joined.Contributions = append(joined.Contributions, result.Contributions...)
	}
	return joined, nil
}

func (s Set) documentBatches(document Document) (map[int][]Fragment, error) {
	batches := make(map[int][]Fragment)
	seen := make(map[string]bool)
	for _, fragment := range document.Fragments {
		index, key, err := s.fragmentOwner(fragment, document.Tables)
		if err != nil {
			return nil, err
		}
		identity := strings.Join(fragment.Path, "\x00")
		if seen[identity] {
			return nil, fmt.Errorf("%w: YAML document repeats key %q at %q", schemaext.ErrDuplicate, key, fragment.Path)
		}
		seen[identity] = true
		batches[index] = append(batches[index], fragment)
	}
	return batches, nil
}

func (s Set) fragmentOwner(fragment Fragment, tables Tables) (int, string, error) {
	entry, key, valid := fragment.Entry()
	bound := fragment.Table >= 0 && fragment.Table < len(tables)
	switch {
	case !valid:
	case entry == "":
		valid = fragment.Table == -1 && fragment.Name == ""
	case fragment.Path[0] == EntryIndex:
		// A top-level index entry names its table, which the frontend
		// resolved into the binding.
		valid = bound && fragment.Name != ""
	case entry == EntryTable:
		valid = bound && fragment.Path[1] == tables[fragment.Table].Key && fragment.Name == ""
	default:
		valid = bound && fragment.Path[1] == tables[fragment.Table].Key && fragment.Name != ""
	}
	if !valid {
		return 0, "", fmt.Errorf("invalid YAML document fragment path or table binding")
	}
	if fragment.Node == nil || !s.DocumentKey(entry, key) {
		return 0, "", fmt.Errorf("no document owner reads YAML fragment %q", fragment.Path)
	}
	index := s.sections[key]
	if entry != "" {
		index = s.entryKeys[entry][key]
	}
	for name := range fragment.Inherited {
		if !slices.Contains(s.EntryReads(entry, key), name) {
			return 0, "", fmt.Errorf("YAML fragment %q carries the key %q of its entry, which its owner does not read", fragment.Path, name)
		}
	}
	return index, key, nil
}

func checkDocumentCoverage(extension Extension, claim schemaext.Coverage) error {
	if claim.Representation() != "" && claim.Representation() != schemaext.Desired {
		return fmt.Errorf("%w: YAML document of %s returned non-desired coverage", schemaext.ErrInvalidValue, extension.Owner)
	}
	for _, record := range claim.KindRecords() {
		if record.Model.Owner != extension.Owner || !slices.Contains(extension.Kinds, record.Model.Kind) {
			return fmt.Errorf("%w: YAML document of %s claims another owner's model", schemaext.ErrInvalidValue, extension.Owner)
		}
	}
	return nil
}

func cloneFragments(fragments []Fragment) []Fragment {
	result := make([]Fragment, 0, len(fragments))
	nodes := make(map[*Node]*Node)
	for _, fragment := range fragments {
		fragment.Path = slices.Clone(fragment.Path)
		fragment.Node = cloneNode(fragment.Node, nodes)
		if fragment.Inherited != nil {
			inherited := make(map[string]*Node, len(fragment.Inherited))
			for name, node := range fragment.Inherited {
				inherited[name] = cloneNode(node, nodes)
			}
			fragment.Inherited = inherited
		}
		result = append(result, fragment)
	}
	return result
}

func cloneNode(node *Node, nodes map[*Node]*Node) *Node {
	if node == nil {
		return nil
	}
	if cloned, exists := nodes[node]; exists {
		return cloned
	}
	cloned := new(*node)
	nodes[node] = cloned
	cloned.Content = make([]*Node, len(node.Content))
	for index, child := range node.Content {
		cloned.Content[index] = cloneNode(child, nodes)
	}
	cloned.Alias = cloneNode(node.Alias, nodes)
	return cloned
}
