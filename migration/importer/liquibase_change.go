package importer

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// liquibaseChange is one change inside a changeset, reduced to the shape the
// XML, YAML and JSON changelogs share: the change type the author wrote, its
// scalar attributes, and the elements nested in it.
//
// The serializations spell nesting differently. XML nests elements; YAML and
// JSON wrap a list of one-key mappings in a plural key, so a column is
// `<column>` inside `<createTable>` in one and `columns: [{column: ...}]` in
// the other. The converter must not care which it was handed, and this shape is
// what lets one converter recognize every change type (stokaro/ptah#3625). A
// converter per serialization would agree on the day both were written and stop
// agreeing on the day one of them learned a new change.
type liquibaseChange struct {
	// name is the change type or element name, such as "createTable".
	name string
	// display is the name as the author's serialization spells it, so a
	// refusal quotes what the author wrote: "<createTable>" for XML and
	// "createTable" for YAML and JSON.
	display string
	// attrs are the scalar attributes.
	attrs map[string]string
	// children are the nested elements, in the order the author wrote them.
	children []liquibaseChange
	// text is the element's own text: the statement of a `sql` change, or the
	// SQL a `rollback` holds directly.
	text string
}

// liquibaseXMLChange reduces one XML element and everything below it.
func liquibaseXMLChange(node liquibaseXMLAny) liquibaseChange {
	change := liquibaseChange{
		name:    node.XMLName.Local,
		display: "<" + node.XMLName.Local + ">",
		attrs:   make(map[string]string, len(node.Attrs)),
		text:    strings.TrimSpace(node.Text),
	}
	for _, attr := range node.Attrs {
		change.attrs[attr.Name.Local] = attr.Value
	}
	for _, child := range node.Children {
		if child.XMLName.Local == "" {
			// Chardata between elements, which encoding/xml reports as an
			// element with no name.
			continue
		}
		change.children = append(change.children, liquibaseXMLChange(child))
	}
	return change
}

// liquibaseDocumentChange reduces one `<type>: <value>` entry of a YAML or JSON
// changelog.
//
// A scalar value is the change's text, which is how `- sql: "SELECT 1"` is
// written. In a mapping, a scalar becomes an attribute, a mapping becomes one
// nested element named by its key, and a list of one-key mappings becomes one
// nested element per entry, named by the entry's key -- the plural wrapper
// (`columns`) has no XML counterpart and disappears.
//
// A list whose entries are not one-key mappings has no reading in that scheme.
// It is kept as a nested element named by its key rather than dropped, so the
// converter refuses it by the name the author wrote.
func liquibaseDocumentChange(name string, value any) liquibaseChange {
	change := liquibaseChange{name: name, display: name, attrs: make(map[string]string)}
	mapping, ok := value.(map[string]any)
	if !ok {
		if value != nil {
			change.text = strings.TrimSpace(liquibaseScalar(value))
		}
		return change
	}
	for _, key := range slices.Sorted(maps.Keys(mapping)) {
		switch typed := mapping[key].(type) {
		case nil:
		case map[string]any:
			change.children = append(change.children, liquibaseDocumentChange(key, typed))
		case []any:
			entries, ok := liquibaseTaggedEntries(typed)
			if !ok {
				change.children = append(change.children, liquibaseChange{
					name: key, display: key, attrs: make(map[string]string),
				})
				continue
			}
			for _, entry := range entries {
				change.children = append(change.children, liquibaseDocumentChange(entry.key, entry.value))
			}
		default:
			change.attrs[key] = liquibaseScalar(typed)
		}
	}
	return change
}

type liquibaseTaggedEntry struct {
	key   string
	value any
}

// liquibaseTaggedEntries reads a list of one-key mappings, reporting false when
// any entry is something else.
func liquibaseTaggedEntries(list []any) ([]liquibaseTaggedEntry, bool) {
	entries := make([]liquibaseTaggedEntry, 0, len(list))
	for _, item := range list {
		key, value, ok := liquibaseSingleKey(item)
		if !ok {
			return nil, false
		}
		entries = append(entries, liquibaseTaggedEntry{key: key, value: value})
	}
	return entries, true
}

// liquibaseScalar spells a decoded YAML or JSON scalar the way the XML
// attribute holding the same value would be spelled.
//
// JSON decodes every number into float64, so 3 arrives as 3.0; formatting with
// the shortest representation writes it back as "3" rather than "3.000000".
func liquibaseScalar(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case bool:
		return strconv.FormatBool(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", typed)
	}
}

// liquibaseSQLChange builds the `sql` change a bare rollback string stands for.
func liquibaseSQLChange(text, display string) liquibaseChange {
	return liquibaseChange{
		name: "sql", display: display, attrs: make(map[string]string), text: strings.TrimSpace(text),
	}
}
