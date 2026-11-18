// Package keyedversions writes a migration document whose numeric versions name
// no migration: those of an Atlas directory whose files run under their
// positions in file-name order. A position moves when a file is added before
// it, so a document read by another process names each migration by its
// version key alone. The run document of `ptah migrations up` and `down` and
// the status document of `ptah migrations status` are written through it when
// the migrator reports [migrator.Migrator.KeyedByPosition].
package keyedversions

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
)

// members maps each member of one object that spells migrations by their
// numeric version to the members beside it that carry the same identity as a
// key. A nil list means the member has no key and is always left out.
type members map[string][]string

var (
	// statusMembers are the members of a status: the status document itself,
	// and the status a run document carries. checkpoint_version has no key
	// member; the checkpoint's own entry in the migrations list names it.
	statusMembers = members{
		"current_version":         {"current_version_key"},
		"applied_migrations":      {"applied_migration_keys"},
		"pending_migrations":      {"pending_migration_keys"},
		"out_of_order_migrations": {"out_of_order_migration_keys"},
		"checkpoint_version":      nil,
	}
	// runMembers are the members of a run document's top level.
	runMembers = members{
		"planned":     {"planned_keys"},
		"applied":     {"applied_keys"},
		"rolled_back": {"rolled_back_keys"},
	}
	// entryMembers are the members of one migration named in a list or a
	// failure.
	entryMembers = members{"version": {"version_key"}}
	// revisionMembers are the members of a revision row, which names its
	// migration in atlas_version.
	revisionMembers = members{"version": {"atlas_version"}}
)

// paths names every object [Strip] edits, by its path from the top of the
// document: a member name per level, and [] for the elements of a list. The
// top level is a run document's or a status document's, and the two share no
// member name. An object at any other path keeps every member.
var paths = map[string]members{
	"":                            merged(runMembers, statusMembers),
	"failure":                     entryMembers,
	"inputs[]":                    entryMembers,
	"drift_check":                 {"replayed": {"replayed_keys"}},
	"migrations[]":                entryMembers,
	"missing_migrations[]":        entryMembers,
	"dirty_revision":              revisionMembers,
	"status":                      statusMembers,
	"status.migrations[]":         entryMembers,
	"status.missing_migrations[]": entryMembers,
	"status.dirty_revision":       revisionMembers,
}

func merged(sets ...members) members {
	all := make(members)
	for _, set := range sets {
		maps.Copy(all, set)
	}
	return all
}

// Strip returns doc, a JSON document, with its numeric version members left
// out at the paths [paths] names: a number or a list of numbers that spells
// migrations by their version, beside the member that carries the same
// identity as a key, and an empty list, which names nothing in either
// spelling. Member order and
// every other byte are kept, so the result reads as the document did with
// those members removed. A number with no key beside it stays, since a
// current version of 0 is the empty history, which no key spells; the one
// exception is checkpoint_version, which has no key member at all.
func Strip(doc []byte) ([]byte, error) {
	stripped, err := strip(doc, "")
	if err != nil {
		return nil, fmt.Errorf("strip the numeric versions of a migration document: %w", err)
	}
	return stripped, nil
}

// WriteIndented writes doc to w as [Strip] returns it, indented by two spaces
// and ended by a newline, as a json.Encoder with SetIndent("", "  ") writes a
// document.
func WriteIndented(w io.Writer, doc []byte) error {
	stripped, err := Strip(doc)
	if err != nil {
		return err
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, stripped, "", "  "); err != nil {
		return err
	}
	indented.WriteByte('\n')
	_, err = w.Write(indented.Bytes())
	return err
}

// strip edits raw, the value at path.
func strip(raw []byte, path string) ([]byte, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return trimmed, nil
	}
	switch trimmed[0] {
	case '{':
		return stripObject(trimmed, path)
	case '[':
		return stripArray(trimmed, path)
	default:
		return trimmed, nil
	}
}

func stripObject(raw []byte, path string) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	var names []string
	values := make(map[string][]byte)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("an object member is named by %v", token)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		stripped, err := strip(value, member(path, name))
		if err != nil {
			return nil, err
		}
		names = append(names, name)
		values[name] = stripped
	}
	var out bytes.Buffer
	out.WriteByte('{')
	written := 0
	for _, name := range names {
		if leftOut(paths[path], name, values) {
			continue
		}
		if written > 0 {
			out.WriteByte(',')
		}
		written++
		encodedName, err := json.Marshal(name)
		if err != nil {
			return nil, err
		}
		out.Write(encodedName)
		out.WriteByte(':')
		out.Write(values[name])
	}
	out.WriteByte('}')
	return out.Bytes(), nil
}

func stripArray(raw []byte, path string) ([]byte, error) {
	var elements []json.RawMessage
	if err := json.Unmarshal(raw, &elements); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.WriteByte('[')
	for index, element := range elements {
		stripped, err := strip(element, path+"[]")
		if err != nil {
			return nil, err
		}
		if index > 0 {
			out.WriteByte(',')
		}
		out.Write(stripped)
	}
	out.WriteByte(']')
	return out.Bytes(), nil
}

// member is the path of the member name of the object at path.
func member(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}

// leftOut reports that the member name of an object holding values is one
// of rules that [Strip] leaves out.
func leftOut(rules members, name string, values map[string][]byte) bool {
	keys, ok := rules[name]
	if !ok {
		return false
	}
	value := values[name]
	if string(value) == "[]" || string(value) == "null" {
		return true
	}
	if !numeric(value) {
		return false
	}
	if keys == nil {
		return true
	}
	for _, key := range keys {
		if _, ok := values[key]; ok {
			return true
		}
	}
	return false
}

// numeric reports that value is a number or a list of numbers.
func numeric(value []byte) bool {
	if number(value) {
		return true
	}
	var elements []json.RawMessage
	if len(value) == 0 || value[0] != '[' || json.Unmarshal(value, &elements) != nil {
		return false
	}
	for _, element := range elements {
		if !number(bytes.TrimSpace(element)) {
			return false
		}
	}
	return true
}

// number reports that value is a JSON number literal, which starts with a
// minus sign or a digit.
func number(value []byte) bool {
	return len(value) > 0 && (value[0] == '-' || (value[0] >= '0' && value[0] <= '9'))
}
