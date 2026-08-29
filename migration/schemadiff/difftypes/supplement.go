package difftypes

import (
	"reflect"
	"strings"
)

// The struct-tag key and value prefix a [SchemaDiff] list uses to declare that
// it qualifies another list rather than carrying changes of its own. The value
// after the prefix names the qualified list by its JSON name, the spelling
// every reader of a serialized diff already has.
const (
	supplementTagKey    = "ptah"
	supplementTagPrefix = "supplement="
)

// SupplementLists returns the [SchemaDiff] lists that qualify another list
// rather than carrying changes of their own, as a map from the supplement's
// JSON name to the JSON name of the list it qualifies.
//
// A supplement is ignored without a matching entry in the list it qualifies, so
// it changes nothing on its own: [SchemaDiff.HasChanges] is false for a diff
// carrying only one, and a prose report that printed it would print the same
// removed object a second time under a second heading. It stays on the wire,
// where a machine reading the diff needs the qualifier the base list cannot
// carry.
//
// The answer is derived from the fields' own tags rather than kept as a list
// beside them. A list beside them is a second answer that no compiler holds to
// the first: the one this replaces named two fields that had already been
// retired, and excluded two that the report went on printing anyway
// (stokaro/ptah#2476).
func SupplementLists() map[string]string {
	supplements := make(map[string]string)
	for field := range reflect.TypeFor[SchemaDiff]().Fields() {
		base, declared := supplementBase(field)
		if !declared {
			continue
		}
		supplements[jsonName(field)] = base
	}
	return supplements
}

// supplementBase returns the JSON name of the list field qualifies, and whether
// field declares itself a supplement at all.
func supplementBase(field reflect.StructField) (string, bool) {
	return strings.CutPrefix(field.Tag.Get(supplementTagKey), supplementTagPrefix)
}

// jsonName is the name a field serializes under: its JSON tag with any options
// removed, or the Go field name when it carries no tag.
func jsonName(field reflect.StructField) string {
	tag, tagged := field.Tag.Lookup("json")
	if !tagged {
		return field.Name
	}
	name, _, _ := strings.Cut(tag, ",")
	return name
}
