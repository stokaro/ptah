package difftypes_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/schemadiff/difftypes"
)

// TestSupplementListsNameALiveWireCategory holds every declared supplement to
// the three properties that make the declaration readable rather than decorative:
// the name it is keyed by is the one the diff actually serializes under, the
// list it names as its base exists and is a list too, and that base is not
// itself a supplement.
//
// The wire assertion is the one worth stating out loud. A key derived from a
// struct tag agrees with any other derivation from the same tag, so deriving it
// twice proves nothing; marshaling the diff asks the encoder, which is what a
// reader of `--format json` actually meets.
func TestSupplementListsNameALiveWireCategory(t *testing.T) {
	c := qt.New(t)

	supplements := difftypes.SupplementLists()
	c.Assert(supplements, qt.Not(qt.HasLen), 0)

	for name, base := range supplements {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)

			field, found := diffFieldByJSONName(name)
			c.Assert(found, qt.IsTrue, qt.Commentf("no SchemaDiff field serializes as %q", name))
			c.Assert(marshalDiffWithOnly(c, field), qt.Contains, `"`+name+`":`)

			baseField, found := diffFieldByJSONName(base)
			c.Assert(found, qt.IsTrue, qt.Commentf("%s names %q as the list it qualifies, and no field serializes as that", name, base))
			c.Assert(baseField.Type.Kind(), qt.Equals, reflect.Slice)

			_, baseIsSupplement := supplements[base]
			c.Assert(baseIsSupplement, qt.IsFalse, qt.Commentf("%s qualifies %q, which qualifies something else in turn", name, base))
		})
	}
}

// TestSupplementListsChangeNothingOnTheirOwn asserts the property that makes
// suppressing a supplement from a report correct: a diff carrying only one is
// not a difference. Were HasChanges to answer true here, the report would owe
// the operator the category it declines to print.
func TestSupplementListsChangeNothingOnTheirOwn(t *testing.T) {
	c := qt.New(t)

	supplements := difftypes.SupplementLists()
	c.Assert(supplements, qt.Not(qt.HasLen), 0)

	for name := range supplements {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)

			field, found := diffFieldByJSONName(name)
			c.Assert(found, qt.IsTrue)
			c.Assert(diffWithOnly(field).HasChanges(), qt.IsFalse)
		})
	}
}

// TestSupplementListsExcludeAnUntaggedList is the control for the two above: a
// list that declares nothing is not reported as a supplement, so a passing run
// of those tests is not the tag reader answering yes to everything.
func TestSupplementListsExcludeAnUntaggedList(t *testing.T) {
	c := qt.New(t)

	supplements := difftypes.SupplementLists()

	_, tablesAdded := supplements["tables_added"]
	c.Assert(tablesAdded, qt.IsFalse)
	_, indexesRemoved := supplements["indexes_removed"]
	c.Assert(indexesRemoved, qt.IsFalse)
	c.Assert(len(supplements) < reflect.TypeFor[difftypes.SchemaDiff]().NumField(), qt.IsTrue)
}

// diffFieldByJSONName returns the SchemaDiff field that serializes under name.
func diffFieldByJSONName(name string) (reflect.StructField, bool) {
	for field := range reflect.TypeFor[difftypes.SchemaDiff]().Fields() {
		tag, tagged := field.Tag.Lookup("json")
		serialized, _, _ := strings.Cut(tag, ",")
		if !tagged {
			serialized = field.Name
		}
		if serialized == name {
			return field, true
		}
	}
	return reflect.StructField{}, false
}

// diffWithOnly builds a diff whose one non-empty list is field, holding a
// single zero-valued element. The element's contents do not matter to any
// property here, and a zero value keeps the fixture from having to know each
// element type.
func diffWithOnly(field reflect.StructField) *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}
	reflect.ValueOf(diff).Elem().FieldByIndex(field.Index).Set(reflect.MakeSlice(field.Type, 1, 1))
	return diff
}

func marshalDiffWithOnly(c *qt.C, field reflect.StructField) string {
	encoded, err := json.Marshal(diffWithOnly(field))
	c.Assert(err, qt.IsNil)
	return string(encoded)
}
