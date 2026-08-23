package schemafile

// White-box testing required: the merge is unexported, and the exported loader
// returns the same shape whether a family was merged or silently dropped.

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
)

// TestAppendDatabase_MergesEveryObjectFamily is the guard the two families
// stokaro/ptah#1999 lost would have needed.
//
// [appendDatabase] is a list of twenty-four assignments, and a family added to
// [goschema.Database] joins it only if somebody remembers. Two did not, and the
// failure was silent in the direction that matters: the objects were dropped
// from the DESIRED side, so `schema apply` answered "no changes" for a
// declaration it had thrown away.
//
// So the expectation is derived from the struct, not written out. A field is
// merged, or it is named below as one that must not be.
func TestAppendDatabase_MergesEveryObjectFamily(t *testing.T) {
	for _, field := range mergeableDatabaseFields() {
		t.Run(field, func(t *testing.T) {
			c := qt.New(t)
			source := &goschema.Database{}
			seedSliceField(c, source, field)

			merged := &goschema.Database{}
			appendDatabase(merged, source)

			c.Assert(sliceFieldLen(merged, field), qt.Equals, 1,
				qt.Commentf("appendDatabase drops %s, so a document declaring one is read as declaring none", field))
		})
	}
}

// TestAppendDatabase_UnionsTheCoverageRecord pins the one field that is not a
// slice of objects and is not appended either.
//
// Several files loaded together are one description, and it describes only what
// all of them together describe. Union, never intersection: a limit one file
// declares is a limit of the whole (stokaro/ptah#1276).
func TestAppendDatabase_UnionsTheCoverageRecord(t *testing.T) {
	c := qt.New(t)
	merged := &goschema.Database{}

	limited := coverage.Set{}.With(coverage.Object{
		Kind:       coverage.VirtualTable,
		Reason:     coverage.Unsupported,
		Provenance: coverage.DerivedFromFact,
	})

	appendDatabase(merged, &goschema.Database{NotDescribed: limited})
	appendDatabase(merged, &goschema.Database{})

	c.Assert(merged.NotDescribed.Describes(coverage.VirtualTable), qt.IsFalse)
}

// nonMergeableDatabaseFields are the slice fields [appendDatabase] must NOT
// append, with the reason each one is exempt.
//
// It is empty, and that is the claim: every object family a document can
// declare belongs in the merge. The list exists so that adding an exemption is
// a decision somebody writes down rather than a line nobody notices is missing.
var nonMergeableDatabaseFields = make(map[string]string)

// mergeableDatabaseFields is every slice field of the IR, minus the exemptions.
func mergeableDatabaseFields() []string {
	databaseType := reflect.TypeFor[goschema.Database]()
	fields := make([]string, 0, databaseType.NumField())
	for field := range databaseType.Fields() {
		if field.Type.Kind() != reflect.Slice {
			continue
		}
		if _, exempt := nonMergeableDatabaseFields[field.Name]; exempt {
			continue
		}
		fields = append(fields, field.Name)
	}
	return fields
}

// seedSliceField puts exactly one zero element into the named slice field, so
// the assertion is about the merge rather than about any family's contents.
func seedSliceField(c *qt.C, database *goschema.Database, field string) {
	c.Helper()
	value := reflect.ValueOf(database).Elem().FieldByName(field)
	c.Assert(value.IsValid(), qt.IsTrue)
	value.Set(reflect.MakeSlice(value.Type(), 1, 1))
}

func sliceFieldLen(database *goschema.Database, field string) int {
	return reflect.ValueOf(database).Elem().FieldByName(field).Len()
}
