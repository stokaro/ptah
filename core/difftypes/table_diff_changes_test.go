package difftypes_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
)

// TestTableDiff_HasChangesReadsEveryField is the census that keeps
// [difftypes.TableDiff.HasChanges] from ignoring a field added later.
//
// The answer decides whether a comparison lists a table as modified, and
// whether a diff policy that took a table's column drops leaves the table in
// the plan. A change field read as "no change" drops that change from both, so
// each field has to be classified here, and the predicate has to agree with
// the classification field by field.
func TestTableDiff_HasChangesReadsEveryField(t *testing.T) {
	c := qt.New(t)

	classified := map[string]bool{
		"TableName":          false, // the table's name
		"ColumnsAdded":       true,
		"ColumnsRemoved":     true,
		"ColumnsModified":    true,
		"ConstraintsAdded":   true,
		"ConstraintsRemoved": true,
		"CommentChange":      true,
		"FeatureChanges":     true,
		"Desired":            false, // the table as declared, which a change is made to
		"Current":            false, // the table as observed before the change
		"ColumnKeyNames":     false, // goes with a column change ColumnsAdded or ColumnsModified carries
	}

	// One non-zero value per field kind. A field of a kind not listed here
	// fails the lookup below: a new shape needs a decision, not a default.
	nonZero := map[reflect.Kind]func(reflect.Type) reflect.Value{
		reflect.String: func(fieldType reflect.Type) reflect.Value {
			return reflect.ValueOf("users").Convert(fieldType)
		},
		reflect.Slice: func(fieldType reflect.Type) reflect.Value {
			return reflect.MakeSlice(fieldType, 1, 1)
		},
		reflect.Pointer: func(fieldType reflect.Type) reflect.Value {
			return reflect.New(fieldType.Elem())
		},
		reflect.Map: func(fieldType reflect.Type) reflect.Value {
			value := reflect.MakeMap(fieldType)
			value.SetMapIndex(reflect.ValueOf("email"), reflect.ValueOf("users_email_key"))
			return value
		},
		// Desired and Current are made non-zero by naming their table. A
		// struct field without one panics here rather than being built zero.
		reflect.Struct: func(fieldType reflect.Type) reflect.Value {
			value := reflect.New(fieldType).Elem()
			value.FieldByName("Table").FieldByName("Name").SetString("users")
			return value
		},
	}

	fields := reflect.VisibleFields(reflect.TypeFor[difftypes.TableDiff]())
	c.Assert(fields, qt.HasLen, len(classified))

	for _, field := range fields {
		want, known := classified[field.Name]
		c.Assert(known, qt.IsTrue, qt.Commentf(
			"TableDiff.%s is not classified: decide whether it is a change and record the answer here and in HasChanges",
			field.Name,
		))
		build, buildable := nonZero[field.Type.Kind()]
		c.Assert(buildable, qt.IsTrue, qt.Commentf("TableDiff.%s has kind %s, which this census cannot populate", field.Name, field.Type.Kind()))

		table := difftypes.TableDiff{}
		reflect.ValueOf(&table).Elem().FieldByName(field.Name).Set(build(field.Type))

		c.Assert(table.HasChanges(), qt.Equals, want, qt.Commentf("TableDiff.%s alone", field.Name))
	}
}

// TestTableDiff_HasChangesOfNil pins the nil answer.
func TestTableDiff_HasChangesOfNil(t *testing.T) {
	c := qt.New(t)
	var table *difftypes.TableDiff

	c.Assert(table.HasChanges(), qt.IsFalse)
}
