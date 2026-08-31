package schemamodel_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
)

// TestTargetNamesAreAllReported is the guard the reflection in
// targetNameMetadata exists for.
//
// A per-target name added to TargetNames and forgotten by the loss report would
// be lost exactly the way every attribute in that file already was, and
// silently: the report is the only thing standing between such an attribute and
// a cleanup that deletes the annotations holding it. This asserts every field of
// the struct has an authored spelling, so adding one without teaching
// TargetNameAttribute about it fails here rather than in somebody's contract.
func TestTargetNamesAreAllReported(t *testing.T) {
	fields := reflect.TypeFor[schemamodel.TargetNames]()

	for field := range fields.Fields() {
		t.Run(field.Name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(schemamodel.TargetNameAttribute(field.Name), qt.Not(qt.Equals), "")
		})
	}
}

// TestExportMetadataIn_ReportsEveryTargetName drives the same struct through the
// census, so a field with a spelling that is never read is caught as well.
func TestExportMetadataIn_ReportsEveryTargetName(t *testing.T) {
	fields := reflect.TypeFor[schemamodel.TargetNames]()

	for i := range fields.NumField() {
		t.Run(fields.Field(i).Name, func(t *testing.T) {
			c := qt.New(t)
			names := reflect.New(fields).Elem()
			names.Field(i).SetString("declared")
			database := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "U", Name: "users"}},
				Fields: []schemamodel.Field{{
					StructName: "U", Name: "a", Type: "TEXT",
					APINames: names.Interface().(schemamodel.TargetNames),
				}},
			}

			carried := schemamodel.ExportMetadataIn(database)

			c.Assert(carried, qt.HasLen, 1)
			c.Assert(carried[0].Attribute, qt.Equals, schemamodel.TargetNameAttribute(fields.Field(i).Name))
			c.Assert(carried[0].Value, qt.Equals, "declared")
		})
	}
}

// TestExportMetadataIn_ReportsNothingWithoutMetadata is the control for both:
// a census that returned an entry per column would satisfy them.
func TestExportMetadataIn_ReportsNothingWithoutMetadata(t *testing.T) {
	c := qt.New(t)

	carried := schemamodel.ExportMetadataIn(&schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "U", Name: "users"}},
		Fields: []schemamodel.Field{{StructName: "U", Name: "a", Type: "TEXT"}},
	})

	c.Assert(carried, qt.HasLen, 0)
}
