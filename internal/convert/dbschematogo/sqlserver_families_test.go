package dbschematogo_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/synonym"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvert_CarriesSynonyms pins that a synonym a read found reaches the IR
// as a declaration of the same alias and target.
//
// `ptah schema inspect` described no synonym at all, in any format, while the
// reader found every one: the loss was in this conversion, between the read and
// the document, so nothing that renders from a hand-built schema could see it
// (stokaro/ptah#2001). The reader keeps the target in the spelling a
// declaration uses, so the conversion carries it unchanged.
func TestConvert_CarriesSynonyms(t *testing.T) {
	c := qt.New(t)
	observed := synonym.ObservedSynonym{Synonym: synonym.Synonym{Name: "s_linked", Schema: "dbo", Target: "srv..dbo.gauge"}}

	converted := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(synonym.ObservedObject(observed))),
		FeatureCoverage: must.Must(synonym.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}, "sqlserver", must.Must(builtin.New())))

	c.Assert(must.Must(converted.FeatureObjects.All()), qt.DeepEquals, []schemaext.Object{
		synonym.DesiredObject(synonym.DesiredSynonym{Synonym: observed.Synonym}),
	})
}

// TestConvert_DecidesEveryFamilyTheReadCanCarry is the guard the two families
// stokaro/ptah#2001 lost would have needed.
//
// A family added to [catalog.Database] joins this conversion only if
// somebody remembers, and forgetting is silent: the read finds the objects, the
// IR does not carry them, and `schema inspect` describes a database that is
// missing part of itself.
//
// So every slice field of the read's shape is either converted -- named in
// [convertedFamilies] with the IR field it becomes -- or exempt, with the
// reason written down. A new family belongs in one list or the other, and this
// fails until it is in one.
func TestConvert_DecidesEveryFamilyTheReadCanCarry(t *testing.T) {
	for _, field := range readSliceFields() {
		t.Run(field, func(t *testing.T) {
			c := qt.New(t)
			_, converted := convertedFamilies[field]
			_, exempt := unconvertedFamilies[field]

			c.Assert(converted, qt.Not(qt.Equals), exempt,
				qt.Commentf("%s is in neither list, or in both", field))
		})
	}
}

// TestConvert_NamesAnIRFieldThatExists keeps [convertedFamilies] honest: a
// mapping to a field the IR does not have would satisfy the guard above while
// naming nothing.
func TestConvert_NamesAnIRFieldThatExists(t *testing.T) {
	irType := reflect.TypeFor[schemamodel.Database]()
	for _, irField := range convertedFamilies {
		t.Run(irField, func(t *testing.T) {
			c := qt.New(t)
			_, found := irType.FieldByName(irField)
			c.Assert(found, qt.IsTrue)
		})
	}
}

// readSliceFields is every object family the read's shape carries, derived from
// the struct rather than listed here.
func readSliceFields() []string {
	databaseType := reflect.TypeFor[catalog.Database]()
	fields := make([]string, 0, databaseType.NumField())
	for field := range databaseType.Fields() {
		if field.Type.Kind() != reflect.Slice {
			continue
		}
		fields = append(fields, field.Name)
	}
	return fields
}

// convertedFamilies maps each read family to the IR field it becomes.
var convertedFamilies = map[string]string{
	"Schemas":           "Schemas",
	"Tables":            "Tables",
	"Enums":             "Enums",
	"Indexes":           "Indexes",
	"Constraints":       "Constraints",
	"Extensions":        "Extensions",
	"Functions":         "Functions",
	"Sequences":         "Sequences",
	"Domains":           "Domains",
	"Composites":        "CompositeTypes",
	"Ranges":            "Ranges",
	"Views":             "Views",
	"MatViews":          "MaterializedViews",
	"Triggers":          "Triggers",
	"RLSPolicies":       "RLSPolicies",
	"Roles":             "Roles",
	"Grants":            "Grants",
	"DefaultPrivileges": "DefaultPrivileges",
}

// unconvertedFamilies are the read families that deliberately do not become
// declarations, with the reason each one does not.
var unconvertedFamilies = map[string]string{
	"ObjectOwners":                 "ownership is read for diagnostics; no declaration carries it",
	"RoleMemberships":              "read for the role graph rather than as a declarable object",
	"RolesOutOfScope":              "a report about what the read did not cover, not an object",
	"UnregisteredVirtualTables":    "a report about SQLite virtual tables no module registered",
	"UndescribedDefaultPrivileges": "a report about default privileges no declaration can carry: without IN SCHEMA, or FOR ALL ROLES",
}
