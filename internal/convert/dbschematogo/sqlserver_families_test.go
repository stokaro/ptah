package dbschematogo_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
)

// TestConvert_CarriesSynonyms pins that a synonym a read found reaches the IR,
// and that its target arrives in the spelling a declaration uses.
//
// `ptah schema inspect` described no synonym at all, in any format, while the
// reader found every one: the loss was in this conversion, between the read and
// the document, so nothing that renders from a hand-built schema could see it
// (stokaro/ptah#2001).
//
// The target is the second half of the claim. `DBSynonym.Target` is
// base_object_name exactly as the catalog records it, brackets included, and
// [goschema.Synonym.Target] is what will be emitted. Copying the catalog's form
// would put `[other].[dbo].[gauge]` in a document and render it again as a name
// with brackets inside it.
func TestConvert_CarriesSynonyms(t *testing.T) {
	tests := []struct {
		name       string
		synonym    dbschematypes.DBSynonym
		wantTarget string
	}{
		{
			name: "a local target",
			synonym: dbschematypes.DBSynonym{
				Name: "s_gauge", Schema: "dbo",
				Target:       "[dbo].[gauge]",
				TargetSchema: "dbo", TargetObject: "gauge",
			},
			wantTarget: "dbo.gauge",
		},
		{
			name: "another database",
			synonym: dbschematypes.DBSynonym{
				Name: "s_remote", Schema: "dbo",
				Target:         "[other].[dbo].[gauge]",
				TargetDatabase: "other", TargetSchema: "dbo", TargetObject: "gauge",
			},
			wantTarget: "other.dbo.gauge",
		},
		{
			name: "a linked server",
			synonym: dbschematypes.DBSynonym{
				Name: "s_linked", Schema: "dbo",
				Target:       "[srv].[other].[dbo].[gauge]",
				TargetServer: "srv", TargetDatabase: "other",
				TargetSchema: "dbo", TargetObject: "gauge",
			},
			wantTarget: "srv.other.dbo.gauge",
		},
		{
			// A row the reader could not parse still names something, and the
			// catalog's own form is better than an empty target: a declaration
			// with no target is not a synonym.
			name: "a target with no parsed parts",
			synonym: dbschematypes.DBSynonym{
				Name: "s_raw", Schema: "dbo", Target: "whatever_the_server_said",
			},
			wantTarget: "whatever_the_server_said",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted := dbschematogo.ConvertDBSchemaToGoSchema(&dbschematypes.DBSchema{
				Synonyms: []dbschematypes.DBSynonym{test.synonym},
			})

			c.Assert(converted.Synonyms, qt.HasLen, 1)
			c.Assert(converted.Synonyms[0].Target, qt.Equals, test.wantTarget)
			c.Assert(converted.Synonyms[0].Name, qt.Equals, test.synonym.Name)
			c.Assert(converted.Synonyms[0].Schema, qt.Equals, test.synonym.Schema)
		})
	}
}

// TestConvert_CarriesEveryPropertyScopeExceptTheOneItCannotWrite pins both
// halves of the extended-property conversion.
//
// The four addresses are four different statements, and the conversion has to
// keep them apart: a schema property passes level 0 alone, a table adds level 1,
// a column adds level 2, and a database property passes none.
//
// The exclusion is the second half and it is a decision, not an omission. A
// value SQL Server stores under a base type Ptah cannot write back must not
// become a declaration: the renderer emits an N” literal, so an int or a date
// would change type on the next apply, and CONVERT(NVARCHAR, …) on a date
// answers a locale-dependent rendering rather than the value. The comparator
// declines those in both directions, and describing one would undo that by
// asking for the string.
func TestConvert_CarriesEveryPropertyScopeExceptTheOneItCannotWrite(t *testing.T) {
	tests := []struct {
		name     string
		property dbschematypes.DBExtendedProperty
		want     []goschema.ExtendedProperty
	}{
		{
			name: "database scope",
			property: dbschematypes.DBExtendedProperty{
				Name: "ptah_db", Value: "on", ValueType: "nvarchar",
			},
			want: []goschema.ExtendedProperty{{Name: "ptah_db", Value: "on"}},
		},
		{
			name: "schema scope",
			property: dbschematypes.DBExtendedProperty{
				Name: "ptah_schema", Schema: "dbo", Value: "on", ValueType: "nvarchar",
			},
			want: []goschema.ExtendedProperty{{Name: "ptah_schema", Schema: "dbo", Value: "on"}},
		},
		{
			name: "table scope",
			property: dbschematypes.DBExtendedProperty{
				Name: "ptah_table", Schema: "dbo", Table: "gauge",
				Value: "on", ValueType: "nvarchar",
			},
			want: []goschema.ExtendedProperty{{
				Name: "ptah_table", Schema: "dbo", Table: "gauge", Value: "on",
			}},
		},
		{
			name: "column scope",
			property: dbschematypes.DBExtendedProperty{
				Name: "ptah_column", Schema: "dbo", Table: "gauge", Column: "title",
				Value: "on", ValueType: "nvarchar",
			},
			want: []goschema.ExtendedProperty{{
				Name: "ptah_column", Schema: "dbo", Table: "gauge", Column: "title", Value: "on",
			}},
		},
		{
			name: "a value no declaration could restore",
			property: dbschematypes.DBExtendedProperty{
				Name: "ptah_int", Schema: "dbo", Table: "gauge",
				Value: "42", ValueType: "int", ValueNotRepresentable: true,
			},
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted := dbschematogo.ConvertDBSchemaToGoSchema(&dbschematypes.DBSchema{
				ExtendedProperties: []dbschematypes.DBExtendedProperty{test.property},
			})

			c.Assert(converted.ExtendedProperties, qt.DeepEquals, test.want)
		})
	}
}

// TestConvert_DecidesEveryFamilyTheReadCanCarry is the guard the two families
// stokaro/ptah#2001 lost would have needed.
//
// A family added to [dbschematypes.DBSchema] joins this conversion only if
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
	irType := reflect.TypeFor[goschema.Database]()
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
	databaseType := reflect.TypeFor[dbschematypes.DBSchema]()
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
	"Schemas":            "Schemas",
	"Tables":             "Tables",
	"Enums":              "Enums",
	"Indexes":            "Indexes",
	"Constraints":        "Constraints",
	"Extensions":         "Extensions",
	"Functions":          "Functions",
	"Sequences":          "Sequences",
	"Domains":            "Domains",
	"Composites":         "CompositeTypes",
	"Ranges":             "Ranges",
	"Views":              "Views",
	"MatViews":           "MaterializedViews",
	"Synonyms":           "Synonyms",
	"ExtendedProperties": "ExtendedProperties",
	"Triggers":           "Triggers",
	"RLSPolicies":        "RLSPolicies",
	"Roles":              "Roles",
	"Grants":             "Grants",
}

// unconvertedFamilies are the read families that deliberately do not become
// declarations, with the reason each one does not.
var unconvertedFamilies = map[string]string{
	"ContinuousAggregates":      "TimescaleDB objects Ptah describes and does not manage; no declaration syntax names one",
	"Hypertables":               "the same, and the note the read emits is what says so",
	"ObjectOwners":              "ownership is read for diagnostics; no declaration carries it",
	"RoleMemberships":           "read for the role graph rather than as a declarable object",
	"RolesOutOfScope":           "a report about what the read did not cover, not an object",
	"UnregisteredVirtualTables": "a report about SQLite virtual tables no module registered",
}
