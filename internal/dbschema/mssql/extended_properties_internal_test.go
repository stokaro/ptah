package mssql

// White-box testing required: the extended-property read decides which
// sql_variant base types it can carry, and the exported read returns the
// verdict without saying which branch produced it.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// TestRepresentableExtendedPropertyType_IsTheRendererQuestion pins which base
// types survive a round trip.
//
// The renderer writes an N” literal, so a value stored under any other base
// type would come back with a different type. The list is the four character
// types and nothing else -- and the empty string is not one of them, because
// it is what SQL_VARIANT_PROPERTY answers for a value it cannot describe, and
// treating "no answer" as a character type would write a literal over a value
// nobody read.
func TestRepresentableExtendedPropertyType_IsTheRendererQuestion(t *testing.T) {
	tests := []struct {
		name     string
		baseType string
		want     bool
	}{
		{name: "nvarchar", baseType: "nvarchar", want: true},
		{name: "varchar", baseType: "varchar", want: true},
		{name: "nchar", baseType: "nchar", want: true},
		{name: "char", baseType: "char", want: true},
		{name: "int", baseType: "int", want: false},
		{name: "date", baseType: "date", want: false},
		{name: "bit", baseType: "bit", want: false},
		{name: "uniqueidentifier", baseType: "uniqueidentifier", want: false},
		{name: "no answer at all", baseType: "", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(representableExtendedPropertyType(test.baseType), qt.Equals, test.want)
		})
	}
}

// TestReadExtendedProperties_CarriesTheAddressAndDeclinesTheValueItCannotWrite
// reads both catalog arms through a scripted server.
func TestReadExtendedProperties_CarriesTheAddressAndDeclinesTheValueItCannotWrite(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringExtendedProperties)
	reader := NewSQLServerReader(db.SQL, "app")

	properties, err := reader.readExtendedProperties(t.Context())
	c.Assert(err, qt.IsNil)

	c.Assert(properties, qt.DeepEquals, []types.DBExtendedProperty{
		{Name: "ptah_db_prop", Value: "database scope", ValueType: "nvarchar"},
		{Name: "ptah_schema_prop", Schema: "app", Value: "schema scope", ValueType: "nvarchar"},
		{Name: "ptah_flag", Schema: "app", Table: "docs", Value: "enabled", ValueType: "nvarchar"},
		{
			Name: "ptah_int", Schema: "app", Table: "docs",
			ValueType: "int", ValueNotRepresentable: true,
		},
		{
			Name: "ptah_col", Schema: "app", Table: "docs", Column: "title",
			Value: "sensitive", ValueType: "nvarchar",
		},
	})
}

// TestExtendedPropertyQuery_ScopesEveryArmButTheDatabaseOne pins the one arm
// the schema predicate must not reach.
//
// A database-scoped property is in no schema, so narrowing the read to one
// cannot exclude it -- and a description that dropped it while the declaration
// still carried one would plan sp_dropextendedproperty for a property the
// operator declared. The predicate placeholder appears twice, once for each of
// the other two arms.
func TestExtendedPropertyQuery_ScopesEveryArmButTheDatabaseOne(t *testing.T) {
	c := qt.New(t)

	c.Assert(strings.Count(extendedPropertyQuery, schemaPredicatePlaceholder), qt.Equals, 2)
	databaseArm, _, found := strings.Cut(extendedPropertyQuery, "UNION ALL")
	c.Assert(found, qt.IsTrue)
	c.Assert(databaseArm, qt.Contains, "ep.class = 0")
	c.Assert(databaseArm, qt.Not(qt.Contains), schemaPredicatePlaceholder)
}

// TestExtendedPropertyQuery_AsksForTheThingsThatDecideTheAnswer holds the
// three exclusions and the two conversions in the statement itself.
//
// They are asserted on the query rather than on rows because a scripted server
// answers whatever it is told to: a query that stopped excluding
// MS_Description would return the same fixture rows and pass.
func TestExtendedPropertyQuery_AsksForTheThingsThatDecideTheAnswer(t *testing.T) {
	tests := []struct {
		name     string
		fragment string
	}{
		{name: "the database arm", fragment: "ep.class = 0"},
		{name: "the schema arm", fragment: "ep.class = 3"},
		{name: "the object arm", fragment: "ep.class = 1"},
		{name: "the comment Ptah already models", fragment: "ep.name <> N'MS_Description'"},
		{name: "only tables, not every class 1 object", fragment: "JOIN sys.tables AS t ON t.object_id = ep.major_id"},
		{name: "the column, when there is one", fragment: "AND c.column_id = ep.minor_id"},
		{name: "the stored base type", fragment: "SQL_VARIANT_PROPERTY(ep.value, 'BaseType')"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(extendedPropertyQuery, qt.Contains, test.fragment)
		})
	}
}

// answeringExtendedProperties answers the two arms of the union with the rows
// each one projects.
//
// It answers per projection rather than per call: the union is one statement,
// so the fake has to return the six columns the reader scans, and a query that
// stopped selecting the base type would scan six values into five and fail
// here rather than being handed the same rows.
func answeringExtendedProperties(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if !strings.Contains(strings.ToLower(query), "sys.extended_properties") {
		return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
	}
	return dbtest.QueryResult{
		Columns: []string{"schema_name", "table_name", "column_name", "name", "value", "base_type"},
		Rows: [][]driver.Value{
			{"", "", "", "ptah_db_prop", "database scope", "nvarchar"},
			{"app", "", "", "ptah_schema_prop", "schema scope", "nvarchar"},
			{"app", "docs", "", "ptah_flag", "enabled", "nvarchar"},
			{"app", "docs", "", "ptah_int", "42", "int"},
			{"app", "docs", "title", "ptah_col", "sensitive", "nvarchar"},
		},
	}, nil
}
