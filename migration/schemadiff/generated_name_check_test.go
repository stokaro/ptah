package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// thingsWithCheck declares APP.THINGS, in the shape the SQL reader gives
// `CREATE TABLE things (id NUMBER(10) PRIMARY KEY, CHECK (<condition>))`
// on Oracle, with the CHECK named name, or unnamed when name is empty.
func thingsWithCheck(name, condition string) *schemamodel.Database {
	desired := declaredTable("things")
	desired.Constraints = []schemamodel.Constraint{{
		StructName: "things", Name: name, Type: "CHECK", Table: "things", CheckExpression: condition,
	}}
	return desired
}

// thingsHolding is the read of APP.THINGS holding one CHECK.
func thingsHolding(check catalog.Constraint) *catalog.Database {
	check.TableName, check.Schema, check.Type = "THINGS", "APP", "CHECK"
	check.ColumnNames = []string{"ID"}
	check.ColumnName = "ID"
	return &catalog.Database{StoredNames: true, Tables: []catalog.Table{heldTable("THINGS")}, Constraints: []catalog.Constraint{check}}
}

// TestCompare_GeneratedNameCheck_HappyPath compares an unnamed declared CHECK
// with the CHECK Oracle created for it, which the server named SYS_C008933 and
// whose condition it keeps as written. The two are one CHECK, so nothing is
// planned, whatever case and spacing the condition was written in. Keyed by
// name alone, the declared CHECK is added again and SYS_C008933 dropped on
// every run.
func TestCompare_GeneratedNameCheck_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		held     string
	}{
		{name: "the same text", declared: "id > 0", held: "id > 0"},
		{name: "another case and spacing", declared: "ID>0", held: "id > 0"},
		{name: "outer parentheses", declared: "(id > 0)", held: "id > 0"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), thingsWithCheck("", test.declared),
				thingsHolding(catalog.Constraint{Name: "SYS_C008933", GeneratedName: true, CheckClause: new(test.held)}),
				oracleRead(), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompare_GeneratedNameCheck_NotPaired holds the controls: a condition
// that differs is a changed CHECK, a CHECK the author named is not paired with
// one the server named, and a CHECK the read does not mark as server-named is
// compared by its name, as any named constraint is.
func TestCompare_GeneratedNameCheck_NotPaired(t *testing.T) {
	tests := []struct {
		name     string
		declared *schemamodel.Database
		held     catalog.Constraint
		added    []string
		removed  []string
	}{
		{
			name:     "another condition",
			declared: thingsWithCheck("", "id > 1"),
			held:     catalog.Constraint{Name: "SYS_C008933", GeneratedName: true, CheckClause: new("id > 0")},
			added:    []string{""},
			removed:  []string{"SYS_C008933"},
		},
		{
			name:     "a declared name",
			declared: thingsWithCheck("id_pos", "id > 0"),
			held:     catalog.Constraint{Name: "SYS_C008933", GeneratedName: true, CheckClause: new("id > 0")},
			added:    []string{"id_pos"},
			removed:  []string{"SYS_C008933"},
		},
		{
			name:     "a name the author chose",
			declared: thingsWithCheck("", "id > 0"),
			held:     catalog.Constraint{Name: "SYS_C008933", CheckClause: new("id > 0")},
			added:    []string{""},
			removed:  []string{"SYS_C008933"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), test.declared, thingsHolding(test.held),
				oracleRead(), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ConstraintsAdded.Names(), qt.DeepEquals, test.added)
			c.Assert(diff.ConstraintsRemoved.Names(), qt.DeepEquals, test.removed)
		})
	}
}
