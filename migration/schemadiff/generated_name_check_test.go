package schemadiff_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
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
		{
			name:     "a deferred CHECK",
			declared: thingsWithCheck("", "id > 0"),
			held:     catalog.Constraint{Name: "SYS_C008933", GeneratedName: true, Deferrable: true, Initially: "deferred", CheckClause: new("id > 0")},
			added:    []string{""},
			removed:  []string{"SYS_C008933"},
		},
		{
			name:     "a disabled CHECK of another condition",
			declared: thingsWithCheck("", "id > 1"),
			held:     catalog.Constraint{Name: "SYS_C008933", GeneratedName: true, Disabled: true, CheckClause: new("id > 0")},
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

// TestCompare_GeneratedNameCheck_Disabled compares an unnamed declared CHECK
// with the CHECK Oracle named SYS_C008933 for it and keeps disabled, or
// enabled without validating the rows the table held. The two are one CHECK
// in another state, so the plan enables it and adds and drops nothing. Left
// unpaired, the declared CHECK is added again, which the rows the disabled
// CHECK let in refuse with ORA-02293.
func TestCompare_GeneratedNameCheck_Disabled(t *testing.T) {
	tests := []struct {
		name string
		held catalog.Constraint
	}{
		{name: "disabled", held: catalog.Constraint{Disabled: true, NotValid: true}},
		{name: "enabled without validating", held: catalog.Constraint{NotValid: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			held := test.held
			held.Name, held.GeneratedName, held.CheckClause = "SYS_C008933", true, new("id > 0")

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), thingsWithCheck("", "id > 0"), thingsHolding(held),
				oracleRead(), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
			c.Assert(diff.ConstraintsValidated, qt.DeepEquals, []difftypes.ConstraintValidation{{TableName: "APP.THINGS", Name: "SYS_C008933"}})
		})
	}
}

// TestCompare_CheckDeferralAndEnablement compares a declared CHECK named
// id_pos with the CHECK of that name an Oracle account holds. A CHECK the
// server checks at commit is another constraint from the one declared, since
// Oracle fixes a CHECK's deferrability when it creates it, so the plan
// replaces it; deferral declared as the server holds it plans nothing. A
// CHECK the server keeps disabled, or enabled without validating its rows, is
// the declared CHECK in another state, and the plan enables it. Compared by
// condition alone, every row plans nothing.
func TestCompare_CheckDeferralAndEnablement(t *testing.T) {
	tests := []struct {
		name     string
		declared schemamodel.Constraint
		held     catalog.Constraint
		// added, removed and validated name the CHECKs the plan adds,
		// drops and enables, joined by commas.
		added     string
		removed   string
		validated string
	}{
		{
			name:     "a deferred CHECK for a plain one",
			declared: schemamodel.Constraint{},
			held:     catalog.Constraint{Deferrable: true, Initially: "deferred"},
			added:    "id_pos",
			removed:  "ID_POS",
		},
		{
			name:     "a deferrable CHECK for a plain one",
			declared: schemamodel.Constraint{},
			held:     catalog.Constraint{Deferrable: true, Initially: "immediate"},
			added:    "id_pos",
			removed:  "ID_POS",
		},
		{
			name:     "a plain CHECK for a deferred one",
			declared: schemamodel.Constraint{Deferrable: true, Initially: "deferred"},
			held:     catalog.Constraint{},
			added:    "id_pos",
			removed:  "ID_POS",
		},
		{
			name:     "another timing",
			declared: schemamodel.Constraint{Deferrable: true, Initially: "deferred"},
			held:     catalog.Constraint{Deferrable: true, Initially: "immediate"},
			added:    "id_pos",
			removed:  "ID_POS",
		},
		{
			name:      "a disabled CHECK",
			declared:  schemamodel.Constraint{},
			held:      catalog.Constraint{Disabled: true, NotValid: true},
			validated: "ID_POS",
		},
		{
			name:      "a CHECK enabled without validating",
			declared:  schemamodel.Constraint{},
			held:      catalog.Constraint{NotValid: true},
			validated: "ID_POS",
		},
		{
			name:     "a disabled deferred CHECK for a plain one",
			declared: schemamodel.Constraint{},
			held:     catalog.Constraint{Disabled: true, NotValid: true, Deferrable: true, Initially: "deferred"},
			added:    "id_pos",
			removed:  "ID_POS",
		},
		{
			name:     "the same deferral",
			declared: schemamodel.Constraint{Deferrable: true, Initially: "deferred"},
			held:     catalog.Constraint{Deferrable: true, Initially: "deferred"},
		},
		{
			name:     "neither defers",
			declared: schemamodel.Constraint{},
			held:     catalog.Constraint{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := thingsWithCheck("id_pos", "id > 0")
			declared.Constraints[0].Deferrable, declared.Constraints[0].Initially = test.declared.Deferrable, test.declared.Initially
			held := test.held
			held.Name, held.CheckClause = "ID_POS", new("id > 0")

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, thingsHolding(held),
				oracleRead(), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(diff.ConstraintsAdded.Names(), ","), qt.Equals, test.added)
			c.Assert(strings.Join(diff.ConstraintsRemoved.Names(), ","), qt.Equals, test.removed)
			validated := make([]string, 0, len(diff.ConstraintsValidated))
			for _, validation := range diff.ConstraintsValidated {
				validated = append(validated, validation.Name)
			}
			c.Assert(strings.Join(validated, ","), qt.Equals, test.validated)
		})
	}
}
