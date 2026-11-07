package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// TestJSONHeldAsText holds which declarations are held as text, and on which engines. The
// renderer and the comparison ask the same function.
func TestJSONHeldAsText(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		dialect  string
		want     bool
	}{
		{"json on MariaDB", "json", "mariadb", true},
		{"upper case on MariaDB", " JSON ", "mariadb", true},
		{"another type on MariaDB", "text", "mariadb", false},
		{"json on MySQL, which has the type", "json", "mysql", false},
		{"json on PostgreSQL", "json", "postgres", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaprep.JSONHeldAsText(test.declared, test.dialect), qt.Equals, test.want)
		})
	}
}

// TestHoldsJSONText holds the shapes that leave only a CHECK to compare: what a catalog reads
// for such a column, and what a second document declares.
func TestHoldsJSONText(t *testing.T) {
	tests := []struct {
		name    string
		current string
		dialect string
		want    bool
	}{
		{"a read on MariaDB", "longtext", "mariadb", true},
		{"a second document on MariaDB", "json", "mariadb", true},
		{"another length on MariaDB", "text", "mariadb", false},
		{"json on MySQL", "json", "mysql", false},
		{"longtext on MySQL", "longtext", "mysql", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaprep.HoldsJSONText(test.current, test.dialect), qt.Equals, test.want)
		})
	}
}

// TestNeedsJSONCheck holds what the renderer writes: the CHECK the type implies, unless the
// column states one.
func TestNeedsJSONCheck(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		check    string
		dialect  string
		want     bool
	}{
		{"no CHECK", "json", "", "mariadb", true},
		{"a CHECK of something else", "json", "json_length(c) < 5", "mariadb", true},
		{"the CHECK already", "json", "JSON_VALID(c)", "mariadb", false},
		{"another type", "text", "", "mariadb", false},
		{"another engine", "json", "", "mysql", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaprep.NeedsJSONCheck(test.declared, test.check, test.dialect), qt.Equals, test.want)
		})
	}
}

// TestColumnCheckNames_AJSONColumnsCheckIsNamedAfterTheColumn holds the name the comparison
// looks for. MariaDB names it after the column, and a read of the column reports it there.
func TestColumnCheckNames_AJSONColumnsCheckIsNamedAfterTheColumn(t *testing.T) {
	fields := []schemamodel.Field{
		{StructName: "E", Name: "doc", Type: "json"},
		{StructName: "E", Name: "body", Type: "text"},
		{StructName: "E", Name: "checked", Type: "json", Check: "json_valid(checked)", CheckName: "e_checked"},
	}
	table := schemamodel.Table{StructName: "E", Name: "e"}
	tests := []struct {
		dialect string
		want    map[string]string
	}{
		{"mariadb", map[string]string{"doc": "doc", "checked": "e_checked"}},
		{"mysql", map[string]string{"checked": "e_checked"}},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaprep.ColumnCheckNames(table, fields, nil, test.dialect), qt.DeepEquals, test.want)
		})
	}
}

// TestWithJSONCheck_AColumnThatStatesACheckKeepsIt holds the fields the implied CHECK does not
// reach, beside the one it does.
func TestWithJSONCheck_AColumnThatStatesACheckKeepsIt(t *testing.T) {
	tests := []struct {
		name      string
		field     schemamodel.Field
		dialect   string
		wantCheck string
	}{
		{"a JSON column", schemamodel.Field{Name: "c", Type: "json"}, "mariadb", "json_valid(`c`)"},
		{"a JSON column that states one", schemamodel.Field{Name: "c", Type: "json", Check: "json_length(c) < 5"}, "mariadb", "json_length(c) < 5"},
		{"another type", schemamodel.Field{Name: "c", Type: "text"}, "mariadb", ""},
		{"another engine", schemamodel.Field{Name: "c", Type: "json"}, "mysql", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaprep.WithJSONCheck(test.field, test.dialect).Check, qt.Equals, test.wantCheck)
		})
	}
}
