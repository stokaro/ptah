package autoincrement_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/autoincrement"
)

// TestDeclared names the declarations that build an auto-increment column.
func TestDeclared(t *testing.T) {
	tests := []struct {
		name  string
		field schemamodel.Field
		want  bool
	}{
		{name: "AutoInc", field: schemamodel.Field{Name: "id", Type: "INT", AutoInc: true}, want: true},
		{name: "an identity generation", field: schemamodel.Field{Name: "id", Type: "BIGINT", IdentityGeneration: "BY DEFAULT"}, want: true},
		{name: "neither", field: schemamodel.Field{Name: "id", Type: "INT", Primary: true}, want: false},
		{name: "a SERIAL type alone", field: schemamodel.Field{Name: "id", Type: "SERIAL"}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(autoincrement.Declared(test.field), qt.Equals, test.want)
		})
	}
}

// TestSerialType names PostgreSQL's serial types.
func TestSerialType(t *testing.T) {
	tests := []struct {
		declared string
		want     bool
	}{
		{declared: "SERIAL", want: true},
		{declared: "bigserial", want: true},
		{declared: " SmallSerial ", want: true},
		{declared: "INT", want: false},
		{declared: "SERIAL4", want: false},
	}
	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(autoincrement.SerialType(test.declared), qt.Equals, test.want)
		})
	}
}

// TestDeclaredOn names the declarations each target builds auto-increment. A
// serial type is one on MySQL, MariaDB and SQL Server, which write it as their
// own auto-increment integer, unless the field spells the target's type itself.
func TestDeclaredOn(t *testing.T) {
	tests := []struct {
		name    string
		field   schemamodel.Field
		dialect string
		want    bool
	}{
		{name: "SERIAL on MySQL", field: schemamodel.Field{Type: "SERIAL"}, dialect: "mysql", want: true},
		{name: "BIGSERIAL on MariaDB", field: schemamodel.Field{Type: "BIGSERIAL"}, dialect: "mariadb", want: true},
		{name: "SMALLSERIAL on SQL Server", field: schemamodel.Field{Type: "smallserial"}, dialect: "mssql", want: true},
		{name: "AutoInc anywhere", field: schemamodel.Field{Type: "INT", AutoInc: true}, dialect: "sqlite", want: true},
		{name: "SERIAL on SQLite, the rowid", field: schemamodel.Field{Type: "SERIAL"}, dialect: "sqlite", want: false},
		{name: "SERIAL on PostgreSQL, a sequence", field: schemamodel.Field{Type: "SERIAL"}, dialect: "postgres", want: false},
		{
			name:    "SERIAL whose MySQL type is its own",
			field:   schemamodel.Field{Type: "SERIAL", Overrides: map[string]map[string]string{"mysql": {"type": "INT AUTO_INCREMENT"}}},
			dialect: "mysql", want: false,
		},
		{
			name:    "SERIAL whose override is for another target",
			field:   schemamodel.Field{Type: "SERIAL", Overrides: map[string]map[string]string{"postgres": {"type": "INTEGER"}}},
			dialect: "mysql", want: true,
		},
		{name: "SERIAL written as raw SQL", field: schemamodel.Field{Type: "SERIAL", TypeRawSQL: true}, dialect: "mysql", want: false},
		{name: "a plain integer", field: schemamodel.Field{Type: "INT"}, dialect: "mysql", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(autoincrement.DeclaredOn(test.field, test.dialect), qt.Equals, test.want)
		})
	}
}
