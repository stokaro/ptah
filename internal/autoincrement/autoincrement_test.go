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
