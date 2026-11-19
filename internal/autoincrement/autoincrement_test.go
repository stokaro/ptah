package autoincrement_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
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

// TestCounterType names the types that name a generated counter rather than
// a width.
func TestCounterType(t *testing.T) {
	tests := []struct {
		declared string
		want     bool
	}{
		{declared: "SERIAL", want: true},
		{declared: "bigserial", want: true},
		{declared: " Auto_Increment ", want: true},
		{declared: "identity", want: true},
		{declared: "NUMBER(10)", want: false},
		{declared: "IDENTITY(1,1)", want: false},
	}
	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(autoincrement.CounterType(test.declared), qt.Equals, test.want)
		})
	}
}

// TestGeneration names the identity generation each declaration is built with
// where every auto-increment column is an identity column.
func TestGeneration(t *testing.T) {
	tests := []struct {
		name  string
		field schemamodel.Field
		want  string
	}{
		{name: "ALWAYS", field: schemamodel.Field{Type: "NUMBER(10)", IdentityGeneration: "ALWAYS"}, want: "ALWAYS"},
		{name: "always, spaced", field: schemamodel.Field{Type: "NUMBER(10)", IdentityGeneration: " always "}, want: "ALWAYS"},
		{name: "BY DEFAULT", field: schemamodel.Field{Type: "NUMBER(10)", IdentityGeneration: "BY DEFAULT"}, want: "BY_DEFAULT"},
		{name: "an AutoInc flag", field: schemamodel.Field{Type: "NUMBER(10)", AutoInc: true}, want: "BY_DEFAULT"},
		{name: "a serial type", field: schemamodel.Field{Type: "BIGSERIAL"}, want: "BY_DEFAULT"},
		{name: "an AUTO_INCREMENT type", field: schemamodel.Field{Type: "AUTO_INCREMENT"}, want: "BY_DEFAULT"},
		{name: "a plain column", field: schemamodel.Field{Type: "NUMBER(10)"}, want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(autoincrement.Generation(test.field), qt.Equals, test.want)
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

// TestNotNull names the columns each fact holds NOT NULL. An identity
// generation is held by IdentityNotNull and AutoIncrementNotNull, an
// AutoInc flag and a counter type other than a serial type only by
// AutoIncrementNotNull, and a serial type by SerialNotNull and
// AutoIncrementNotNull.
func TestNotNull(t *testing.T) {
	identity := schemamodel.Field{Name: "id", Type: "BIGINT", Nullable: true, IdentityGeneration: "ALWAYS"}
	flagged := schemamodel.Field{Name: "id", Type: "INT", Nullable: true, AutoInc: true}
	serial := schemamodel.Field{Name: "id", Type: "serial", Nullable: true}
	counter := schemamodel.Field{Name: "id", Type: "IDENTITY", Nullable: true}
	plain := schemamodel.Field{Name: "id", Type: "INT", Nullable: true}
	tests := []struct {
		name    string
		field   schemamodel.Field
		columns descriptor.Columns
		want    bool
	}{
		{name: "an identity under IdentityNotNull", field: identity, columns: descriptor.Columns{IdentityNotNull: true}, want: true},
		{name: "an identity under AutoIncrementNotNull", field: identity, columns: descriptor.Columns{AutoIncrementNotNull: true}, want: true},
		{name: "an identity under SerialNotNull", field: identity, columns: descriptor.Columns{SerialNotNull: true}, want: false},
		{name: "a flag under IdentityNotNull", field: flagged, columns: descriptor.Columns{IdentityNotNull: true}, want: false},
		{name: "a flag under AutoIncrementNotNull", field: flagged, columns: descriptor.Columns{AutoIncrementNotNull: true}, want: true},
		{name: "a serial type under SerialNotNull", field: serial, columns: descriptor.Columns{SerialNotNull: true}, want: true},
		{name: "a serial type under AutoIncrementNotNull", field: serial, columns: descriptor.Columns{AutoIncrementNotNull: true}, want: true},
		{name: "an IDENTITY type under AutoIncrementNotNull", field: counter, columns: descriptor.Columns{AutoIncrementNotNull: true}, want: true},
		{name: "an IDENTITY type under SerialNotNull", field: counter, columns: descriptor.Columns{SerialNotNull: true}, want: false},
		{
			name: "a plain column under every fact", field: plain,
			columns: descriptor.Columns{IdentityNotNull: true, AutoIncrementNotNull: true, SerialNotNull: true}, want: false,
		},
		{name: "an identity without a fact", field: identity, columns: descriptor.Columns{}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(autoincrement.NotNull(test.field, test.columns), qt.Equals, test.want)
		})
	}
}
