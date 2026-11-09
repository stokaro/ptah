package sqlservertype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlservertype"
)

// TestMap holds the names the renderer rewrites, and the names it leaves alone.
func TestMap(t *testing.T) {
	tests := []struct {
		declared string
		want     string
	}{
		{"INTEGER", "INT"},
		{"int4", "INT"},
		{"serial", "INT"},
		{"BIGSERIAL", "BIGINT"},
		{"boolean", "BIT"},
		{"BOOL", "BIT"},
		{"text", "NVARCHAR(MAX)"},
		{"CITEXT", "NVARCHAR(MAX)"},
		{"varchar", "NVARCHAR"},
		{"varchar(50)", "NVARCHAR(50)"},
		{"character varying(50)", "NVARCHAR(50)"},
		{" VARCHAR(MAX) ", "NVARCHAR(MAX) "},
		{"char", "NCHAR"},
		{"char(5)", "NCHAR(5)"},
		{"CHARACTER(5)", "NCHAR(5)"},
		{"bytea", "VARBINARY(MAX)"},
		{"BLOB", "VARBINARY(MAX)"},
		{"double precision", "FLOAT"},
		{"timestamptz", "DATETIMEOFFSET"},
		{"timestamptz(3)", "DATETIMEOFFSET(3)"},
		{"timestamp with time zone", "DATETIMEOFFSET"},
		{"timestamp(3) with time zone", "DATETIMEOFFSET(3)"},
		{"TIMESTAMP ( 0 ) WITH TIME ZONE", "DATETIMEOFFSET(0)"},
		{"TIMESTAMP ( 3 ) WITH  TIME ZONE", "DATETIMEOFFSET(3)"},
		{"timestamp", "DATETIME2"},
		{"timestamp(3)", "DATETIME2(3)"},
		{"timestamp without time zone", "DATETIME2"},
		{"timestamp(3) without time zone", "DATETIME2(3)"},
		{"timestamp(3) with nonsense", "timestamp(3) with nonsense"},
		{"int", "int"},
		{"nvarchar(50)", "nvarchar(50)"},
		{"datetime2(3)", "datetime2(3)"},
		{"float", "float"},
		{"decimal(10,2)", "decimal(10,2)"},
	}
	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlservertype.Map(test.declared), qt.Equals, test.want)
		})
	}
}

// TestDeclaration_AVerbatimTypeIsWrittenAsItStands holds the one form that is not a
// portable declaration: a type named for this target with sql() or read from a catalog.
func TestDeclaration_AVerbatimTypeIsWrittenAsItStands(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		verbatim bool
		want     string
	}{
		{"a portable name", "varchar(50)", false, "NVARCHAR(50)"},
		{"the same name named for the target", "varchar(50)", true, "varchar(50)"},
		{"the server's own timestamp", "timestamp", true, "timestamp"},
		{"a verbatim type has its spaces taken off", " TEXT ", true, "TEXT"},
		{"a verbatim type with no text is mapped", "", true, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declaration := sqlservertype.Declaration{Type: test.declared, Verbatim: test.verbatim}
			c.Assert(declaration.Rendered(), qt.Equals, test.want)
		})
	}
}

// TestServerBuilt_HappyPath holds the portable names the server also takes as its own.
func TestServerBuilt_HappyPath(t *testing.T) {
	tests := []struct {
		declared string
		want     string
	}{
		{"text", "text"},
		{"TEXT", "TEXT"},
		{"timestamp", "timestamp"},
		{"varchar", "VARCHAR"},
		{"varchar(50)", "VARCHAR(50)"},
		{"character varying(50)", "VARCHAR(50)"},
		{"char", "CHAR"},
		{"char(5)", "CHAR(5)"},
		{"character(5)", "CHAR(5)"},
	}
	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			c := qt.New(t)
			built, named := sqlservertype.ServerBuilt(test.declared)
			c.Assert(named, qt.IsTrue)
			c.Assert(built, qt.Equals, test.want)
		})
	}
}

// TestServerBuilt_FailurePath holds the names that do not name a type of the server's
// own, so nothing a server built can match them by the word alone.
func TestServerBuilt_FailurePath(t *testing.T) {
	tests := []string{
		"boolean", "bool", "bytea", "blob", "citext", "timestamptz", "timestamp with time zone",
		"timestamp without time zone", "timestamp(3)", "text(10)", "integer", "double precision", "nvarchar(50)",
	}
	for _, declared := range tests {
		t.Run(declared, func(t *testing.T) {
			c := qt.New(t)
			_, named := sqlservertype.ServerBuilt(declared)
			c.Assert(named, qt.IsFalse)
		})
	}
}

// TestBase holds what the comparison reads as a type's name.
func TestBase(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"nvarchar(max)", "NVARCHAR"},
		{" VARCHAR (50) ", "VARCHAR"},
		{"double precision", "DOUBLE PRECISION"},
		{"DATETIME2", "DATETIME2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlservertype.Base(test.name), qt.Equals, test.want)
		})
	}
}
