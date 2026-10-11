package pgtypealias_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgtypealias"
)

func TestCanonical(t *testing.T) {
	tests := []struct {
		name     string
		spelling string
		want     string
	}{
		{name: "an alias", spelling: "VARCHAR", want: "character varying"},
		{name: "an alias with parameters", spelling: " varchar ( 20 ) ", want: "character varying(20)"},
		{name: "a timestamp", spelling: "timestamptz", want: "timestamp with time zone"},
		{name: "a serial", spelling: "bigserial", want: "bigint"},
		{name: "numeric parameters", spelling: "DECIMAL(10, 2)", want: "numeric(10,2)"},
		{name: "a canonical spelling", spelling: "character varying(20)", want: "character varying(20)"},
		{name: "a user type", spelling: "App.Email", want: "app.email"},
		{name: "an array keeps its marker", spelling: "int[]", want: "int[]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgtypealias.Canonical(test.spelling), qt.Equals, test.want)
		})
	}
}

func TestAlias(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
		found bool
	}{
		{name: "an alias in any case", input: "TimestampTZ", want: "timestamp with time zone", found: true},
		{name: "a canonical name", input: "integer"},
		{name: "a parameter list", input: "varchar(20)"},
		{name: "an array", input: "int[]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, found := pgtypealias.Alias(test.input)
			c.Assert(got, qt.Equals, test.want)
			c.Assert(found, qt.Equals, test.found)
		})
	}
}
