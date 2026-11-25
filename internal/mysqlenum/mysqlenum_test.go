package mysqlenum_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mysqlenum"
)

func TestValues_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		want       []string
	}{
		{name: "simple enum", columnType: "enum('active','inactive')", want: []string{"active", "inactive"}},
		{name: "values holding spaces", columnType: "enum('value 1', 'value 2', 'value 3')", want: []string{"value 1", "value 2", "value 3"}},
		{name: "double quotes", columnType: `enum("active","inactive")`, want: []string{"active", "inactive"}},
		{name: "single value", columnType: "enum('single')", want: []string{"single"}},
		{
			// Measured on MySQL 26.10 and MariaDB 12.3 for ENUM('e\\f', 'g,h', 'it''s').
			name:       "values holding a backslash, a comma and a quote",
			columnType: `enum('e\\f','g,h','it''s')`,
			want:       []string{`e\f`, "g,h", "it's"},
		},
		{
			// The type an Atlas HCL schema declares: the keyword in any case,
			// and a space before the parenthesis.
			name:       "a declared type",
			columnType: `ENUM ("happy", " sad", "ok ")`,
			want:       []string{"happy", " sad", "ok "},
		},
		{name: "a value of spaces", columnType: "enum('a',' ')", want: []string{"a", " "}},
		{name: "an empty value is left out", columnType: "enum('a','')", want: []string{"a"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mysqlenum.Values(test.columnType), qt.DeepEquals, test.want)
		})
	}
}

func TestSetValues_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		want       []string
	}{
		{name: "the type a catalog reports", columnType: "set('a',' b')", want: []string{"a", " b"}},
		{name: "a declared type", columnType: `SET ("a ", "b")`, want: []string{"a ", "b"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mysqlenum.SetValues(test.columnType), qt.DeepEquals, test.want)
		})
	}
}

// An ENUM type has no SET members and a SET type no ENUM values, so the
// catalog reader, which reads a column's ENUM values, never reads a SET
// column as an enum.
func TestSetValues_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		got  []string
	}{
		{name: "an enum read as a set", got: mysqlenum.SetValues("enum('a','b')")},
		{name: "a set read as an enum", got: mysqlenum.Values("set('a','b')")},
		{name: "a longer keyword", got: mysqlenum.SetValues("settings('a')")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.got, qt.IsNil)
		})
	}
}

func TestValues_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
	}{
		{name: "not an enum", columnType: "varchar(255)"},
		{name: "empty enum", columnType: "enum()"},
		{name: "no parenthesis", columnType: "enum"},
		{name: "a longer keyword", columnType: "enumeration('a')"},
		{name: "invalid format", columnType: "not_enum_format"},
		{name: "a literal nothing closes", columnType: "enum('a','b)"},
		{name: "a value that is not a literal", columnType: "enum('a', b)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mysqlenum.Values(test.columnType), qt.IsNil)
		})
	}
}
