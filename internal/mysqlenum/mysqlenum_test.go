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
		{
			// A declared type may name a character set and a collation after
			// the list, and a parenthesis inside a value does not close it.
			name:       "attributes after the list",
			columnType: "ENUM('ok ', 'a)') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
			want:       []string{"ok ", "a)"},
		},
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
		{name: "attributes after the list", columnType: "set('m ','n') COLLATE utf8mb4_bin", want: []string{"m ", "n"}},
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

func TestCharset_HappyPath(t *testing.T) {
	tests := []struct {
		name, columnType, charset, collation string
	}{
		{
			name:       "a character set and a collation",
			columnType: "ENUM('ok ','x') CHARACTER SET binary COLLATE binary",
			charset:    "binary", collation: "binary",
		},
		{name: "CHARSET in lower case", columnType: "enum('a') charset latin1", charset: "latin1"},
		{name: "a collation alone", columnType: "set('m ','n') COLLATE utf8mb4_bin", collation: "utf8mb4_bin"},
		{name: "quoted names", columnType: "enum('a') CHARACTER SET 'binary' COLLATE `binary`", charset: "binary", collation: "binary"},
		{name: "other attributes are passed over", columnType: "enum('a') NOT NULL CHARSET binary", charset: "binary"},
		{name: "a parenthesis inside a value", columnType: "enum('CHARSET latin1)') CHARSET binary", charset: "binary"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			charset, collation := mysqlenum.Charset(test.columnType)
			c.Assert(charset, qt.Equals, test.charset)
			c.Assert(collation, qt.Equals, test.collation)
		})
	}
}

func TestCharset_FailurePath(t *testing.T) {
	tests := []struct {
		name, columnType string
	}{
		{name: "no attributes", columnType: "enum('a','b')"},
		{name: "not an enum or a set", columnType: "varchar(10) CHARACTER SET binary"},
		{name: "a list nothing closes", columnType: "enum('a' CHARACTER SET binary"},
		{name: "a keyword with no name", columnType: "enum('a') CHARACTER SET"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			charset, collation := mysqlenum.Charset(test.columnType)
			c.Assert(charset, qt.Equals, "")
			c.Assert(collation, qt.Equals, "")
		})
	}
}
