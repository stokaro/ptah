package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
)

// TestRead_ClickHouseQuotedNameEscapes_HappyPath pins that a ClickHouse schema
// file names a column the way the server reads the same file: measured on
// ClickHouse 24.10 and 26.10, \t is a tab, \x41 is A, a doubled backslash is
// one, and a backslash before a byte with no escape of its own stays.
func TestRead_ClickHouseQuotedNameEscapes_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		written string
		want    string
	}{
		{name: "tab escape in backquotes", written: "`a\\tb`", want: "a\tb"},
		{name: "hexadecimal escape in double quotes", written: `"a\x41"`, want: "aA"},
		{name: "doubled backslash", written: "`a\\\\b`", want: `a\b`},
		{name: "kept backslash", written: "`a\\qb`", want: `a\qb`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql := "CREATE TABLE tq (id Int32, " + test.written + " Int32) ENGINE = MergeTree ORDER BY id;"
			database, _, err := readWithOwners([]byte(sql), platform.ClickHouse)
			c.Assert(err, qt.IsNil)
			c.Assert(fieldNames(database), qt.DeepEquals, []string{"id", test.want})
		})
	}
}

// TestRead_ClickHouseQuotedNameEscapes_FailurePath pins that a quoted name
// the server refuses, or reads differently by release, refuses the file and
// names the name. Read by the SQL standard's quoting instead, `a\tb\xZZ`
// became a, two backslashes, tb and xZZ, while the server creates the column
// with a tab in it: a plan against that table added the one and dropped the
// other with its data. `a\x4` is refused by the server with code 26.
func TestRead_ClickHouseQuotedNameEscapes_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		written string
		want    string
	}{
		{name: "non-hexadecimal escape in backquotes", written: "`a\\tb\\xZZ`", want: "line 2: the quoted name `a\\\\tb\\\\xZZ` is not a clickhouse name .*"},
		{name: "non-hexadecimal escape in double quotes", written: `"a\tb\xzz"`, want: `line 2: the quoted name "a\\tb\\xzz" is not a clickhouse name .*`},
		{name: "one hexadecimal digit", written: "`a\\x4`", want: "line 2: the quoted name `a\\\\x4` is not a clickhouse name .*"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql := "-- a column the server reads by its own escapes\nCREATE TABLE tq (id Int32, " + test.written + " Int32) ENGINE = MergeTree ORDER BY id;"
			database, statements, err := readWithOwners([]byte(sql), platform.ClickHouse)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(database.Tables, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRead_ClickHouseQuotedNameEscapes_OnlyNamesAreRead pins that the refusal
// reads names alone: a string literal with the same text is a value, and a
// dialect whose quoted names take no escapes reads the name as written.
func TestRead_ClickHouseQuotedNameEscapes_OnlyNamesAreRead(t *testing.T) {
	c := qt.New(t)
	sql := "CREATE TABLE tq (id Int32, s String DEFAULT 'a\\x4') ENGINE = MergeTree ORDER BY id;"
	database, _, err := readWithOwners([]byte(sql), platform.ClickHouse)
	c.Assert(err, qt.IsNil)
	c.Assert(fieldNames(database), qt.DeepEquals, []string{"id", "s"})

	database, _, err = readWithOwners([]byte("CREATE TABLE tq (id INT, `a\\x4` INT);"), platform.MySQL)
	c.Assert(err, qt.IsNil)
	c.Assert(fieldNames(database), qt.DeepEquals, []string{"id", `a\x4`})
}

func fieldNames(database schemamodel.Database) []string {
	names := make([]string, 0, len(database.Fields))
	for _, field := range database.Fields {
		names = append(names, field.Name)
	}
	return names
}
