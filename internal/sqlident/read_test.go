package sqlident_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlident"
)

func TestNamesArePaths(t *testing.T) {
	for _, test := range []struct {
		dialect string
		want    bool
	}{
		{dialect: "postgres", want: false},
		{dialect: "mysql", want: false},
		{dialect: "", want: false},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlident.NamesArePaths(test.dialect), qt.Equals, test.want)
		})
	}
}

// TestReadName_HappyPath reads a quoted ClickHouse name the way the server
// reads it. Each row is a measurement: the name written in a CREATE TABLE on
// ClickHouse 24.10 and 26.10, and the bytes system.columns reported for it.
func TestReadName_HappyPath(t *testing.T) {
	for _, test := range []struct{ name, written, want string }{
		{name: "a bare name in backticks", written: "`id`", want: "id"},
		{name: "a doubled backtick", written: "`a``b`", want: "a`b"},
		{name: "a backslash before a backtick", written: "`a\\`b`", want: "a`b"},
		{name: "a doubled backslash", written: "`a\\\\b`", want: `a\b`},
		{name: "the control escapes", written: "`\\a\\b\\e\\f\\n\\r\\t\\v\\0`", want: "\a\b\x1b\f\n\r\t\v\x00"},
		{name: "a hexadecimal byte", written: "`a\\x41b`", want: "aAb"},
		{name: "an escape that stands for nothing", written: "`a\\Nb`", want: "ab"},
		{name: "an escaped quote, slash or equals sign", written: "`\\'\\\"\\/\\=`", want: `'"/=`},
		{name: "an escaped control byte", written: "`a\\\tb`", want: "a\tb"},
		{name: "a backslash kept before any other byte", written: "`a\\zb\\:c\\ d\\%e\\X41`", want: `a\zb\:c\ d\%e\X41`},
		{name: "a backslash kept before DEL and a multibyte character", written: "`\\\x7f\\\u00e9`", want: "\\\x7f\\\u00e9"},
		{name: "a raw newline", written: "`a\nb`", want: "a\nb"},
		{name: "a comment marker", written: "`c -- d`", want: "c -- d"},
		{name: "double quotes", written: `"a""b\"c` + "`" + `d\tb"`, want: "a\"b\"c`d\tb"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := sqlident.ReadName("clickhouse", test.written)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestReadName_FailurePath leaves to the caller a dialect with no quoting of
// its own: no built-in dialect names an object by one path, so every one is
// read by the SQL standard's quoting.
func TestReadName_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, dialect, written string }{
		{name: "another dialect", dialect: "postgres", written: `"users"`},
		{name: "another dialect, unquoted", dialect: "mysql", written: "users"},
		{name: "no dialect", dialect: "", written: "users"},
		{name: "an engine only a provider serves", dialect: "ydb", written: "`users`"},
		{name: "ClickHouse, a hexadecimal escape cut short", dialect: "clickhouse", written: "`a\\x4`"},
		{name: "ClickHouse, a hexadecimal escape without digits", dialect: "clickhouse", written: "`a\\xZZb`"},
		{name: "ClickHouse, a hexadecimal escape without digits after a tab", dialect: "clickhouse", written: "`a\\tb\\xZZ`"},
		{name: "ClickHouse, double quotes, a hexadecimal escape without digits", dialect: "clickhouse", written: `"a\tb\xzz"`},
		{name: "ClickHouse, a backslash before the closing quote", dialect: "clickhouse", written: "`ab\\`"},
		{name: "ClickHouse, no closing quote", dialect: "clickhouse", written: "`open"},
		{name: "ClickHouse, a lone quote inside", dialect: "clickhouse", written: "`a`b`"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := sqlident.ReadName(test.dialect, test.written)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestSplitPath(t *testing.T) {
	for _, test := range []struct{ name, path, directory, leaf string }{
		{name: "no directory", path: "t", leaf: "t"},
		{name: "one directory", path: "app/t", directory: "app", leaf: "t"},
		{name: "nested directories", path: "app/sub/t", directory: "app/sub", leaf: "t"},
		{name: "a dot stays in its part", path: "a.b/c.d", directory: "a.b", leaf: "c.d"},
		{name: "a trailing slash leaves an empty name", path: "app/", directory: "app"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			directory, leaf := sqlident.SplitPath(test.path)
			c.Assert(directory, qt.Equals, test.directory)
			c.Assert(leaf, qt.Equals, test.leaf)
		})
	}
}
