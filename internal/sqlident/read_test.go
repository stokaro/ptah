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
		{dialect: "ydb", want: true},
		{dialect: "ydbs", want: true},
		{dialect: " YDB ", want: true},
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

// TestReadName_HappyPath decodes a YQL identifier, and undoes Quote on YDB for
// names whose characters YQL escapes or a SQL reader would split at.
func TestReadName_HappyPath(t *testing.T) {
	for _, test := range []struct{ name, written, want string }{
		{name: "unquoted", written: "users", want: "users"},
		{name: "unquoted keeps its case", written: "Users", want: "Users"},
		{name: "backticked", written: "`users`", want: "users"},
		{name: "an escaped backtick", written: "`c\\`d`", want: "c`d"},
		{name: "a doubled backtick", written: "`c``d`", want: "c`d"},
		{name: "a hex escape", written: "`i\\x64`", want: "id"},
		{name: "a dot and a slash are the name's", written: "`app/a.b`", want: "app/a.b"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := sqlident.ReadName("ydb", test.written)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
	for _, name := range []string{"plain", "c`d", `a\b`, "e\\`f", "dir/sub/t.v1", "line\nnext", "Ärger"} {
		t.Run("round trip "+name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := sqlident.ReadName("ydb", sqlident.Quote("ydb", name))
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, name)
		})
	}
}

// TestReadName_FailurePath leaves to the caller a dialect with no quoting of
// its own and text that is not one YQL identifier.
func TestReadName_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, dialect, written string }{
		{name: "another dialect", dialect: "postgres", written: `"users"`},
		{name: "another dialect, unquoted", dialect: "mysql", written: "users"},
		{name: "no dialect", dialect: "", written: "users"},
		{name: "empty", dialect: "ydb", written: ""},
		{name: "text after the closing backtick", dialect: "ydb", written: "`a`b"},
		{name: "a dangling escape", dialect: "ydb", written: "`a\\"},
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
