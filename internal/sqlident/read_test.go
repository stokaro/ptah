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

// TestReadName_FailurePath leaves to the caller a dialect with no quoting of
// its own: no built-in dialect names an object by one path, so every one is
// read by the SQL standard's quoting.
func TestReadName_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, dialect, written string }{
		{name: "another dialect", dialect: "postgres", written: `"users"`},
		{name: "another dialect, unquoted", dialect: "mysql", written: "users"},
		{name: "no dialect", dialect: "", written: "users"},
		{name: "an engine only a provider serves", dialect: "ydb", written: "`users`"},
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
