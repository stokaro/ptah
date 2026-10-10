package ydbpath_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbpath"
)

// TestSplit_HappyPath reads a path relative to the database root: the last
// slash separates the directory from the name, a dot stays in the name, and
// surrounding space is ignored. A trailing slash leaves the name empty for the
// owner's identity check to refuse.
func TestSplit_HappyPath(t *testing.T) {
	tests := []struct {
		path, directory, name string
	}{
		{path: "pg", directory: "", name: "pg"},
		{path: "pg.pw", directory: "", name: "pg.pw"},
		{path: "ext/pg", directory: "ext", name: "pg"},
		{path: " app/ext/pg.pw ", directory: "app/ext", name: "pg.pw"},
		{path: "ext/", directory: "ext", name: ""},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			c := qt.New(t)

			directory, name, err := ydbpath.Split(test.path)

			c.Assert(err, qt.IsNil)
			c.Assert(directory, qt.Equals, test.directory)
			c.Assert(name, qt.Equals, test.name)
		})
	}
}

// TestSplit_FailurePath refuses a path that starts with a slash, which names
// its database, instead of reading it as a relative one.
func TestSplit_FailurePath(t *testing.T) {
	for _, path := range []string{"/pg", " /local/ext/pg"} {
		t.Run(path, func(t *testing.T) {
			c := qt.New(t)

			directory, name, err := ydbpath.Split(path)

			c.Assert(err, qt.ErrorIs, ydbpath.ErrAbsolute)
			c.Assert(directory, qt.Equals, "")
			c.Assert(name, qt.Equals, "")
		})
	}
}

// TestRelative_HappyPath returns a relative path as it is, and an absolute one
// under the database root without the root, once cleaned.
func TestRelative_HappyPath(t *testing.T) {
	tests := []struct {
		name, root, path, want string
	}{
		{name: "relative", root: "/local", path: "ext/pg", want: "ext/pg"},
		{name: "relative without a root", root: "", path: "ext/pg", want: "ext/pg"},
		{name: "absolute under the root", root: "/local", path: "/local/ext/pg", want: "ext/pg"},
		{name: "root written without slashes", root: "local", path: "/local/pg", want: "pg"},
		{name: "unclean absolute", root: "/local", path: " /local//ext/./pg ", want: "ext/pg"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ydbpath.Relative(test.root, test.path)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRelative_FailurePath refuses an absolute path outside the database, the
// database's own path included, and every absolute path where the root is not
// known.
func TestRelative_FailurePath(t *testing.T) {
	tests := []struct {
		name, root, path string
		want             error
	}{
		{name: "another database", root: "/local", path: "/other/ext/pg", want: ydbpath.ErrOutsideDatabase},
		{name: "a prefix of the name", root: "/local", path: "/localx/pg", want: ydbpath.ErrOutsideDatabase},
		{name: "the root itself", root: "/local", path: "/local", want: ydbpath.ErrOutsideDatabase},
		{name: "climbing out", root: "/local", path: "/local/../other/pg", want: ydbpath.ErrOutsideDatabase},
		{name: "no root", root: "", path: "/local/pg", want: ydbpath.ErrAbsolute},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ydbpath.Relative(test.root, test.path)

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(got, qt.Equals, "")
		})
	}
}
