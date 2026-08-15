package projectconfig

// White-box testing required: the rule is unexported, and its Windows answer is
// not reachable through the exported loader from a Unix runner — filepath.IsAbs
// is what differs, and only a direct call can hand it both spellings.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestLeavesTheProjectDirectory_RefusesEverySpellingOfARoot pins that an
// atlas.hcl is allowed to name the same set of paths whatever machine opens it.
//
// filepath.IsAbs was the whole rule, and on Windows it answers false for
// "/tmp/secret.txt" because there is no volume name — while the path still
// resolves to C:\tmp\secret.txt, outside every project. A project file refused
// on Linux was therefore read on Windows.
//
// The rows run on every operating system because the predicate is now a
// property of the string rather than of the host.
func TestLeavesTheProjectDirectory_RefusesEverySpellingOfARoot(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "a unix root", path: "/tmp/secret.txt", want: true},
		{name: "a windows root with no volume", path: `\tmp\secret.txt`, want: true},
		{name: "a drive path", path: `C:\tmp\secret.txt`, want: true},
		{name: "a drive path with forward slashes", path: "C:/tmp/secret.txt", want: true},
		{name: "a UNC share", path: `\\server\share\secret.txt`, want: true},
		{name: "a plain relative path", path: "schema.hcl", want: false},
		{name: "a nested relative path", path: "schemas/app.hcl", want: false},
		{name: "a relative path with a drive-looking element", path: "db/C:/x.hcl", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(leavesTheProjectDirectory(test.path), qt.Equals, test.want)
		})
	}
}
