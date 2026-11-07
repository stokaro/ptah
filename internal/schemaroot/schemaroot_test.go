package schemaroot_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/manageddata"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaroot"
)

func TestOf(t *testing.T) {
	c := qt.New(t)
	parent := t.TempDir()
	t.Chdir(parent)
	working, err := os.Getwd()
	c.Assert(err, qt.IsNil)

	tests := []struct {
		name     string
		rootDirs []string
		want     []string
	}{
		{name: "no root is the working directory", want: []string{working}},
		{name: "an empty root is no root", rootDirs: []string{""}, want: []string{working}},
		{
			name:     "every root counts, made absolute",
			rootDirs: []string{"first", "", "second"},
			want:     []string{filepath.Join(working, "first"), filepath.Join(working, "second")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			qt.New(t).Assert(schemaroot.Of(test.rootDirs), qt.DeepEquals, test.want)
		})
	}
}

// twoRoots lays out what a composite schema reads: a file under the second root,
// and one outside both.
func twoRoots(c *qt.C) (first, second, outside string) {
	c.Helper()
	parent := c.TempDir()
	first = filepath.Join(parent, "first")
	second = filepath.Join(parent, "second")
	outside = filepath.Join(parent, "outside")
	for _, dir := range []string{first, second, outside} {
		c.Assert(os.Mkdir(dir, 0o750), qt.IsNil)
	}
	c.Assert(os.WriteFile(filepath.Join(second, "rows.yaml"), []byte("- code: SECOND\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(outside, "rows.yaml"), []byte("- code: OUTSIDE\n"), 0o600), qt.IsNil)
	return first, second, outside
}

func TestLoad_ReadsThroughTheRootThatHoldsTheFile(t *testing.T) {
	c := qt.New(t)
	first, second, _ := twoRoots(c)

	rows, err := schemaroot.Load([]string{first, second}, schemamodel.ManagedData{
		Table: "regions", File: "rows.yaml", SourceDir: second,
	}, manageddata.LoadRows)

	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.DeepEquals, []map[string]any{{"code": "SECOND"}})
}

func TestLoad_FailurePath(t *testing.T) {
	setup := qt.New(t)
	first, second, outside := twoRoots(setup)
	declaration := schemamodel.ManagedData{Table: "regions", File: "rows.yaml", SourceDir: outside}

	t.Run("a file outside every root answers with the first refusal", func(t *testing.T) {
		c := qt.New(t)
		rows, err := schemaroot.Load([]string{first, second}, declaration, manageddata.LoadRows)
		c.Assert(err, qt.ErrorIs, manageddata.ErrOutsideRoot)
		c.Assert(err, qt.ErrorMatches, `.*is outside allowed root ".*first"`)
		c.Assert(rows, qt.IsNil)
	})
	t.Run("no roots is a refusal", func(t *testing.T) {
		c := qt.New(t)
		rows, err := schemaroot.Load(nil, declaration, manageddata.LoadRows)
		c.Assert(err, qt.ErrorIs, manageddata.ErrOutsideRoot)
		c.Assert(rows, qt.IsNil)
	})
	t.Run("another failure stops at the root that gave it", func(t *testing.T) {
		c := qt.New(t)
		broken := errors.New("broken")
		calls := 0
		rows, err := schemaroot.Load([]string{first, second}, declaration,
			func(string, schemamodel.ManagedData) ([]map[string]any, error) {
				calls++
				return nil, broken
			},
		)
		c.Assert(err, qt.ErrorIs, broken)
		c.Assert(calls, qt.Equals, 1)
		c.Assert(rows, qt.IsNil)
	})
}
