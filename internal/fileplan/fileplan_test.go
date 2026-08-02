package fileplan_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/fileplan"
)

func TestApply_HappyPath(t *testing.T) {
	c := qt.New(t)
	root := filepath.Join(t.TempDir(), "schema")

	err := fileplan.Apply([]fileplan.File{
		{Root: root, Path: "main.sql", Data: "-- atlas:import ./tables/users.sql\n"},
		{Root: root, Path: "tables/users.sql", Data: "CREATE TABLE users (id int);\n"},
	})

	c.Assert(err, qt.IsNil)
	mainSQL, err := os.ReadFile(filepath.Join(root, "main.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(mainSQL), qt.Contains, "atlas:import")
	usersSQL, err := os.ReadFile(filepath.Join(root, "tables", "users.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(usersSQL), qt.Contains, "CREATE TABLE users")
}

func TestApply_OverwritesRegularFile(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	target := filepath.Join(root, "schema.hcl")
	c.Assert(os.WriteFile(target, []byte("old"), 0o600), qt.IsNil)

	err := fileplan.Apply([]fileplan.File{{Root: root, Path: "schema.hcl", Data: "new"}})

	c.Assert(err, qt.IsNil)
	data, err := os.ReadFile(target)
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, "new")
}

func TestApply_FailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		files   func(c *qt.C, root string) []fileplan.File
		wantErr string
	}{
		{
			name: "empty root",
			files: func(_ *qt.C, _ string) []fileplan.File {
				return []fileplan.File{{Root: "  ", Path: "a.sql", Data: "x"}}
			},
			wantErr: `output directory must not be empty`,
		},
		{
			name: "empty path",
			files: func(_ *qt.C, root string) []fileplan.File {
				return []fileplan.File{{Root: root, Path: " ", Data: "x"}}
			},
			wantErr: `output path must not be empty`,
		},
		{
			name: "absolute path",
			files: func(_ *qt.C, root string) []fileplan.File {
				return []fileplan.File{{Root: root, Path: "/etc/passwd", Data: "x"}}
			},
			wantErr: `unsafe output path "/etc/passwd"`,
		},
		{
			name: "traversal path",
			files: func(_ *qt.C, root string) []fileplan.File {
				return []fileplan.File{{Root: root, Path: "../escape.sql", Data: "x"}}
			},
			wantErr: `unsafe output path "\.\./escape\.sql"`,
		},
		{
			name: "nested traversal path",
			files: func(_ *qt.C, root string) []fileplan.File {
				return []fileplan.File{{Root: root, Path: "tables/../../escape.sql", Data: "x"}}
			},
			wantErr: `unsafe output path "tables/\.\./\.\./escape\.sql"`,
		},
		{
			name: "backslash path",
			files: func(_ *qt.C, root string) []fileplan.File {
				return []fileplan.File{{Root: root, Path: `tables\users.sql`, Data: "x"}}
			},
			wantErr: `unsafe output path "tables\\\\users\.sql"`,
		},
		{
			name: "duplicate path",
			files: func(_ *qt.C, root string) []fileplan.File {
				return []fileplan.File{
					{Root: root, Path: "tables/users.sql", Data: "a"},
					{Root: root, Path: "tables//users.sql", Data: "b"},
				}
			},
			wantErr: `duplicate output path "tables//users\.sql" .*also produced as "tables/users\.sql".*`,
		},
		{
			name: "file collides with planned directory",
			files: func(_ *qt.C, root string) []fileplan.File {
				return []fileplan.File{
					{Root: root, Path: "tables", Data: "a"},
					{Root: root, Path: "tables/users.sql", Data: "b"},
				}
			},
			wantErr: `output path "tables/users\.sql" needs directory "tables", which is also a planned output file`,
		},
		{
			name: "existing directory at destination",
			files: func(c *qt.C, root string) []fileplan.File {
				c.Assert(os.MkdirAll(filepath.Join(root, "tables"), 0o755), qt.IsNil)
				return []fileplan.File{{Root: root, Path: "tables", Data: "x"}}
			},
			wantErr: `output path "tables" already exists as a directory`,
		},
		{
			name: "symlink escape from root",
			files: func(c *qt.C, root string) []fileplan.File {
				c.Assert(os.MkdirAll(root, 0o755), qt.IsNil)
				outside := c.TempDir()
				c.Assert(os.Symlink(outside, filepath.Join(root, "linked")), qt.IsNil)
				return []fileplan.File{{Root: root, Path: "linked/escape.sql", Data: "x"}}
			},
			wantErr: `unsafe output path "linked/escape\.sql": .*outside allowed root.*`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			root := filepath.Join(c.TempDir(), "out")

			err := fileplan.Apply(test.files(c, root))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestApply_ValidationFailureWritesNothing proves the all-or-nothing contract:
// a plan with one invalid file must not write its valid files either.
func TestApply_ValidationFailureWritesNothing(t *testing.T) {
	c := qt.New(t)
	root := filepath.Join(t.TempDir(), "out")

	err := fileplan.Apply([]fileplan.File{
		{Root: root, Path: "ok.sql", Data: "fine"},
		{Root: root, Path: "../escape.sql", Data: "bad"},
	})

	c.Assert(err, qt.ErrorMatches, `unsafe output path .*`)
	_, statErr := os.Stat(filepath.Join(root, "ok.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}
