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

// TestApply_FailurePath holds the refusals a plan carries on its own, before
// anything on disk is consulted. The two refusals that need a prepared
// destination are separate tests below, because there the fixture is half of
// what is being refused.
func TestApply_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		files   func(root string) []fileplan.File
		wantErr string
	}{
		{
			name: "empty root",
			files: func(string) []fileplan.File {
				return []fileplan.File{{Root: "  ", Path: "a.sql", Data: "x"}}
			},
			wantErr: `output directory must not be empty`,
		},
		{
			name: "empty path",
			files: func(root string) []fileplan.File {
				return []fileplan.File{{Root: root, Path: " ", Data: "x"}}
			},
			wantErr: `output path must not be empty`,
		},
		{
			name: "absolute path",
			files: func(root string) []fileplan.File {
				return []fileplan.File{{Root: root, Path: "/etc/passwd", Data: "x"}}
			},
			wantErr: `unsafe output path "/etc/passwd"`,
		},
		{
			name: "traversal path",
			files: func(root string) []fileplan.File {
				return []fileplan.File{{Root: root, Path: "../escape.sql", Data: "x"}}
			},
			wantErr: `unsafe output path "\.\./escape\.sql"`,
		},
		{
			name: "nested traversal path",
			files: func(root string) []fileplan.File {
				return []fileplan.File{{Root: root, Path: "tables/../../escape.sql", Data: "x"}}
			},
			wantErr: `unsafe output path "tables/\.\./\.\./escape\.sql"`,
		},
		{
			name: "backslash path",
			files: func(root string) []fileplan.File {
				return []fileplan.File{{Root: root, Path: `tables\users.sql`, Data: "x"}}
			},
			wantErr: `unsafe output path "tables\\\\users\.sql"`,
		},
		{
			name: "duplicate path",
			files: func(root string) []fileplan.File {
				return []fileplan.File{
					{Root: root, Path: "tables/users.sql", Data: "a"},
					{Root: root, Path: "tables//users.sql", Data: "b"},
				}
			},
			wantErr: `duplicate output path "tables//users\.sql" .*also produced as "tables/users\.sql".*`,
		},
		{
			name: "file collides with planned directory",
			files: func(root string) []fileplan.File {
				return []fileplan.File{
					{Root: root, Path: "tables", Data: "a"},
					{Root: root, Path: "tables/users.sql", Data: "b"},
				}
			},
			wantErr: `output path "tables/users\.sql" needs directory "tables", which is also a planned output file`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := filepath.Join(c.TempDir(), "out")

			err := fileplan.Apply(test.files(root))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestApply_FailurePath_ExistingDirectoryAtDestination refuses a planned file
// whose name is already taken on disk by a directory.
func TestApply_FailurePath_ExistingDirectoryAtDestination(t *testing.T) {
	c := qt.New(t)
	root := filepath.Join(c.TempDir(), "out")
	c.Assert(os.MkdirAll(filepath.Join(root, "tables"), 0o750), qt.IsNil)

	err := fileplan.Apply([]fileplan.File{{Root: root, Path: "tables", Data: "x"}})

	c.Assert(err, qt.ErrorMatches, `output path "tables" already exists as a directory`)
}

// TestApply_FailurePath_SymlinkEscapeFromRoot refuses a path that stays inside
// the root textually and leaves it through a symlink already on disk.
func TestApply_FailurePath_SymlinkEscapeFromRoot(t *testing.T) {
	c := qt.New(t)
	root := filepath.Join(c.TempDir(), "out")
	c.Assert(os.MkdirAll(root, 0o750), qt.IsNil)
	c.Assert(os.Symlink(c.TempDir(), filepath.Join(root, "linked")), qt.IsNil)

	err := fileplan.Apply([]fileplan.File{{Root: root, Path: "linked/escape.sql", Data: "x"}})

	c.Assert(err, qt.ErrorMatches, `unsafe output path "linked/escape\.sql": .*outside allowed root.*`)
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
