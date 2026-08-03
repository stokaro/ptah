package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// projectFileHCL is a genuine Atlas project file: variables and an env block,
// and not one schema object. Handed to a command as a desired state, it must be
// refused. Parsed as a schema it yields an EMPTY desired state, and "empty" is
// indistinguishable from "drop everything".
const projectFileHCL = `variable "db" {
  type    = string
  default = "sqlite://app.db"
}

env "local" {
  url = var.db
  dev = "sqlite://dev?mode=memory"
  migration {
    dir = "file://migrations"
  }
}
`

// plainSchemaFileHCL is the control: a real schema file that must keep working.
const plainSchemaFileHCL = `schema "main" {
}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`

// envSchemaFileHCL is plainSchemaFileHCL plus a project-file env block, so any
// difference in outcome between the two is caused by that block alone.
const envSchemaFileHCL = `schema "main" {
}

env "local" {
  url = "sqlite://local.db"
}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`

// writeProjectFileFixtures lays down the three HCL files plus an empty migration
// directory and returns the directory holding them.
func writeProjectFileFixtures(t *testing.T) string {
	t.Helper()
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "plain.hcl"), []byte(plainSchemaFileHCL), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas_project.hcl"), []byte(projectFileHCL), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "env.hcl"), []byte(envSchemaFileHCL), 0o600), qt.IsNil)
	c.Assert(os.MkdirAll(filepath.Join(dir, "migrations"), 0o750), qt.IsNil)
	return dir
}

// projectFileCommandCase drives one compat invocation. Both fields are per-row
// funcs so argument wiring and expectations live in the row rather than in the
// loop body.
type projectFileCommandCase struct {
	name   string
	args   func(dir string) []string
	assert func(c *qt.C, dir, out string, err error)
}

// TestCompatRefusesProjectFileAsSchemaSource is the acceptance test for issue
// #1073 at the command boundary.
//
// Every assertion here is on BYTES, not on exit status alone, because exit
// status cannot see this bug: before the fix both commands SUCCEEDED, and a
// success that silently drops the user's schema shares its exit code with a
// success that did the right thing. An exit-code-only assertion would also be
// satisfied by a fix that refused the file for an unrelated reason.
func TestCompatRefusesProjectFileAsSchemaSource(t *testing.T) {
	cases := []projectFileCommandCase{
		{
			// Measured before the fix: exit 0 with
			//   -- WARNING: This will delete all data!
			//   DROP TABLE IF EXISTS "main"."users";
			// The project file was read as an empty desired state, so the
			// entire real schema was planned away.
			name: "schema diff refuses a project file as the desired state",
			args: func(dir string) []string {
				return []string{
					"schema", "diff",
					"--from", "file://" + filepath.Join(dir, "plain.hcl"),
					"--to", "file://" + filepath.Join(dir, "atlas_project.hcl"),
					"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
				}
			},
			assert: func(c *qt.C, _, out string, err error) {
				// Bytes first, on purpose: a red run then reports the
				// destructive statement it actually emitted rather than a bare
				// "wanted an error".
				c.Assert(out, qt.Not(qt.Contains), "DROP TABLE")
				c.Assert(out, qt.Not(qt.Contains), "This will delete all data")
				c.Assert(err, qt.IsNotNil)
				c.Assert(err.Error(), qt.Contains, "cannot parse project file")
				c.Assert(err.Error(), qt.Contains, `"env"`)
			},
		},
		{
			// Measured before the fix: exit 0, and it wrote both a migration
			// file and atlas.sum for a desired state it had silently emptied.
			name: "migrate diff writes nothing for a file carrying an env block",
			args: func(dir string) []string {
				return []string{
					"migrate", "diff", "x",
					"--to", "file://" + filepath.Join(dir, "env.hcl"),
					"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
					"--dir", "file://" + filepath.Join(dir, "migrations"),
				}
			},
			assert: func(c *qt.C, dir, out string, err error) {
				// The directory contents are the load-bearing assertion: the
				// pre-fix failure mode was files on disk, not a message.
				entries, readErr := os.ReadDir(filepath.Join(dir, "migrations"))
				c.Assert(readErr, qt.IsNil)
				c.Assert(entries, qt.HasLen, 0)
				c.Assert(out, qt.Not(qt.Contains), "Created migration file")
				c.Assert(err, qt.IsNotNil)
				c.Assert(err.Error(), qt.Contains, "cannot parse project file")
			},
		},
		{
			// The control. A refusal that also broke real schema files would
			// trade one defect for a worse one, so this row has to stay green.
			name: "schema inspect still reads a plain schema file",
			args: func(dir string) []string {
				return []string{
					"schema", "inspect",
					"--url", "file://" + filepath.Join(dir, "plain.hcl"),
					"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
				}
			},
			assert: func(c *qt.C, _, out string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(out, qt.Contains, `table "users"`)
				c.Assert(out, qt.Contains, `column "id"`)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeProjectFileFixtures(t)
			out, err := runCompatCommand(t, tc.args(dir)...)
			tc.assert(c, dir, out, err)
		})
	}
}
