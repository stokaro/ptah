package atlas_test

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/atlascompat"
	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// The `--dir` default matrix for stokaro/ptah#1241 items 2, 3 and 4.
//
// Every expectation below was measured against the pinned Atlas community
// binary v1.3.0 on 2026-08-07, each cell in its own freshly built directory,
// with the exit status read on a line of its own rather than through a pipe.
// Before the change the three "defaults to ./migrations" rows exited 1 with
// `migrations directory is required` and the two "creates missing parents" rows
// exited 1 with `parent directory … is not available`, while that binary exited
// 0 and wrote in all five.
//
// The rows that do NOT move are the point of the file. A default that also acts
// as a fallback is worse than no default: `--dir file://migrtions` beside a
// perfectly good ./migrations would silently migrate the wrong directory, and a
// default that is consulted before the atlas.sum gate would let `migrate new`
// append to a drifted directory. Those rows are measured too, and they are the
// ones that go red when the default is turned into a fallback.

// dirDefaultCase is one cell: a fixture, an argv, and what must be true after.
type dirDefaultCase struct {
	name   string
	setup  func(c *qt.C, root string)
	args   []string
	assert func(c *qt.C, root string, err error, output string)
}

func TestCompatMigrateDirDefaults(t *testing.T) {
	migrationSQL := "CREATE TABLE wanted (id INTEGER PRIMARY KEY);\n"
	decoySQL := "CREATE TABLE decoy (id INTEGER PRIMARY KEY);\n"

	tests := []dirDefaultCase{
		{
			// #1241 item 2. The headline row.
			name: "apply defaults to file://migrations",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_init.sql", migrationSQL)
			},
			args: []string{"migrate", "apply", "--url", "sqlite://local.db?_fk=1"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				assertSQLiteTablePresent(c, filepath.Join(root, "local.db"), "wanted")
			},
		},
		{
			// The default names a directory, it does not invent one: with no
			// ./migrations the pinned binary exits 1 with
			// `sql/migrate: stat migrations: no such file or directory`.
			name:  "apply without ./migrations still fails",
			setup: func(_ *qt.C, _ string) {},
			args:  []string{"migrate", "apply", "--url", "sqlite://local.db?_fk=1"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				// Deliberately not just "any error": before the default
				// existed this row also failed, with `migrations directory is
				// required`. Naming the failed OPEN is what makes the row
				// distinguish a default that resolved and found nothing from a
				// verb that never had a directory to look for.
				c.Assert(err.Error(), qt.Contains, "open migrations directory")
				// The sentinel, not a rendering of it. This failure is an open,
				// not a stat, and the two carry different syscall errors whose
				// texts also differ per platform -- so spelling either one asserts
				// a coincidence.
				c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
				assertPathAbsent(c, filepath.Join(root, "migrations"))
			},
		},
		{
			// An explicit --dir still wins. The decoy ./migrations is hashed
			// and applicable, so a default that beat the flag would exit 0 and
			// create the wrong table rather than failing visibly.
			name: "apply --dir overrides the default",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_decoy.sql", decoySQL)
				writeHashedAtlasDir(c, filepath.Join(root, "elsewhere"), "20240101000000_init.sql", migrationSQL)
			},
			args: []string{"migrate", "apply", "--url", "sqlite://local.db?_fk=1", "--dir", "file://elsewhere"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				assertSQLiteTablePresent(c, filepath.Join(root, "local.db"), "wanted")
				assertSQLiteTableAbsent(c, filepath.Join(root, "local.db"), "decoy")
			},
		},
		{
			// The typo control. `migrtions` does not exist and ./migrations
			// does; a fallback would apply the wrong directory and exit 0.
			name: "apply --dir typo does not fall back to the default",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_decoy.sql", decoySQL)
			},
			args: []string{"migrate", "apply", "--url", "sqlite://local.db?_fk=1", "--dir", "file://migrtions"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Contains, "migrtions")
				assertSQLiteTableAbsent(c, filepath.Join(root, "local.db"), "decoy")
			},
		},
		{
			// The default reaches the same atlas.sum gate an explicit --dir
			// reaches. Measured: the pinned binary exits 1 with
			// `checksum mismatch` on this fixture with no --dir at all.
			name: "apply default directory is still checksum gated",
			setup: func(c *qt.C, root string) {
				dir := filepath.Join(root, "migrations")
				writeHashedAtlasDir(c, dir, "20240101000000_init.sql", migrationSQL)
				c.Assert(os.WriteFile(
					filepath.Join(dir, "20240101000000_init.sql"),
					[]byte("CREATE TABLE edited (id INTEGER PRIMARY KEY);\n"),
					0o600,
				), qt.IsNil)
			},
			args: []string{"migrate", "apply", "--url", "sqlite://local.db?_fk=1"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Contains, "checksum mismatch")
				assertSQLiteTableAbsent(c, filepath.Join(root, "local.db"), "wanted")
			},
		},
		{
			// #1241 item 3. `migrate new` documented the default and ignored
			// it; the pinned binary creates ./migrations and writes into it.
			name:  "new defaults to file://migrations and creates it",
			setup: func(_ *qt.C, _ string) {},
			args:  []string{"migrate", "new", "addcol"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				assertOneMigrationNamed(c, filepath.Join(root, "migrations"), "*_addcol.sql")
				assertPathPresent(c, filepath.Join(root, "migrations", atlascompat.AtlasSumFileName))
			},
		},
		{
			name: "new --dir overrides the default",
			setup: func(c *qt.C, root string) {
				c.Assert(os.MkdirAll(filepath.Join(root, "migrations"), 0o755), qt.IsNil)
			},
			args: []string{"migrate", "new", "addcol", "--dir", "file://elsewhere"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				assertOneMigrationNamed(c, filepath.Join(root, "elsewhere"), "*_addcol.sql")
				assertDirEmpty(c, filepath.Join(root, "migrations"))
			},
		},
		{
			// #1241 item 4, at three missing levels rather than one: measured,
			// the pinned binary creates a, a/b, a/b/c, the migration and
			// atlas.sum, and exits 0.
			name:  "new --dir creates missing parent directories",
			setup: func(_ *qt.C, _ string) {},
			args:  []string{"migrate", "new", "addcol", "--dir", "file://a/b/c"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				assertOneMigrationNamed(c, filepath.Join(root, "a", "b", "c"), "*_addcol.sql")
				assertPathPresent(c, filepath.Join(root, "a", "b", "c", atlascompat.AtlasSumFileName))
			},
		},
		{
			// Creating parents must not create them THROUGH a regular file.
			// Measured: the pinned binary exits 1 with
			// `sql/migrate: stat a/b: not a directory`.
			name: "new --dir refuses a parent that is a regular file",
			setup: func(c *qt.C, root string) {
				c.Assert(os.WriteFile(filepath.Join(root, "a"), []byte("not a directory\n"), 0o600), qt.IsNil)
			},
			args: []string{"migrate", "new", "addcol", "--dir", "file://a/b"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				// `a` is still the file it was: nothing was replaced or
				// written through it. Statting `a/b` reports ENOTDIR rather
				// than ENOENT here, so absence is asserted on the parent's
				// contents instead of on the child's stat error.
				assertRegularFileContent(c, filepath.Join(root, "a"), "not a directory\n")
			},
		},
		{
			// The default reaches the #1086 write gate. An unhashed
			// ./migrations is refused with nothing added, exactly as the
			// pinned binary refuses it (`checksum file not found`).
			name: "new default directory is still checksum gated",
			setup: func(c *qt.C, root string) {
				dir := filepath.Join(root, "migrations")
				c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
				c.Assert(os.WriteFile(
					filepath.Join(dir, "20240101000000_init.sql"),
					[]byte(migrationSQL),
					0o600,
				), qt.IsNil)
			},
			args: []string{"migrate", "new", "addcol"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Contains, "checksum file not found")
				assertNoMigrationNamed(c, filepath.Join(root, "migrations"), "*_addcol.sql")
			},
		},
		{
			// atlas.hcl outranks the default on both binaries: measured, each
			// writes into mydir and leaves the hashed decoy ./migrations
			// untouched.
			name: "new atlas.hcl migration.dir outranks the default",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_decoy.sql", decoySQL)
				c.Assert(os.MkdirAll(filepath.Join(root, "mydir"), 0o755), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(`env "local" {
  migration {
    dir = "file://mydir"
  }
}
`), 0o600), qt.IsNil)
			},
			args: []string{"migrate", "new", "addcol", "--env", "local"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				assertOneMigrationNamed(c, filepath.Join(root, "mydir"), "*_addcol.sql")
				assertNoMigrationNamed(c, filepath.Join(root, "migrations"), "*_addcol.sql")
			},
		},
		{
			// PTAH_MIGRATIONS_DIR is the native flag's own environment twin,
			// the layer atlasargs.appendDefaultArgs consults before it fills
			// in a default. It has no counterpart on the pinned binary, so the
			// rule it protects is Ptah's own: adding an Atlas default must not
			// take a capability away (AGENTS.md, third part).
			name: "new PTAH_MIGRATIONS_DIR outranks the default",
			setup: func(c *qt.C, root string) {
				c.Assert(os.MkdirAll(filepath.Join(root, "migrations"), 0o755), qt.IsNil)
				c.Setenv("PTAH_MIGRATIONS_DIR", filepath.Join(root, "mydir"))
			},
			args: []string{"migrate", "new", "addcol"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				assertOneMigrationNamed(c, filepath.Join(root, "mydir"), "*_addcol.sql")
				assertDirEmpty(c, filepath.Join(root, "migrations"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			t.Chdir(root)
			tt.setup(c, root)

			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(tt.args)

			err := cmd.Execute()

			tt.assert(c, root, err, out.String())
		})
	}
}

// TestCompatMigrateDirDefaultIsDocumented pins the help line to the runtime.
//
// `migrate new` used to print no default at all while refusing to run without a
// directory, and `migrate hash` / `migrate validate` printed no default while
// silently using ./migrations — two ways for `--help` and the runtime to
// disagree. The pinned community binary v1.3.0 prints
// `(default "file://migrations")` on every migrate verb that registers --dir.
func TestCompatMigrateDirDefaultIsDocumented(t *testing.T) {
	verbs := []string{"apply", "new", "diff", "status", "set", "lint", "hash", "validate"}
	for _, verb := range verbs {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{"migrate", verb, "--help"})

			c.Assert(cmd.Execute(), qt.IsNil)

			c.Assert(out.String(), qt.Contains, `(default "file://migrations")`)
		})
	}
}

func writeHashedAtlasDir(c *qt.C, dir, name, body string) {
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	sum, err := atlascompat.ComputeSum(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, atlascompat.AtlasSumFileName), sum.Bytes(), 0o600), qt.IsNil)
}

func assertOneMigrationNamed(c *qt.C, dir, pattern string) {
	c.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
}

func assertNoMigrationNamed(c *qt.C, dir, pattern string) {
	c.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 0)
}

func assertDirEmpty(c *qt.C, dir string) {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

func assertPathPresent(c *qt.C, path string) {
	c.Helper()
	_, err := os.Stat(path)
	c.Assert(err, qt.IsNil)
}

func assertPathAbsent(c *qt.C, path string) {
	c.Helper()
	_, err := os.Stat(path)
	c.Assert(os.IsNotExist(err), qt.IsTrue, qt.Commentf("expected %s to be absent, stat error was %v", path, err))
}

func assertRegularFileContent(c *qt.C, path, want string) {
	c.Helper()
	info, err := os.Lstat(path)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().IsRegular(), qt.IsTrue)
	got, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(got), qt.Equals, want)
}

// sqliteTableNames reads the tables a migration run actually created. The
// assertions below name a table rather than counting revisions because the two
// fixtures differ only in which directory was read, and the table name is the
// only observable that says which one won.
func sqliteTableNames(c *qt.C, dbPath string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	schema, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		names = append(names, table.Name)
	}
	return names
}

func assertSQLiteTablePresent(c *qt.C, dbPath, table string) {
	c.Helper()
	c.Assert(sqliteTableNames(c, dbPath), qt.Contains, table)
}

func assertSQLiteTableAbsent(c *qt.C, dbPath, table string) {
	c.Helper()
	c.Assert(sqliteTableNames(c, dbPath), qt.Not(qt.Contains), table)
}
