package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// The cells stokaro/ptah#1241 lists as "ptah-compat exits 1 where the pinned
// community binary v1.3.0 exits 0", for the two items closed here.
//
// Every expectation was measured against that binary on 2026-08-08, each cell
// in its own directory, the exit status read on a line of its own after a
// redirect rather than through a pipe. Before the change:
//
//	migrate validate --dir file://migrations --var foo=bar   ptah 1 / binary 0
//	migrate new nm   --dir file://migrations --var foo=bar   ptah 1 / binary 0
//	migrate validate --dir file://<empty dir>                ptah 1 / binary 0
//	migrate lint --latest 1 --dir file://<empty dir>         ptah 1 / binary 0
//
// The refusals that must NOT move are in the failure table below, and they are
// the rows that go red if either fix is widened: a project file that was
// SELECTED is still required, and a directory that actually holds migrations is
// still refused when it carries no atlas.sum.
//
// That table also pins item 13, a cell that is deliberately NOT closed. The
// pinned binary accepts a trailing positional and discards it, answering about
// a directory the caller did not name; the refusal stays and now says which
// flag the value belongs on.

// unformattedAtlasHCL is HCL that `schema fmt` rewrites, so a test asserting
// these exact bytes survived proves the run was refused before the verb did its
// work rather than merely finding nothing to change.
const unformattedAtlasHCL = "schema   \"main\"   {  }\n"

// overstrictCase is one cell: a fixture, an argv, and what must hold after.
type overstrictCase struct {
	name   string
	setup  func(c *qt.C, root string)
	args   []string
	assert func(c *qt.C, root string, err error, output string)
}

func TestCompatOverstrictCellsNowAccepted(t *testing.T) {
	tests := []overstrictCase{
		{
			// #1241 item 12. --var supplies values to a project file; it does
			// not select one, so it cannot make one required.
			name: "validate with --var and no atlas.hcl",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_init.sql", "CREATE TABLE t (id INTEGER);\n")
			},
			args: []string{"migrate", "validate", "--dir", "file://migrations", "--var", "foo=bar"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
			},
		},
		{
			// The same flag on a writing verb. `migrate new` reaches the same
			// extractor, which is why enumerating the verbs found it and the
			// issue's own list, naming only validate and hash, did not.
			name: "new with --var and no atlas.hcl",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_init.sql", "CREATE TABLE t (id INTEGER);\n")
			},
			args: []string{"migrate", "new", "addcol", "--dir", "file://migrations", "--var", "foo=bar"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				assertOneMigrationNamed(c, filepath.Join(root, "migrations"), "*_addcol.sql")
			},
		},
		{
			name: "hash with --var and no atlas.hcl",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_init.sql", "CREATE TABLE t (id INTEGER);\n")
			},
			args: []string{"migrate", "hash", "--dir", "file://migrations", "--var", "foo=bar"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
			},
		},
		{
			// The accept side of the tree-wide gate, and the row that holds it
			// to account for accepting. Every other accept row here names a verb
			// whose flags cobra does not parse, so their --var travels the raw
			// extractor and cannot redden a change to the gate at all -- widening
			// the gate to refuse every --var was measured against this table and
			// reddened no accepted row until this one existed. The assertion that
			// a.hcl was REWRITTEN is what proves the verb ran rather than being
			// quietly skipped. Measured 2026-08-08: exit 0 on both binaries.
			name: "a well-formed --var still runs a verb that never reads it",
			setup: func(c *qt.C, root string) {
				c.Assert(os.WriteFile(filepath.Join(root, "a.hcl"), []byte(unformattedAtlasHCL), 0o600), qt.IsNil)
			},
			args: []string{"schema", "fmt", "--var", "a=1"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				body, readErr := os.ReadFile(filepath.Join(root, "a.hcl"))
				c.Assert(readErr, qt.IsNil)
				c.Assert(string(body), qt.Not(qt.Equals), unformattedAtlasHCL)
			},
		},
		{
			// The writing verb's accept side: the destination must still be
			// created on a well-formed value, so the import refusal above cannot
			// be satisfied by an import that stopped working. Measured
			// 2026-08-08: exit 0 on both binaries, both writing ./dst.
			name: "a well-formed --var still lets migrate import write its destination",
			setup: func(c *qt.C, root string) {
				src := filepath.Join(root, "src")
				c.Assert(os.MkdirAll(src, 0o755), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(src, "V1__a.sql"), []byte("CREATE TABLE a (id INTEGER);\n"), 0o600), qt.IsNil)
			},
			args: []string{
				"migrate", "import",
				"--from", "file://src",
				"--to", "file://dst",
				"--dir-format", "flyway",
				"--var", "a=1",
			},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				assertPathPresent(c, filepath.Join(root, "dst", "atlas.sum"))
			},
		},
		{
			// An EMPTY name is accepted, because the pinned binary accepts it:
			// measured 2026-08-08, `--var =v` is exit 0 on both. The row exists
			// because the syntax rule tests for the separator alone, and
			// "tighten it to require a name" is the obvious wrong edit -- with
			// no row here it reddens nothing. config/projectconfig still refuses
			// the empty name later, where a project file is evaluated.
			name: "--var with an empty name is accepted, as on the pinned binary",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_init.sql", "CREATE TABLE t (id INTEGER);\n")
			},
			args: []string{"migrate", "validate", "--dir", "file://migrations", "--var", "=v"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
			},
		},
		{
			// The values are still carried. Without this row the fix could be
			// "drop --var on the floor", which passes every other row here and
			// silently ignores what the caller asked for.
			name: "--var still feeds a project file that is present",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "realdir"), "20240101000000_init.sql", "CREATE TABLE t (id INTEGER);\n")
				writeAtlasProjectFile(c, root)
			},
			args: []string{"migrate", "validate", "--env", "local", "--var", "d=realdir"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
			},
		},
		{
			// #1241 item 7. An existing directory holding no migration files
			// has nothing for an integrity file to cover.
			name: "validate an empty migration directory",
			setup: func(c *qt.C, root string) {
				c.Assert(os.MkdirAll(filepath.Join(root, "migrations"), 0o755), qt.IsNil)
			},
			args: []string{"migrate", "validate", "--dir", "file://migrations"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				c.Assert(output, qt.Equals, "")
			},
		},
		{
			// The converted-source path reaches the same conclusion through the
			// covered file set rather than through a directory listing.
			name: "validate an empty directory named as a foreign layout",
			setup: func(c *qt.C, root string) {
				c.Assert(os.MkdirAll(filepath.Join(root, "migrations"), 0o755), qt.IsNil)
			},
			args: []string{"migrate", "validate", "--dir", "file://migrations?format=flyway"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				c.Assert(output, qt.Equals, "")
			},
		},
		{
			// Adjacent to item 7 and found by running the same fixture through
			// every verb: the repository that lints its migrations in CI before
			// the first migration exists.
			name: "lint --latest on an empty migration directory",
			setup: func(c *qt.C, root string) {
				c.Assert(os.MkdirAll(filepath.Join(root, "migrations"), 0o755), qt.IsNil)
			},
			args: []string{
				"migrate", "lint",
				"--dir", "file://migrations",
				"--dev-url", "sqlite://file?mode=memory",
				"--latest", "1",
			},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				// The pinned binary writes zero bytes on both streams here.
				c.Assert(output, qt.Equals, "")
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

func TestCompatOverstrictCellsStillRefused(t *testing.T) {
	tests := []overstrictCase{
		{
			// -c SELECTS a project file, so a missing one is still an error on
			// both binaries. This is the row that reddens if the fix is widened
			// from "--var" to "any project flag".
			name:  "config flag still requires the project file",
			setup: func(_ *qt.C, _ string) {},
			args:  []string{"migrate", "validate", "--dir", "file://migrations", "-c", "file://nosuch.hcl"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.ErrorMatches, `failed to read atlas config nosuch\.hcl: .*`, qt.Commentf("%s", output))
			},
		},
		{
			name:  "env flag still requires the project file",
			setup: func(_ *qt.C, _ string) {},
			args:  []string{"migrate", "validate", "--dir", "file://migrations", "--env", "local"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.ErrorMatches, `failed to read atlas config atlas\.hcl: .*`, qt.Commentf("%s", output))
			},
		},
		{
			// The discriminating half of "--var is still carried": the value
			// really selects the directory, so a bad value fails naming it.
			// Dropping --var would make this row pass by validating ./migrations.
			name: "a --var value that names nothing still fails naming it",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_init.sql", "CREATE TABLE t (id INTEGER);\n")
				writeAtlasProjectFile(c, root)
			},
			args: []string{"migrate", "validate", "--env", "local", "--var", "d=nosuchdir"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Contains, "nosuchdir")
			},
		},
		{
			// #1241 item 7's boundary. A directory that DOES hold migrations
			// and carries no atlas.sum is still refused, byte-identically to
			// the pinned binary.
			name: "unhashed directory holding a migration is still refused",
			setup: func(c *qt.C, root string) {
				dir := filepath.Join(root, "migrations")
				c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_init.sql"), []byte("CREATE TABLE t (id INTEGER);\n"), 0o600), qt.IsNil)
			},
			args: []string{"migrate", "validate", "--dir", "file://migrations"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.ErrorMatches, "checksum file not found", qt.Commentf("%s", output))
				c.Assert(output, qt.Contains, "You have a checksum error in your migration directory.")
			},
		},
		{
			// The same boundary on the converted path: the covered set is not
			// empty, so the missing integrity file is still drift.
			name: "unhashed foreign-layout directory holding a migration is still refused",
			setup: func(c *qt.C, root string) {
				dir := filepath.Join(root, "migrations")
				c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(dir, "V1__init.sql"), []byte("CREATE TABLE t (id INTEGER);\n"), 0o600), qt.IsNil)
			},
			args: []string{"migrate", "validate", "--dir", "file://migrations?format=flyway"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.ErrorMatches, "checksum file not found", qt.Commentf("%s", output))
			},
		},
		{
			// #1241 item 13, a DELIBERATE divergence rather than a cell to
			// close. Measured on the pinned binary with both ./migrations and
			// a hashed mig2 present, `migrate status --url … file://mig2`
			// exits 0 reporting on ./migrations: the directory the caller
			// named is discarded and the answer is about another one. The
			// refusal stays, and it names the flag the value belongs on.
			name: "status still refuses a trailing positional, and says where it belongs",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_init.sql", "CREATE TABLE t (id INTEGER);\n")
			},
			args: []string{"migrate", "status", "--url", "sqlite://local.db?_fk=1", "file://mig2"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Contains, `unexpected positional arguments ["file://mig2"]`)
				c.Assert(err.Error(), qt.Contains, "name the migration directory with --dir")
			},
		},
		{
			// The same divergence on the verb that forwards to a native
			// command. Without the wrapper's own check this row still refuses,
			// but with the forwarded command's shorter wording, which does not
			// say where the value belongs.
			name: "validate still refuses a trailing positional, and says where it belongs",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_init.sql", "CREATE TABLE t (id INTEGER);\n")
			},
			args: []string{"migrate", "validate", "--dir", "file://migrations", "stray"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Contains, `unexpected positional arguments ["stray"]`)
				c.Assert(err.Error(), qt.Contains, "name the migration directory with --dir")
			},
		},
		{
			name:  "schema inspect still refuses a trailing positional, and says where it belongs",
			setup: func(_ *qt.C, _ string) {},
			args:  []string{"schema", "inspect", "-u", "sqlite://local.db?_fk=1", "stray"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Contains, "name the database with -u/--url")
			},
		},
		{
			// The other half of "--var does not require a project file": it is
			// still READ. The pinned binary parses --var while parsing flags,
			// before it looks for an atlas.hcl, so `--var foo` is refused with
			// no project file in sight. Measured on 2026-08-08 in this exact
			// fixture -- a hashed ./migrations, no atlas.hcl -- pinned binary
			// exit 1, ptah-compat before the check exit 0.
			name: "a malformed --var is refused with no atlas.hcl in sight",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_init.sql", "CREATE TABLE t (id INTEGER);\n")
			},
			args: []string{"migrate", "validate", "--dir", "file://migrations", "--var", "foo"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Equals,
					`invalid argument "foo" for "--var" flag: variables must be format as key=value, got: "foo"`)
			},
		},
		{
			// The row that would have caught it. The accepted-cells table above
			// only ever passes `--var foo=bar` to `migrate new`, so a check
			// deleted rather than moved passes every row there -- while this
			// verb WRITES. The pinned binary refuses this argv and leaves the
			// directory untouched; ptah-compat created ./migrations, a
			// timestamped migration and an atlas.sum on it.
			name:  "a malformed --var on a writing verb writes nothing",
			setup: func(_ *qt.C, _ string) {},
			args:  []string{"migrate", "new", "nm", "--var", "foo"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Equals,
					`invalid argument "foo" for "--var" flag: variables must be format as key=value, got: "foo"`)
				assertDirEmpty(c, root)
			},
		},
		{
			// The same hole on the parsed-flag surface, which had it before
			// this branch existed: `schema inspect --url … --var foo` was
			// pinned binary 1 / ptah-compat 0, and it printed a schema.
			name:  "a malformed --var is refused on the parsed-flag surface too",
			setup: func(_ *qt.C, _ string) {},
			args:  []string{"schema", "inspect", "--url", "sqlite://local.db?_fk=1", "--var", "foo"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Equals,
					`invalid argument "foo" for "--var" flag: variables must be format as key=value, got: "foo"`)
			},
		},
		{
			// The value is CSV and every field is checked, so a well-formed
			// first field does not license a bare second one. Measured
			// 2026-08-08: both binaries exit 1 naming `b`, not the whole value.
			// Without this row, collapsing the rule to one Contains over the raw
			// string reddens nothing.
			name: "a malformed field in a comma-separated --var is refused, naming that field",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_init.sql", "CREATE TABLE t (id INTEGER);\n")
			},
			args: []string{"migrate", "validate", "--dir", "file://migrations", "--var", "a=1,b"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Equals,
					`invalid argument "a=1,b" for "--var" flag: variables must be format as key=value, got: "b"`)
			},
		},
		{
			// The reader's own failures are the pinned binary's wording too: an
			// empty value is `EOF`, from the CSV reader rather than from any rule
			// written here. Measured 2026-08-08, exit 1 on both.
			name: "an empty --var is refused with the CSV reader's own error",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_init.sql", "CREATE TABLE t (id INTEGER);\n")
			},
			args: []string{"migrate", "validate", "--dir", "file://migrations", "--var", ""},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Equals, `invalid argument "" for "--var" flag: EOF`)
			},
		},
		{
			// A verb that never CONSUMES --var. Checking the value inside the
			// consumers left every such verb unchecked, and --var is registered
			// on the group's PersistentFlags, so every one of them accepts it.
			// Measured on 2026-08-08 in this fixture: the pinned binary exits 1
			// and leaves a.hcl alone; ptah-compat with the check in the
			// consumers exited 0 and REFORMATTED it. The row asserts the bytes
			// are untouched, so it cannot be satisfied by a fmt that merely
			// found nothing to do.
			name: "a malformed --var is refused on a verb that never reads it",
			setup: func(c *qt.C, root string) {
				c.Assert(os.WriteFile(filepath.Join(root, "a.hcl"), []byte(unformattedAtlasHCL), 0o600), qt.IsNil)
			},
			args: []string{"schema", "fmt", "--var", "foo"},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Equals,
					`invalid argument "foo" for "--var" flag: variables must be format as key=value, got: "foo"`)
				body, readErr := os.ReadFile(filepath.Join(root, "a.hcl"))
				c.Assert(readErr, qt.IsNil)
				c.Assert(string(body), qt.Equals, unformattedAtlasHCL)
			},
		},
		{
			// The same hole on a verb that never reads --var AND WRITES. This
			// is the destructive half: measured on 2026-08-08 the pinned binary
			// exits 1 leaving the directory as it found it, while ptah-compat
			// exited 0 and created ./dst holding two converted migrations and a
			// freshly computed atlas.sum. The row asserts the destination is
			// still absent, which is what an exit code alone would not catch.
			name: "a malformed --var on migrate import writes no destination",
			setup: func(c *qt.C, root string) {
				src := filepath.Join(root, "src")
				c.Assert(os.MkdirAll(src, 0o755), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(src, "V1__a.sql"), []byte("CREATE TABLE a (id INTEGER);\n"), 0o600), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(src, "V2__b.sql"), []byte("CREATE TABLE b (id INTEGER);\n"), 0o600), qt.IsNil)
			},
			args: []string{
				"migrate", "import",
				"--from", "file://src",
				"--to", "file://dst",
				"--dir-format", "flyway",
				"--var", "foo",
			},
			assert: func(c *qt.C, root string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Equals,
					`invalid argument "foo" for "--var" flag: variables must be format as key=value, got: "foo"`)
				assertPathAbsent(c, filepath.Join(root, "dst"))
			},
		},
		{
			// The group command carries the flag itself, so it is a cell too.
			// Pinned binary exit 1; ptah-compat printed the group's help at
			// exit 0.
			name:  "a malformed --var is refused on the schema group itself",
			setup: func(_ *qt.C, _ string) {},
			args:  []string{"schema", "--var", "foo"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Equals,
					`invalid argument "foo" for "--var" flag: variables must be format as key=value, got: "foo"`)
			},
		},
		{
			name:  "a malformed --var is refused on the migrate group itself",
			setup: func(_ *qt.C, _ string) {},
			args:  []string{"migrate", "--var", "foo"},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Equals,
					`invalid argument "foo" for "--var" flag: variables must be format as key=value, got: "foo"`)
			},
		},
		{
			// #1241 item 7's other boundary, and the one the empty-directory
			// relaxation uncovered. An empty directory exits 0 only when a
			// scope selector was given; with none, the pinned binary refuses
			// before it reads the directory or connects to --dev-url, and it
			// stays refused here. Against a MySQL 9.7 dev schema holding one
			// table, ptah-compat without this check exited 0 and DROPPED the
			// table on the same argv.
			name: "lint with no --latest and no --git-base is still refused",
			setup: func(c *qt.C, root string) {
				c.Assert(os.MkdirAll(filepath.Join(root, "migrations"), 0o755), qt.IsNil)
			},
			args: []string{
				"migrate", "lint",
				"--dir", "file://migrations",
				"--dev-url", "sqlite://file?mode=memory",
			},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Equals, "--latest or --git-base is required")
			},
		},
		{
			// The same refusal on a directory that DOES hold migrations, so the
			// row cannot be satisfied by anything the empty-directory change
			// touched. Pinned binary exit 1 here too.
			name: "lint with no scope is refused on a populated directory too",
			setup: func(c *qt.C, root string) {
				writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_init.sql", "CREATE TABLE t (id INTEGER);\n")
			},
			args: []string{
				"migrate", "lint",
				"--dir", "file://migrations",
				"--dev-url", "sqlite://file?mode=memory",
			},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Equals, "--latest or --git-base is required")
			},
		},
		{
			// A directory holding SQL that carries no version still refuses
			// --latest: the selector was asked to order a set it cannot order,
			// and answering "nothing to lint" there would lint nothing quietly.
			name: "lint --latest over unversioned files is still refused",
			setup: func(c *qt.C, root string) {
				dir := filepath.Join(root, "migrations")
				c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(dir, "notaversion.sql"), []byte("CREATE TABLE t (id INTEGER);\n"), 0o600), qt.IsNil)
			},
			args: []string{
				"migrate", "lint",
				"--dir", "file://migrations",
				"--dev-url", "sqlite://file?mode=memory",
				"--latest", "1",
			},
			assert: func(c *qt.C, _ string, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(err.Error(), qt.Contains, "--latest requires versioned migration files")
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

// TestCompatMigrateLintScopeOptIn pins the capability the scope refusal would
// otherwise delete. Linting a whole directory with no selector is something
// Ptah's linter can do and the pinned binary cannot; defaulting to that
// binary's refusal is the drop-in requirement, and
// PTAH_ATLAS_LINT_ALL_VERSIONS=1 keeps the fuller behavior reachable on this
// same surface rather than only through native `ptah` (AGENTS.md,
// "Compatibility never removes a capability").
func TestCompatMigrateLintScopeOptIn(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	t.Setenv("PTAH_ATLAS_LINT_ALL_VERSIONS", "1")
	writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_init.sql", "CREATE TABLE t (id INTEGER);\n")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--dir", "file://migrations",
		"--dev-url", "sqlite://file?mode=memory",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, "-- 1 version ok")
}

// TestCompatMigrateLintScopeOptInIgnoresNonBoolean keeps the opt-in from being
// a check that anything at all disables. Unset, empty, false and unparsable
// values all keep the default refusal.
func TestCompatMigrateLintScopeOptInIgnoresNonBoolean(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	t.Setenv("PTAH_ATLAS_LINT_ALL_VERSIONS", "yes please")
	writeHashedAtlasDir(c, filepath.Join(root, "migrations"), "20240101000000_init.sql", "CREATE TABLE t (id INTEGER);\n")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--dir", "file://migrations",
		"--dev-url", "sqlite://file?mode=memory",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "--latest or --git-base is required", qt.Commentf("%s", out.String()))
}

// writeAtlasProjectFile writes an atlas.hcl whose migration directory is chosen
// by a `--var`, so a test can tell a variable that reached the project file
// from one that was dropped.
func writeAtlasProjectFile(c *qt.C, root string) {
	c.Helper()
	body := "variable \"d\" {\n  type = string\n}\n\nenv \"local\" {\n  migration {\n    dir = \"file://${var.d}\"\n  }\n}\n"
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(body), 0o600), qt.IsNil)
}
