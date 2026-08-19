package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// These two verbs read a migration directory and print what they found. They
// contact no database and execute none of the SQL, which is what makes them
// measurable end to end from a temporary directory alone.

const (
	compatReadFirst  = "-- create \"users\" table\nCREATE TABLE `users` (`id` int NOT NULL);\n"
	compatReadSecond = "-- create \"pets\" table\nCREATE TABLE `pets` (`id` int NOT NULL);\n"
)

// compatReadDir writes a hashed two-migration directory in the Atlas layout.
func compatReadDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	compatReadFiles(c, dir)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

// compatReadUnhashedDir writes the same two migrations and no atlas.sum.
func compatReadUnhashedDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	compatReadFiles(c, dir)
	_, err := os.Stat(filepath.Join(dir, migratesum.AtlasFileName))
	c.Assert(os.IsNotExist(err), qt.IsTrue)
	return dir
}

func compatReadFiles(c *qt.C, dir string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_init.sql"),
		[]byte(compatReadFirst), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20240102000000_add_pets.sql"),
		[]byte(compatReadSecond), 0o600), qt.IsNil)
}

func TestCompatMigrateLs_ListsTheDirectory(t *testing.T) {
	tests := []struct {
		name string
		// args builds the invocation from the fixture directory. It asserts
		// nothing; it is the input the row varies.
		args func(dir string) []string
		want string
	}{
		{
			name: "every file, oldest version first",
			args: func(dir string) []string {
				return []string{"migrate", "ls", "--dir", "file://" + dir}
			},
			want: "20240101000000_init.sql\n20240102000000_add_pets.sql\n",
		},
		{
			name: "--short drops the description and the suffix",
			args: func(dir string) []string {
				return []string{"migrate", "ls", "--dir", "file://" + dir, "--short"}
			},
			want: "20240101000000\n20240102000000\n",
		},
		{
			name: "-s is the same flag",
			args: func(dir string) []string {
				return []string{"migrate", "ls", "--dir", "file://" + dir, "-s"}
			},
			want: "20240101000000\n20240102000000\n",
		},
		{
			name: "--latest prints the newest file",
			args: func(dir string) []string {
				return []string{"migrate", "ls", "--dir", "file://" + dir, "--latest"}
			},
			want: "20240102000000_add_pets.sql\n",
		},
		{
			name: "-l -s prints the newest version",
			args: func(dir string) []string {
				return []string{"migrate", "ls", "--dir", "file://" + dir, "-l", "-s"}
			},
			want: "20240102000000\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := compatReadDir(c)

			stdout, stderr, err := runCompat(test.args(dir)...)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, test.want)
			c.Assert(stderr, qt.Equals, "")
		})
	}
}

func TestCompatMigrateShow_PrintsMigrationBodies(t *testing.T) {
	tests := []struct {
		name string
		args func(dir string) []string
		want string
	}{
		{
			name: "one version",
			args: func(dir string) []string {
				return []string{"migrate", "show", "20240101000000", "--dir", "file://" + dir}
			},
			want: compatReadFirst,
		},
		{
			name: "a migration file name names the same migration",
			args: func(dir string) []string {
				return []string{"migrate", "show", "20240101000000_init.sql", "--dir", "file://" + dir}
			},
			want: compatReadFirst,
		},
		{
			name: "two versions, separated by a blank line",
			args: func(dir string) []string {
				return []string{
					"migrate", "show", "20240101000000", "20240102000000",
					"--dir", "file://" + dir,
				}
			},
			want: compatReadFirst + "\n" + compatReadSecond,
		},
		{
			name: "the order asked for, not the order on disk",
			args: func(dir string) []string {
				return []string{
					"migrate", "show", "20240102000000", "20240101000000",
					"--dir", "file://" + dir,
				}
			},
			want: compatReadSecond + "\n" + compatReadFirst,
		},
		{
			name: "a version named twice is printed once",
			args: func(dir string) []string {
				return []string{
					"migrate", "show", "20240101000000", "20240101000000",
					"--dir", "file://" + dir,
				}
			},
			want: compatReadFirst,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := compatReadDir(c)

			stdout, stderr, err := runCompat(test.args(dir)...)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, test.want)
			c.Assert(stderr, qt.Equals, "")
		})
	}
}

// TestCompatMigrateRead_RefusesADirectoryCarryingNoChecksum is the half of the
// contract the native default does not have.
//
// Natively these are read-only verbs outside the always-on integrity class, so
// a directory carrying no sum lists and prints at exit 0. This surface refuses
// it, and the adapter is what makes it: without the pinned --verify-sum the
// compatibility spelling would accept a directory the surface it mirrors
// refuses, which is the one direction the compatibility policy forbids.
func TestCompatMigrateRead_RefusesADirectoryCarryingNoChecksum(t *testing.T) {
	tests := []struct {
		name string
		args func(dir string) []string
	}{
		{
			name: "ls",
			args: func(dir string) []string {
				return []string{"migrate", "ls", "--dir", "file://" + dir}
			},
		},
		{
			name: "show",
			args: func(dir string) []string {
				return []string{"migrate", "show", "20240101000000", "--dir", "file://" + dir}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := compatReadUnhashedDir(c)

			stdout, stderr, err := runCompat(test.args(dir)...)

			c.Assert(err, qt.ErrorMatches, `migration sum verification failed: atlas\.sum not found.*`)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Contains, "atlas.sum not found")
		})
	}
}

// TestCompatMigrateRead_AcceptsAHashedDirectory is the acceptance control for
// the refusal above: a gate that refused every directory would satisfy the
// refusing half on its own.
func TestCompatMigrateRead_AcceptsAHashedDirectory(t *testing.T) {
	tests := []struct {
		name string
		args func(dir string) []string
		want string
	}{
		{
			name: "ls",
			args: func(dir string) []string {
				return []string{"migrate", "ls", "--dir", "file://" + dir, "--latest"}
			},
			want: "20240102000000_add_pets.sql\n",
		},
		{
			name: "show",
			args: func(dir string) []string {
				return []string{"migrate", "show", "20240101000000", "--dir", "file://" + dir}
			},
			want: compatReadFirst,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := compatReadDir(c)

			stdout, stderr, err := runCompat(test.args(dir)...)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, test.want)
		})
	}
}

// TestCompatMigrateRead_RefusesADirectoryNamingNoScheme extends the gate
// stokaro/ptah#1186 put on the writing verbs to the two read verbs added here,
// including the environment twin the flag alone would leave open.
func TestCompatMigrateRead_RefusesADirectoryNamingNoScheme(t *testing.T) {
	tests := []struct {
		name string
		// env is set before the invocation runs, so a row can name the
		// directory through the environment instead of the flag.
		env  map[string]string
		args func(dir string) []string
	}{
		{
			name: "ls through the --dir flag",
			args: func(dir string) []string { return []string{"migrate", "ls", "--dir", dir} },
		},
		{
			name: "show through the --dir flag",
			args: func(dir string) []string {
				return []string{"migrate", "show", "20240101000000", "--dir", dir}
			},
		},
		{
			name: "ls through the PTAH_DIR environment twin",
			env:  map[string]string{"PTAH_DIR": "carried-by-the-environment"},
			args: func(string) []string { return []string{"migrate", "ls"} },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := compatReadDir(c)
			for name, value := range test.env {
				c.Setenv(name, value)
			}

			stdout, stderr, err := runCompat(test.args(dir)...)

			// The message is the mirrored surface's, byte for byte, down to
			// the trailing space before the line feed. It goes out on stderr
			// from the command itself rather than only being returned, the way
			// the writing verbs' refusal does.
			c.Assert(err, qt.ErrorMatches, `missing scheme for dir url\. Did you mean ".*"\? `)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Matches, `Error: missing scheme for dir url\. Did you mean ".*"\? \n`)
		})
	}
}

// TestCompatMigrateRead_AcceptsADirectoryNamedByAtlasHCL is the control the
// scheme refusal above needs, and it is the one a value-mapping gate fails.
//
// `migration.dir` is normalized at parse time, so `file://migrations` and
// `migrations` are the same string by the time any verb sees one. A gate that
// cannot tell them apart refuses a project file the mirrored surface accepts,
// which is why the gate reads the layer the value came from rather than the
// value alone. Both spellings are driven here because a gate keyed to the
// wrong layer passes for one of them.
func TestCompatMigrateRead_AcceptsADirectoryNamedByAtlasHCL(t *testing.T) {
	tests := []struct {
		name string
		// dir is the value written into the project file's migration block.
		dir string
	}{
		{name: "a file:// URL", dir: "file://migrations"},
		{name: "a bare path", dir: "migrations"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			migrations := filepath.Join(root, "migrations")
			c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
			compatReadFiles(c, migrations)
			_, err := migratesum.WriteWithFormat(migrations, migrator.MigrationDirFormatAtlas)
			c.Assert(err, qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"),
				[]byte("env \"local\" {\n  migration {\n    dir = \""+test.dir+"\"\n  }\n}\n"),
				0o600), qt.IsNil)
			chdir(c, root)

			stdout, stderr, runErr := runCompat("migrate", "ls", "--env", "local")

			c.Assert(runErr, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals,
				"20240101000000_init.sql\n20240102000000_add_pets.sql\n")
		})
	}
}

// chdir moves the process into dir for one test and restores the previous
// working directory afterwards. The project file is found relative to the
// working directory, so a project-config test has to be in one.
func chdir(c *qt.C, dir string) {
	c.Helper()
	previous, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	c.Cleanup(func() {
		c.Assert(os.Chdir(previous), qt.IsNil)
	})
}

func TestCompatMigrateRead_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    func(dir string) []string
		wantErr string
	}{
		{
			name: "show with no version at all",
			args: func(dir string) []string {
				return []string{"migrate", "show", "--dir", "file://" + dir}
			},
			wantErr: `atlas migrate show requires version argument`,
		},
		{
			name: "show naming a version nobody wrote",
			args: func(dir string) []string {
				return []string{"migrate", "show", "19990101000000", "--dir", "file://" + dir}
			},
			wantErr: `migration version 19990101000000 not found in .*`,
		},
		{
			name: "show naming one good version and one missing one",
			args: func(dir string) []string {
				return []string{
					"migrate", "show", "20240101000000", "19990101000000",
					"--dir", "file://" + dir,
				}
			},
			wantErr: `migration version 19990101000000 not found in .*`,
		},
		{
			name: "ls carrying a native-only flag",
			args: func(dir string) []string {
				return []string{"migrate", "ls", "--migrations-dir", dir}
			},
			wantErr: `atlas migrate ls does not accept native Ptah flag --migrations-dir`,
		},
		{
			name: "show carrying a native-only flag",
			args: func(dir string) []string {
				return []string{"migrate", "show", "--version", "20240101000000", "--dir", "file://" + dir}
			},
			wantErr: `atlas migrate show does not accept native Ptah flag --version`,
		},
		{
			name: "ls carrying the native directory format flag",
			args: func(dir string) []string {
				return []string{"migrate", "ls", "--dir", "file://" + dir, "--dir-format", "ptah"}
			},
			wantErr: `atlas migrate ls does not accept native Ptah flag --dir-format`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := compatReadDir(c)

			stdout, _, err := runCompat(test.args(dir)...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// TestCompatMigrateRead_AbsentUnderStrictCommunityCompatibility pins the other
// half of where these verbs live.
//
// They are beyond the pinned community command set, so the strict profile must
// not register them at all: an unknown verb on a command group is that group's
// help, which is a different answer from a verb that exists and aborts.
func TestCompatMigrateRead_AbsentUnderStrictCommunityCompatibility(t *testing.T) {
	verbs := []struct {
		name string
		verb string
	}{
		{name: "ls", verb: "ls"},
		{name: "show", verb: "show"},
	}

	for _, verb := range verbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			root := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE())

			migrate, _, err := root.Find([]string{"migrate"})
			c.Assert(err, qt.IsNil)
			c.Assert(availableChildNames(migrate), qt.Not(qt.Contains), verb.verb)
		})
	}
}

// TestCompatMigrateRead_PresentUnderTheDefaultProfile is the control for the
// strict-mode absence above: deleting the verbs entirely would satisfy that
// test and this one is what notices.
func TestCompatMigrateRead_PresentUnderTheDefaultProfile(t *testing.T) {
	verbs := []struct {
		name string
		verb string
	}{
		{name: "ls", verb: "ls"},
		{name: "show", verb: "show"},
	}

	for _, verb := range verbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			root := atlas.NewCompatCommand("atlas")

			migrate, _, err := root.Find([]string{"migrate"})
			c.Assert(err, qt.IsNil)
			c.Assert(availableChildNames(migrate), qt.Contains, verb.verb)
		})
	}
}
