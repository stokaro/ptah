package migratels_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/migratels"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// ptahDir writes a hashed two-migration directory in Ptah's reversible layout,
// where one migration is two files.
func ptahDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
		"0000000001_init.down.sql": "DROP TABLE widgets;\n",
		"0000000002_name.up.sql":   "ALTER TABLE widgets ADD COLUMN name TEXT;\n",
		"0000000002_name.down.sql": "ALTER TABLE widgets DROP COLUMN name;\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatPtah)
	c.Assert(err, qt.IsNil)
	return dir
}

// atlasDir writes a hashed two-migration directory in the Atlas layout, where
// one migration is one file.
func atlasDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"20240101000000_init.sql":     "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
		"20240102000000_add_name.sql": "ALTER TABLE widgets ADD COLUMN name TEXT;\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

// unhashedDir writes a migration directory carrying no integrity file at all.
func unhashedDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE widgets;\n"), 0o600), qt.IsNil)
	return dir
}

func execute(args ...string) (stdout, stderr string, err error) {
	cmd := migratels.NewMigrateLsCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// TestLs_ListsAReversibleDirectory pins the listing of a Ptah directory, where
// a migration is a pair of files and --short is what collapses the pair.
func TestLs_ListsAReversibleDirectory(t *testing.T) {
	tests := []struct {
		name string
		// args builds the invocation from the fixture directory. It asserts
		// nothing; it is the input the row varies.
		args func(dir string) []string
		want string
	}{
		{
			name: "every file, oldest version first",
			args: func(dir string) []string { return []string{"--migrations-dir", dir} },
			want: "0000000001_init.down.sql\n0000000001_init.up.sql\n" +
				"0000000002_name.down.sql\n0000000002_name.up.sql\n",
		},
		{
			name: "--short prints each version once",
			args: func(dir string) []string { return []string{"--migrations-dir", dir, "--short"} },
			want: "1\n2\n",
		},
		{
			name: "--latest keeps both halves of the newest migration",
			args: func(dir string) []string { return []string{"--migrations-dir", dir, "--latest"} },
			want: "0000000002_name.down.sql\n0000000002_name.up.sql\n",
		},
		{
			name: "--latest --short names the newest version",
			args: func(dir string) []string {
				return []string{"--migrations-dir", dir, "--latest", "--short"}
			},
			want: "2\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := ptahDir(c)

			stdout, stderr, err := execute(test.args(dir)...)

			c.Assert(err, qt.IsNil)
			c.Assert(stdout, qt.Equals, test.want)
			c.Assert(stderr, qt.Equals, "")
		})
	}
}

// TestLs_ListsAnAtlasDirectory is the same listing over the layout where one
// migration is one file, so a row that only ever saw pairs cannot pass by
// accident.
func TestLs_ListsAnAtlasDirectory(t *testing.T) {
	tests := []struct {
		name string
		args func(dir string) []string
		want string
	}{
		{
			name: "every file, oldest version first",
			args: func(dir string) []string { return []string{"--migrations-dir", dir} },
			want: "20240101000000_init.sql\n20240102000000_add_name.sql\n",
		},
		{
			name: "--short drops the description and the extension",
			args: func(dir string) []string { return []string{"--migrations-dir", dir, "--short"} },
			want: "20240101000000\n20240102000000\n",
		},
		{
			name: "--latest prints one file",
			args: func(dir string) []string { return []string{"--migrations-dir", dir, "--latest"} },
			want: "20240102000000_add_name.sql\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := atlasDir(c)

			stdout, stderr, err := execute(test.args(dir)...)

			c.Assert(err, qt.IsNil)
			c.Assert(stdout, qt.Equals, test.want)
			c.Assert(stderr, qt.Equals, "")
		})
	}
}

// TestLs_EmptyDirectoryListsNothing pins the answer for the first thing anyone
// does with a migration directory: list it before anything is in it. It carries
// no integrity file either, which is why --verify-sum has nothing to refuse.
func TestLs_EmptyDirectoryListsNothing(t *testing.T) {
	tests := []struct {
		name string
		args func(dir string) []string
	}{
		{
			name: "without the gate",
			args: func(dir string) []string { return []string{"--migrations-dir", dir} },
		},
		{
			name: "with the gate",
			args: func(dir string) []string { return []string{"--migrations-dir", dir, "--verify-sum"} },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()

			stdout, stderr, err := execute(test.args(dir)...)

			c.Assert(err, qt.IsNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "")
		})
	}
}

// TestLs_VerifySumAcceptsAHashedDirectory is the acceptance half of the gate.
// Without it, a gate that refused everything would look like a working one.
func TestLs_VerifySumAcceptsAHashedDirectory(t *testing.T) {
	c := qt.New(t)
	dir := ptahDir(c)

	stdout, _, err := execute("--migrations-dir", dir, "--verify-sum")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "0000000001_init.up.sql\n")
}

// TestLs_ListsAnUnhashedDirectoryByDefault is the second half of the same
// control: ls runs no gate unless it is asked to, because it is the verb an
// operator reaches for while diagnosing a directory that has already drifted.
func TestLs_ListsAnUnhashedDirectoryByDefault(t *testing.T) {
	c := qt.New(t)
	dir := unhashedDir(c)

	stdout, _, err := execute("--migrations-dir", dir)

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Equals, "0000000001_init.down.sql\n0000000001_init.up.sql\n")
}

func TestLs_VerifySumRefusesAnUnhashedDirectory(t *testing.T) {
	c := qt.New(t)
	dir := unhashedDir(c)

	stdout, _, err := execute("--migrations-dir", dir, "--verify-sum")

	c.Assert(err, qt.ErrorMatches, `migration sum verification failed: ptah\.sum not found.*`)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "")
}

func TestLs_VerifySumRefusesAnEditedDirectory(t *testing.T) {
	c := qt.New(t)
	dir := ptahDir(c)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE evil (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	stdout, _, err := execute("--migrations-dir", dir, "--verify-sum")

	c.Assert(err, qt.ErrorMatches, `(?s)migration sum verification failed:.*0000000001_init\.up\.sql.*`)
	c.Assert(stdout, qt.Equals, "")
}

func TestLs_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    func(dir string) []string
		wantErr string
	}{
		{
			name:    "a directory that is not there",
			args:    func(dir string) []string { return []string{"--migrations-dir", filepath.Join(dir, "nope")} },
			wantErr: `open migrations directory: .*`,
		},
		{
			name:    "an unknown directory format",
			args:    func(dir string) []string { return []string{"--migrations-dir", dir, "--dir-format", "goose"} },
			wantErr: `unknown migration directory format "goose".*`,
		},
		{
			name:    "a positional argument",
			args:    func(dir string) []string { return []string{"--migrations-dir", dir, "stray"} },
			wantErr: `unexpected positional arguments.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := ptahDir(c)

			stdout, _, err := execute(test.args(dir)...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stdout, qt.Equals, "")
		})
	}
}
