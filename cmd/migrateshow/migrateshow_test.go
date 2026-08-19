package migrateshow_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/migrateshow"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// The bodies below deliberately differ in every direction and version, so an
// implementation that printed the wrong half or the wrong migration cannot pass
// by printing something that looks like SQL.
const (
	upOne     = "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"
	downOne   = "DROP TABLE widgets;\n"
	upTwo     = "ALTER TABLE widgets ADD COLUMN name TEXT;\n"
	downTwo   = "ALTER TABLE widgets DROP COLUMN name;\n"
	atlasOne  = "CREATE TABLE gadgets (id INTEGER PRIMARY KEY);\n"
	atlasTwo  = "ALTER TABLE gadgets ADD COLUMN name TEXT;\n"
	unhashedU = "CREATE TABLE unhashed (id INTEGER PRIMARY KEY);\n"
)

// ptahDir writes a hashed two-migration directory in Ptah's reversible layout.
func ptahDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"0000000001_init.up.sql":   upOne,
		"0000000001_init.down.sql": downOne,
		"0000000002_name.up.sql":   upTwo,
		"0000000002_name.down.sql": downTwo,
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatPtah)
	c.Assert(err, qt.IsNil)
	return dir
}

// atlasDir writes a hashed two-migration directory in the Atlas layout, where a
// migration is one file that names no direction.
func atlasDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"20240101000000_init.sql":     atlasOne,
		"20240102000000_add_name.sql": atlasTwo,
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

func unhashedDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"),
		[]byte(unhashedU), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"),
		[]byte(downOne), 0o600), qt.IsNil)
	return dir
}

func execute(args ...string) (stdout, stderr string, err error) {
	cmd := migrateshow.NewMigrateShowCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// TestShow_PrintsAReversibleDirectory pins what reaches standard output for a
// directory whose migrations are pairs: the file's own bytes, and nothing this
// command added to them.
func TestShow_PrintsAReversibleDirectory(t *testing.T) {
	tests := []struct {
		name string
		// args builds the invocation from the fixture directory. It asserts
		// nothing; it is the input the row varies.
		args func(dir string) []string
		want string
	}{
		{
			name: "the up half by default",
			args: func(dir string) []string {
				return []string{"--migrations-dir", dir, "--version", "1"}
			},
			want: upOne,
		},
		{
			name: "the down half on request",
			args: func(dir string) []string {
				return []string{"--migrations-dir", dir, "--version", "1", "--direction", "down"}
			},
			want: downOne,
		},
		{
			name: "a leading-zero version names the same migration",
			args: func(dir string) []string {
				return []string{"--migrations-dir", dir, "--version", "0000000001"}
			},
			want: upOne,
		},
		{
			name: "two versions, separated by a blank line",
			args: func(dir string) []string {
				return []string{"--migrations-dir", dir, "--version", "1", "--version", "2"}
			},
			want: upOne + "\n" + upTwo,
		},
		{
			name: "the order asked for, not the order on disk",
			args: func(dir string) []string {
				return []string{"--migrations-dir", dir, "--version", "2", "--version", "1"}
			},
			want: upTwo + "\n" + upOne,
		},
		{
			name: "a version named twice is printed once",
			args: func(dir string) []string {
				return []string{"--migrations-dir", dir, "--version", "1", "--version", "1"}
			},
			want: upOne,
		},
		{
			name: "the down halves of two versions",
			args: func(dir string) []string {
				return []string{
					"--migrations-dir", dir,
					"--version", "1", "--version", "2", "--direction", "down",
				}
			},
			want: downOne + "\n" + downTwo,
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

// TestShow_PrintsAnAtlasDirectory covers the layout where a migration is one
// file naming no direction, which is what the compatibility spelling reaches.
func TestShow_PrintsAnAtlasDirectory(t *testing.T) {
	tests := []struct {
		name string
		args func(dir string) []string
		want string
	}{
		{
			name: "one migration",
			args: func(dir string) []string {
				return []string{"--migrations-dir", dir, "--version", "20240101000000"}
			},
			want: atlasOne,
		},
		{
			name: "two migrations",
			args: func(dir string) []string {
				return []string{
					"--migrations-dir", dir,
					"--version", "20240101000000", "--version", "20240102000000",
				}
			},
			want: atlasOne + "\n" + atlasTwo,
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

// TestShow_AddsNoTrailingNewline pins that what is printed is the file, not a
// rendering of it. A migration written without a final line feed is piped back
// into a database unchanged.
func TestShow_AddsNoTrailingNewline(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"),
		[]byte("SELECT 1;"), 0o600), qt.IsNil)

	stdout, _, err := execute("--migrations-dir", dir, "--version", "1")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Equals, "SELECT 1;")
}

// TestShow_PrintsNothingWhenOneRequestedVersionIsMissing is the whole reason
// every version is located before any of it is read: a run that failed halfway
// would leave the caller holding half a migration.
func TestShow_PrintsNothingWhenOneRequestedVersionIsMissing(t *testing.T) {
	c := qt.New(t)
	dir := ptahDir(c)

	stdout, _, err := execute("--migrations-dir", dir, "--version", "1", "--version", "99")

	c.Assert(err, qt.ErrorMatches, `migration version 99 not found in .*`)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "")
}

// TestShow_VerifySumAcceptsAHashedDirectory is the acceptance half of the gate,
// without which a gate that refused everything would look like a working one.
func TestShow_VerifySumAcceptsAHashedDirectory(t *testing.T) {
	c := qt.New(t)
	dir := ptahDir(c)

	stdout, _, err := execute("--migrations-dir", dir, "--version", "1", "--verify-sum")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Equals, upOne)
}

// TestShow_PrintsAnUnhashedDirectoryByDefault is the second half of that
// control: show runs no gate unless it is asked to.
func TestShow_PrintsAnUnhashedDirectoryByDefault(t *testing.T) {
	c := qt.New(t)
	dir := unhashedDir(c)

	stdout, _, err := execute("--migrations-dir", dir, "--version", "1")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Equals, unhashedU)
}

func TestShow_VerifySumRefusesAnUnhashedDirectory(t *testing.T) {
	c := qt.New(t)
	dir := unhashedDir(c)

	stdout, _, err := execute("--migrations-dir", dir, "--version", "1", "--verify-sum")

	c.Assert(err, qt.ErrorMatches, `migration sum verification failed: ptah\.sum not found.*`)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "")
}

func TestShow_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    func(dir string) []string
		wantErr string
	}{
		{
			name:    "no version at all",
			args:    func(dir string) []string { return []string{"--migrations-dir", dir} },
			wantErr: `--version is required`,
		},
		{
			name: "a version that is not a number",
			args: func(dir string) []string {
				return []string{"--migrations-dir", dir, "--version", "init"}
			},
			wantErr: `invalid --version "init": must be a positive integer`,
		},
		{
			name: "a version nobody wrote",
			args: func(dir string) []string {
				return []string{"--migrations-dir", dir, "--version", "99"}
			},
			wantErr: `migration version 99 not found in .*`,
		},
		{
			name: "a direction that does not exist",
			args: func(dir string) []string {
				return []string{"--migrations-dir", dir, "--version", "1", "--direction", "sideways"}
			},
			wantErr: `unknown --direction "sideways": expected up or down`,
		},
		{
			name: "a directory that is not there",
			args: func(dir string) []string {
				return []string{"--migrations-dir", filepath.Join(dir, "nope"), "--version", "1"}
			},
			wantErr: `open migrations directory: .*`,
		},
		{
			name: "a positional argument",
			args: func(dir string) []string {
				return []string{"--migrations-dir", dir, "1"}
			},
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

// TestShow_RefusesADirectionAMigrationDoesNotHave keeps the two refusals apart:
// a version nobody wrote and a version written without a reverse are different
// mistakes, and telling them apart is what points at the fix.
func TestShow_RefusesADirectionAMigrationDoesNotHave(t *testing.T) {
	c := qt.New(t)
	dir := atlasDir(c)

	stdout, _, err := execute(
		"--migrations-dir", dir, "--version", "20240101000000", "--direction", "down")

	c.Assert(err, qt.ErrorMatches,
		`migration version 20240101000000 in .* has no down migration \(found 20240101000000_init\.sql\)`)
	c.Assert(stdout, qt.Equals, "")
}
