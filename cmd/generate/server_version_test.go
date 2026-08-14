package generate_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/generate"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

// unevenlyReferencedFixture writes the schema from stokaro/ptah#916 that a
// MySQL 8.0 server accepts and a MySQL 8.4 server does not: a foreign key onto
// a column carrying a plain index rather than a unique one.
//
// It is the fixture the issue's completion criterion 4 names, and it is the
// reason --server-version exists: MySQL84 sets
// foreign_keys_require_unique_reference and is what ForDialect("mysql")
// returns, so an offline render refuses a schema half the supported MySQL
// installed base runs today.
func unevenlyReferencedFixture(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	source := `package models

//ptah:schema:table name="parent"
type Parent struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="code" type="VARCHAR(32)" not_null="true"
	//ptah:schema:index name="idx_parent_code" fields="code"
	Code string
}

//ptah:schema:table name="child"
type Child struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="parent_code" type="VARCHAR(32)" not_null="true" foreign="parent(code)" foreign_key_name="fk_child_parent_code"
	ParentCode string
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "models.go"), []byte(source), 0o600), qt.IsNil)
	return dir
}

func renderFixture(c *qt.C, args ...string) (stdoutText, stderrText string, executeErr error) {
	c.Helper()
	cmd := generate.NewGenerateCommand()
	cmd.SetArgs(args)
	return executeGenerate(c, cmd)
}

// TestGenerateCommand_ServerVersionPinsTheCapabilityPreset is the measurement
// the flag is for.
//
// Each row drives the real command over the same fixture and reads the exit
// code, because the defect is not that a preset was selected wrongly in a unit
// — it is that the command had no spelling at all for saying which server it
// was rendering for.
func TestGenerateCommand_ServerVersionPinsTheCapabilityPreset(t *testing.T) {
	tests := []struct {
		name   string
		args   func(dir string) []string
		assert func(c *qt.C, stdout string, err error)
	}{
		{
			name: "mysql without a version refuses the fixture",
			args: func(dir string) []string {
				return []string{"--root-dir", dir, "--dialect", "mysql"}
			},
			assert: func(c *qt.C, stdout string, err error) {
				c.Assert(err, qt.IsNotNil)
				c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
				c.Assert(err.Error(), qt.Contains, "to be declared unique")
				c.Assert(stdout, qt.Equals, "")
			},
		},
		{
			name: "mysql 8.0.42 renders it",
			args: func(dir string) []string {
				return []string{"--root-dir", dir, "--dialect", "mysql", "--server-version", "8.0.42"}
			},
			assert: func(c *qt.C, stdout string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(stdout, qt.Contains, "fk_child_parent_code")
			},
		},
		{
			name: "mysql 8.4.0 still refuses it",
			args: func(dir string) []string {
				return []string{"--root-dir", dir, "--dialect", "mysql", "--server-version", "8.4.0"}
			},
			assert: func(c *qt.C, stdout string, err error) {
				c.Assert(err, qt.IsNotNil)
				c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
				c.Assert(err.Error(), qt.Contains, "to be declared unique")
			},
		},
		{
			name: "mariadb without a version keeps rendering it",
			args: func(dir string) []string {
				return []string{"--root-dir", dir, "--dialect", "mariadb"}
			},
			assert: func(c *qt.C, stdout string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(stdout, qt.Contains, "fk_child_parent_code")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := unevenlyReferencedFixture(c)

			stdout, _, err := renderFixture(c, test.args(dir)...)

			test.assert(c, stdout, err)
		})
	}
}

// TestGenerateCommand_ServerVersionRefusesAValueThatNamesNoServer is the half
// stokaro/ptah#1466 established on `ptah sql lint` and this change reuses
// rather than re-derives: capability.ForServerVersion answers an unreadable
// string with the dialect default and says nothing, so accepting one here
// would render against a preset the operator did not ask for while their
// version string sat in the scrollback looking honored.
func TestGenerateCommand_ServerVersionRefusesAValueThatNamesNoServer(t *testing.T) {
	tests := []struct {
		name    string
		version string
	}{
		{name: "not a version at all", version: "not-a-version"},
		{name: "a MariaDB banner carrying no version", version: "MariaDB something"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := unevenlyReferencedFixture(c)

			stdout, _, err := renderFixture(c,
				"--root-dir", dir, "--dialect", "mariadb", "--server-version", test.version)

			c.Assert(err, qt.IsNotNil)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(err.Error(), qt.Contains, "invalid --server-version: ")
			c.Assert(err.Error(), qt.Contains, `"`+test.version+`" is not a recognized mariadb server version`)
			c.Assert(err.Error(), qt.Contains, "8.0.42")
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// TestGenerateCommand_ServerVersionRefusesABannerFromAnotherServer is the
// finding a reviewer raised on this change, reproduced and then closed.
//
// capability.ResolveServerVersion lets a product banner outrank the declared
// dialect, which is right on a live connection — MariaDB announces itself over
// the MySQL protocol and CockroachDB over the PostgreSQL one — and wrong for
// two values a person typed. Measured before the refusal existed:
//
//	--dialect mysql                                   -> exit 2, "declared unique"
//	--dialect mysql --server-version 10.11.6-MariaDB  -> exit 0, MySQL DDL
//
// So the flag could lift a refusal by naming a server the render was never
// going to target, and the SQL it emitted was attributed to the wrong engine.
func TestGenerateCommand_ServerVersionRefusesABannerFromAnotherServer(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		version string
		names   string
	}{
		{
			name:    "a MariaDB banner on mysql",
			dialect: "mysql",
			version: "10.11.6-MariaDB",
			names:   "mariadb",
		},
		{
			name:    "a CockroachDB banner on sqlite",
			dialect: "sqlite",
			version: "CockroachDB CCL v25.4.5",
			names:   "cockroachdb",
		},
		{
			name:    "a YugabyteDB banner on postgres",
			dialect: "postgres",
			version: "PostgreSQL 15.2-YB-2026.1.0.0-b0",
			names:   "yugabytedb",
		},
		{
			// The PostgreSQL banner has no product substring of its own in
			// the shapes the flag documents, so before it was detected this
			// one parsed as the numeric MySQL version 16.3 and rendered
			// against MySQL84.
			name:    "a PostgreSQL banner on mysql",
			dialect: "mysql",
			version: "PostgreSQL 16.3 (Debian)",
			names:   "postgres",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := unevenlyReferencedFixture(c)

			stdout, _, err := renderFixture(c,
				"--root-dir", dir, "--dialect", test.dialect, "--server-version", test.version)

			c.Assert(err, qt.IsNotNil)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(err.Error(), qt.Contains,
				`"`+test.version+`" names a `+test.names+` server, but the target dialect is `+test.dialect)
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// TestGenerateCommand_ServerVersionAcceptsAMatchingBanner is the control for
// the test above: the refusal is about a contradiction, not about banners.
//
// Each of these is the same product named twice, and each must still resolve.
// A refusal that also caught these would make the documented banner shapes
// unusable on the flag that documents them.
func TestGenerateCommand_ServerVersionAcceptsAMatchingBanner(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		version string
	}{
		{name: "MariaDB banner on mariadb", dialect: "mariadb", version: "10.11.6-MariaDB"},
		{name: "CockroachDB banner on cockroachdb", dialect: "cockroachdb", version: "CockroachDB CCL v25.4.5"},
		{name: "YugabyteDB banner on yugabytedb", dialect: "yugabytedb", version: "PostgreSQL 15.2-YB-2026.1.0.0-b0"},
		{name: "PostgreSQL banner on postgres", dialect: "postgres", version: "PostgreSQL 16.3 (Debian)"},
		{name: "a plain dotted version on mysql", dialect: "mysql", version: "8.0.42"},
		{name: "a plain dotted version on cockroachdb", dialect: "cockroachdb", version: "25.4.5"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := unevenlyReferencedFixture(c)

			_, _, err := renderFixture(c,
				"--root-dir", dir, "--dialect", test.dialect, "--server-version", test.version)

			c.Assert(errorText(err), qt.Not(qt.Contains), "invalid --server-version")
		})
	}
}

// errorText renders an error for a Contains assertion without branching in a
// test body: a nil error contributes the empty string, which contains nothing.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestGenerateCommand_ServerVersionIsRefusedBeforeAnySchemaIsRead pins the
// order the refusal happens in.
//
// A --root-dir that does not exist is a second, louder error. If the version
// were resolved after the schema load, this invocation would report the
// missing directory and the operator would fix the wrong thing; the version is
// theirs to correct and nothing else has to succeed before they can be told.
func TestGenerateCommand_ServerVersionIsRefusedBeforeAnySchemaIsRead(t *testing.T) {
	c := qt.New(t)

	missing := filepath.Join(c.TempDir(), "no-such-directory")

	_, _, err := renderFixture(c,
		"--root-dir", missing, "--dialect", "mysql", "--server-version", "not-a-version")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "invalid --server-version: ")
	c.Assert(err.Error(), qt.Not(qt.Contains), "no-such-directory")
}

// TestGenerateCommand_ServerVersionRequiresADialect covers the one combination
// that cannot mean anything: with no --dialect the command renders all nine
// supported targets in one pass, and a single server version does not describe
// nine engines.
func TestGenerateCommand_ServerVersionRequiresADialect(t *testing.T) {
	c := qt.New(t)

	dir := unevenlyReferencedFixture(c)

	stdout, _, err := renderFixture(c, "--root-dir", dir, "--server-version", "8.0.42")

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(err.Error(), qt.Contains, "--server-version requires --dialect")
	c.Assert(stdout, qt.Equals, "")
}

// TestGenerateCommand_AnUnknownDialectIsReportedBeforeTheVersion pins which of
// two wrong flags gets named.
//
// A dialect Ptah does not support has no capability ladder, so a complaint
// about the version would be true and useless: "not a recognized oracle server
// version" sends the operator looking for the right Oracle version string, and
// there is no such thing. The dialect is the flag they have to fix, and it is
// the renderer that names it.
//
// The version here is deliberately one that would be refused against a real
// dialect, so this measures the order of the two refusals rather than the
// absence of one.
func TestGenerateCommand_AnUnknownDialectIsReportedBeforeTheVersion(t *testing.T) {
	c := qt.New(t)

	dir := unevenlyReferencedFixture(c)

	_, _, err := renderFixture(c,
		"--root-dir", dir, "--dialect", "oracle", "--server-version", "not-a-version")

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(err.Error(), qt.Contains, "unsupported database dialect: oracle")
	c.Assert(err.Error(), qt.Not(qt.Contains), "--server-version")
}

// TestGenerateCommand_ServerVersionAnnouncesWhatItPlannedInstead keeps the
// resolver's three non-exact outcomes visible on the command.
//
// Silence is reserved for a version that selected an exact measured release
// line. Every other outcome planned against something the operator did not
// name, and a render that says nothing there reads as a render against the
// server they asked for.
func TestGenerateCommand_ServerVersionAnnouncesWhatItPlannedInstead(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		version string
		want    string
	}{
		{
			name:    "between measured lines",
			dialect: "mysql",
			version: "8.0.42",
			want:    "is not a measured release line",
		},
		{
			name:    "past the top of the ladder",
			dialect: "postgres",
			version: "99.0",
			want:    "newer than the newest measured release line",
		},
		{
			name:    "a dialect with no ladder at all",
			dialect: "sqlite",
			version: "3.53.0",
			want:    "no measured version ladder",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := unevenlyReferencedFixture(c)

			_, stderr, _ := renderFixture(c,
				"--root-dir", dir, "--dialect", test.dialect, "--server-version", test.version)

			c.Assert(stderr, qt.Contains, "warning: ")
			c.Assert(stderr, qt.Contains, test.want)
		})
	}
}

// TestGenerateCommand_AnExactMeasuredLineSaysNothing is the control for the
// test above: a warning printed on every run is a warning nobody reads.
func TestGenerateCommand_AnExactMeasuredLineSaysNothing(t *testing.T) {
	c := qt.New(t)

	dir := unevenlyReferencedFixture(c)

	_, stderr, err := renderFixture(c,
		"--root-dir", dir, "--dialect", "mariadb", "--server-version", "10.11.6-MariaDB")

	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Not(qt.Contains), "warning: ")
}

// TestGenerateCommand_WithoutServerVersionRendersExactlyAsBefore is the
// non-interference control.
//
// The flag replaced renderer.GetOrderedCreateStatements with its
// capability-taking sibling on every render, pinned or not. An unpinned render
// must therefore still be byte-identical to the dialect default, on a dialect
// whose default is not the newest thing in its ladder.
func TestGenerateCommand_WithoutServerVersionRendersExactlyAsBefore(t *testing.T) {
	c := qt.New(t)

	dir := unevenlyReferencedFixture(c)

	unpinned, _, err := renderFixture(c, "--root-dir", dir, "--dialect", "mariadb")
	c.Assert(err, qt.IsNil)

	pinned, _, err := renderFixture(c,
		"--root-dir", dir, "--dialect", "mariadb", "--server-version", "10.11.6-MariaDB")
	c.Assert(err, qt.IsNil)

	c.Assert(unpinned, qt.Equals, pinned)
	c.Assert(unpinned, qt.Contains, "fk_child_parent_code")
}
