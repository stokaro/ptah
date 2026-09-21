package migrationlintreport_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/migrationlintreport"
)

// writeOnlineRequireDir writes a directory whose policy requires the online
// mode and names no dialect, which is the shape where only the connection can
// say which engine the policy has to be measured against.
func writeOnlineRequireDir(c *qt.C) string {
	c.Helper()
	dir := c.TB.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, ".ptah-lint.yaml"),
		[]byte("online: require\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"),
		[]byte("CREATE TABLE orders (id integer, email text);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "2_index.sql"),
		[]byte("CREATE INDEX orders_email_idx ON orders (email);\n"), 0o600), qt.IsNil)
	return dir
}

// A policy that requires the online mode is refused on an engine the mode has
// no measurement for, and the dev database is where that engine is named.
//
// The configuration cannot make this call: it names no dialect, so it has
// nothing to check. Without the refusal the run lints as the connected product,
// finds no ON rule that runs there, and reports a clean directory for a
// guarantee nothing measured (stokaro/ptah#3466).
func TestBuild_OnlineModeRefusesALearnedDialectItCannotProve(t *testing.T) {
	c := qt.New(t)
	dir := writeOnlineRequireDir(c)

	_, err := migrationlintreport.Build(c.Context(), migrationlintreport.Options{
		Dir:     dir,
		DevURL:  "sqlite://" + filepath.Join(t.TempDir(), "dev.db"),
		FailOn:  migrationlintreport.FailOnError,
		Changed: migrationlintreport.ChangedOptions{Dir: true, DevURL: true},
	}, projectconfig.Config{})

	c.Assert(err, qt.ErrorMatches, `online: online mode covers .*; it is refused on "sqlite" .*`)
}

// The control: the same policy against an engine the mode covers runs, so the
// refusal above is about the engine rather than about the mode being selected
// at all.
func TestBuild_OnlineModeAcceptsALearnedDialectItCovers(t *testing.T) {
	c := qt.New(t)
	dir := writeOnlineRequireDir(c)

	report, err := migrationlintreport.Build(c.Context(), migrationlintreport.Options{
		Dir:     dir,
		Dialect: "postgres",
		FailOn:  migrationlintreport.FailOnNone,
		Changed: migrationlintreport.ChangedOptions{Dir: true, Dialect: true},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Dialect, qt.Equals, "postgres")
}
