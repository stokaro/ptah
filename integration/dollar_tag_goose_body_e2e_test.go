//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// gooseBodiesWithABlankLine are goose migrations whose function body holds a
// blank line. A conversion drops the blank lines between statements and keeps
// the ones inside a body, so the server stores the body with its blank line.
// Read with a tag rule that allows only ASCII, a tag outside ASCII opens no
// body. Read with a dollar sign inside a name opening a body, the name a$$b
// opens one that the real body's opening tag closes, and the blank line inside
// the real body is dropped.
var gooseBodiesWithABlankLine = []struct {
	name, migration string
}{
	{
		name: "a tag outside ASCII",
		migration: "-- +goose Up\n-- +goose StatementBegin\n" +
			"CREATE FUNCTION goose_body() RETURNS void LANGUAGE plpgsql AS $\u00e9$\nBEGIN\n\nEND\n$\u00e9$;\n" +
			"-- +goose StatementEnd\n-- +goose Down\nDROP FUNCTION goose_body;\n",
	},
	{
		name: "a name that holds two dollar signs",
		migration: "-- +goose Up\nCREATE TABLE a$$b (id int);\n-- +goose StatementBegin\n" +
			"CREATE FUNCTION goose_body() RETURNS void LANGUAGE plpgsql AS $$\nBEGIN\n\nEND\n$$;\n" +
			"-- +goose StatementEnd\n-- +goose Down\nDROP FUNCTION goose_body;\n",
	},
}

// TestMigrateApplyKeepsABlankLineInsideAGooseBodyE2E applies each of
// gooseBodiesWithABlankLine as a goose directory on PostgreSQL and YugabyteDB,
// which store a function body as written, and reads the body back.
func TestMigrateApplyKeepsABlankLineInsideAGooseBodyE2E(t *testing.T) {
	for _, engine := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
	} {
		for _, test := range gooseBodiesWithABlankLine {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				target := probeEscapeTargets[engine.engine](c)
				dir := filepath.Join(c.TempDir(), "migrations")
				c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(dir, "1_body.sql"), []byte(test.migration), 0o600), qt.IsNil)
				dirURL := "file://" + filepath.ToSlash(dir) + "?format=goose"
				hashed, err := runCompatVerb("migrate", "hash", "--dir", dirURL)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", hashed))

				applied, err := runCompatVerb("migrate", "apply", "--dir", dirURL, "--url", target.url)

				c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
				db, err := sql.Open(target.driver, target.driverURL)
				c.Assert(err, qt.IsNil)
				c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
				var body string
				c.Assert(db.QueryRowContext(c.Context(), "SELECT prosrc FROM pg_proc WHERE proname = 'goose_body'").Scan(&body),
					qt.IsNil)
				c.Assert(body, qt.Equals, "\nBEGIN\n\nEND\n")
			})
		}
	}
}
