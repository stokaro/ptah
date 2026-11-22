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

// gooseBodyWithABlankLine is a goose migration whose function body is quoted
// with a tag outside ASCII and holds a blank line. A conversion drops the blank
// lines between statements and keeps the ones inside a body, so the server
// stores the body with its blank line. Read with a tag rule that allows only
// ASCII, the tag opens no body and the blank line inside it is dropped.
const gooseBodyWithABlankLine = "-- +goose Up\n-- +goose StatementBegin\n" +
	"CREATE FUNCTION goose_body() RETURNS void LANGUAGE plpgsql AS $é$\nBEGIN\n\nEND\n$é$;\n" +
	"-- +goose StatementEnd\n-- +goose Down\nDROP FUNCTION goose_body;\n"

// TestMigrateApplyKeepsABlankLineInsideAGooseBodyE2E applies
// gooseBodyWithABlankLine as a goose directory on PostgreSQL and YugabyteDB,
// which store a function body as written, and reads the body back.
func TestMigrateApplyKeepsABlankLineInsideAGooseBodyE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := probeEscapeTargets[test.engine](c)
			dir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "1_body.sql"), []byte(gooseBodyWithABlankLine), 0o600), qt.IsNil)
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
