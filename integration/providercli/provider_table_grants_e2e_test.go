//go:build integration

package providercli_test

import (
	"database/sql"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testutils"
)

// tableGrantsFixture is the CLI over a fixture database holding items with a
// CHECK, served by the provider that rebuilds a table whose constraints change
// and writes the rebuild as text, and whose read reports one grant on every
// table. grants selects whether the provider lists planning.table-grants.
func tableGrantsFixture(c *qt.C, grants string) providerCLI {
	c.Helper()
	spec := fixtureReleaseSpec(c, "constraints", testutils.FixtureRelease{})
	spec.Environment = append(spec.Environment, "FIXTURE_CONSTRAINT_MODE=text", "FIXTURE_TABLE_GRANTS="+grants)
	f := newProviderCLI(c, spec)
	db, err := sql.Open("sqlite", f.databasePath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	_, err = db.ExecContext(c.Context(), heldScoreCheck)
	c.Assert(err, qt.IsNil)
	return f
}

// A rebuild written into a migration file gives the new table the grant the
// read found on the old one. The host sends the provider that grant with the
// held table, since the text runs later and nothing in it reads the server.
// A provider that does not list planning.table-grants receives no grant, so
// the same rebuild writes none back; the grant the provider writes comes from
// the request alone.
func TestProviderRebuildRegrantsInAMigrationFile_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name     string
		grants   string
		regrants int
	}{
		{"a provider that lists the feature", "listed", 1},
		{"a provider that does not list it", "unlisted", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := tableGrantsFixture(c, test.grants)
			f.declare(c, declaredScoreCheck("score >= 0 AND score < 10"))

			_, diagnostics, code, dir := f.generateMigration(c)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
			up := upMigration(c, dir)
			c.Assert(up, qt.Contains, `CONSTRAINT "items_score_check" CHECK (score >= 0 AND score < 10)`)
			c.Assert(strings.Count(up, `GRANT SELECT ON "items" TO "reader";`), qt.Equals, test.regrants, qt.Commentf("%s", up))
		})
	}
}
