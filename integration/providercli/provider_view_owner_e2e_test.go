//go:build integration

package providercli_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testutils"
)

// viewOwnerSchema declares the table items and the view recent with body.
func viewOwnerSchema(body string) string {
	return "tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true}\nviews:\n  recent:\n    body: \"" + body + "\"\n"
}

// viewOwnerFixture is the CLI over the common-objects program reporting app
// as the owner of every view it reads, with the table items and the view
// recent over it applied through the provider.
func viewOwnerFixture(c *qt.C) providerCLI {
	c.Helper()
	spec := fixtureReleaseSpec(c, "common-objects", testutils.FixtureRelease{})
	spec.Environment = append(spec.Environment, "FIXTURE_VIEW_OWNER=app")
	f := newProviderCLI(c, spec)
	c.Assert(os.WriteFile(f.schemaPath, []byte(viewOwnerSchema("SELECT id FROM items")), 0o600), qt.IsNil)
	f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve")
	return f
}

// TestProviderReplacesAViewWhoseOwnerTheReadReports_HappyPath applies a
// changed view through a provider whose read reports the view's owner. The
// fixture's engine manages no owner, so the comparison carries none for a
// plan to give back, and the provider's plan replaces the view rather than
// refuse a field its wire has no form for. The view is read back from the
// database, and a second plan finds nothing to change.
func TestProviderReplacesAViewWhoseOwnerTheReadReports_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := viewOwnerFixture(c)
	c.Assert(os.WriteFile(f.schemaPath, []byte(viewOwnerSchema("SELECT id FROM items WHERE id > 0")), 0o600), qt.IsNil)

	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(heldViewDefinitions(c, f.databasePath), qt.DeepEquals, []string{`recent: CREATE VIEW "recent" AS SELECT id FROM items WHERE id > 0`})
	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
}

// TestProviderReplacesAViewWhoseOwnerTheReadReports_FailurePath serves the
// built-in name postgres, whose descriptor manages the owner of a view, so
// the comparison carries the owner the read reports for a rebuild to give
// back. The wire has no form for it, and the provider would hand the view it
// creates again to the role that runs the plan, so the apply refuses before
// it writes and the view keeps its body. The refusal is also the control of
// the happy path: the program's read reports the owner.
func TestProviderReplacesAViewWhoseOwnerTheReadReports_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := builtInNameViewsCLI(c, "FIXTURE_VIEW_OWNER=app")
	f.success(c, "apply", "--provider-engine", "postgres", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve")
	c.Assert(os.WriteFile(f.schemaPath, []byte(viewOwnerSchema("SELECT id FROM items WHERE id > 0")), 0o600), qt.IsNil)

	_, diagnostics, code := f.run(c, c.Context(), "apply", "--provider-engine", "postgres", "--schema-file", f.schemaPath, "--tx-mode", "none",
		"--auto-approve")

	c.Assert(code, qt.Equals, 2)
	c.Assert(diagnostics, qt.Contains, `the provider serves engine "postgres", whose views a plan gives back their owners`)
	c.Assert(diagnostics, qt.Contains, "the experimental provider contract does not support SchemaDiff.CurrentOwners")
	c.Assert(heldViewDefinitions(c, f.databasePath), qt.DeepEquals, []string{`recent: CREATE VIEW "recent" AS SELECT id FROM items`})
}
