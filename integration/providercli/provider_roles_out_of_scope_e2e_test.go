//go:build integration

package providercli_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testutils"
)

// A provider names a role the server holds outside its read's scope and
// describes none of its attributes, as a YDB dev realm names the database's
// users. A desired role of that name exists and nothing is planned for it:
// comparing the attributes the wire does not carry would plan an alteration
// of every attribute the desired role sets.
func TestProviderRoleOutOfScopeIsNotAltered_HappyPath(t *testing.T) {
	c := qt.New(t)
	spec := fixtureReleaseSpec(c, "common-objects", testutils.FixtureRelease{})
	spec.Environment = append(spec.Environment, "FIXTURE_ROLES_OUT_OF_SCOPE=outsider")
	f := newProviderCLI(c, spec)
	c.Assert(os.WriteFile(f.schemaPath, []byte("roles:\n  outsider: {login: true}\n"), 0o600), qt.IsNil)

	planned := f.success(c, "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")

	c.Assert(planned, qt.Contains, `"outcome": "no-changes"`)
}
