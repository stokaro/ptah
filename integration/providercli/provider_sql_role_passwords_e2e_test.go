//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
)

// rolePasswordSecret is the password the SQL documents below give a user. No
// output may show it.
const rolePasswordSecret = "s3cret-fixture-pw"

// sqlRolesSpec builds the fixture command whose engine reads its SQL schema
// documents whole, users and their passwords among them, and takes a declared
// password as an execution input, from the SDK version the host pins.
func sqlRolesSpec(c *qt.C) providerhost.Spec {
	c.Helper()
	binary := testutils.BuildFixtureProvider(c, "sql-roles")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	return providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"}
}

// writeRoleSchema writes a SQL schema of the fixture engine that creates a
// table and a user with password, and returns its path.
func writeRoleSchema(c *qt.C, f providerCLI, user, password string) string {
	c.Helper()
	dir := filepath.Dir(f.schemaPath)
	writeSQLSchema(c, dir, map[string]string{"schema.sql": "CREATE TABLE items (id INTEGER NOT NULL);\nCREATE USER " + user + " PASSWORD '" + password + "';\n"})
	return filepath.Join(dir, "schema.sql")
}

// TestNativeCLIRendersTheRolePasswordASQLReadingReturns renders a SQL schema
// whose user declares a password through a provider that reads it: the
// statement names the execution input that holds the password, the role's
// own for a literal and the one a reference names, and never the literal.
func TestNativeCLIRendersTheRolePasswordASQLReadingReturns(t *testing.T) {
	for _, test := range []struct {
		name, user, password, want string
	}{
		{"a literal password", "app", rolePasswordSecret, "CREATE USER app PASSWORD $PTAH_SECRET_PASSWORD_APP;"},
		{"a reference to an execution input", "ops", "$PTAH_SECRET_OPS_PW", "CREATE USER ops PASSWORD $PTAH_SECRET_OPS_PW;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newProviderCLI(c, sqlRolesSpec(c))
			schema := writeRoleSchema(c, f, test.user, test.password)

			out, diagnostics, code := executeProviderCLI(c.Context(), []string{"schema", "render", "--provider-config", f.manifestPath, "--dialect", "fixture",
				"--schema-file", schema})

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
			c.Assert(out, qt.Contains, test.want)
			c.Assert(out+diagnostics, qt.Not(qt.Contains), rolePasswordSecret)
		})
	}
}

// TestNativeCLIPlansTheRolePasswordASQLReadingReturns plans the same schema
// against a database: the previewed plan creates the user with the input's
// name, and neither the preview nor the saved plan holds the password.
func TestNativeCLIPlansTheRolePasswordASQLReadingReturns(t *testing.T) {
	c := qt.New(t)
	f := newProviderCLI(c, sqlRolesSpec(c))
	schema := writeRoleSchema(c, f, "app", rolePasswordSecret)

	planned := f.success(c, "plan", "--schema-file", schema, "--dry-run")
	saved := f.success(c, "plan", "--schema-file", schema, "--output", f.planPath, "--json")
	file, err := os.ReadFile(f.planPath)

	c.Assert(err, qt.IsNil)
	c.Assert(planned, qt.Contains, "CREATE USER app PASSWORD $PTAH_SECRET_PASSWORD_APP;")
	c.Assert(planned+saved+string(file), qt.Not(qt.Contains), rolePasswordSecret)
}

// TestNativeCLIRefusedApplyHidesTheRolePassword_FailurePath applies the schema
// to the fixture's SQLite, which refuses the statement that creates the user:
// the refusal shows the statement as planned, with the input's name, and
// nothing the run prints holds the password.
func TestNativeCLIRefusedApplyHidesTheRolePassword_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newProviderCLI(c, sqlRolesSpec(c))
	schema := writeRoleSchema(c, f, "app", rolePasswordSecret)

	out, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", schema, "--tx-mode", "none", "--auto-approve")

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(out+diagnostics, qt.Contains, "$PTAH_SECRET_PASSWORD_APP")
	c.Assert(out+diagnostics, qt.Not(qt.Contains), rolePasswordSecret)
}
