package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// writeAzureDBTokenProject writes an atlas.hcl that declares a
// `data "azure_db_token"` block and never references it, beside a SQLite
// database the env inspects. An unreferenced block mints nothing, so the
// default policy loads the project offline. It takes a *testing.T because
// atlastest.SeedSQLiteDBAt does.
func writeAzureDBTokenProject(t *testing.T) string {
	t.Helper()
	c := qt.New(t)
	dir := t.TempDir()
	database := filepath.Join(dir, "app.db")
	atlastest.SeedSQLiteDBAt(t, database, "CREATE TABLE accounts (id INTEGER PRIMARY KEY)")
	config := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(config, []byte(`data "azure_db_token" "db" {}

env "dev" {
  url = "sqlite://`+filepath.ToSlash(database)+`"
}
`), 0o600), qt.IsNil)
	return config
}

// TestSchemaInspectAzureDBToken_StrictPolicyRefusesTheBlock drives the policy
// wiring rather than the parser option: the strict profile has to reach the
// project loader, where the community binary refuses a project that declares
// the block whether or not anything references it.
func TestSchemaInspectAzureDBToken_StrictPolicyRefusesTheBlock(t *testing.T) {
	c := qt.New(t)
	config := writeAzureDBTokenProject(t)

	stdout, _, err := runCompatWithPolicy(atlascompatpolicy.StrictCE(),
		"schema", "inspect", "--config", "file://"+config, "--env", "dev")

	c.Assert(err, qt.ErrorMatches, `.*missing data source handler for "azure_db_token".*`)
	c.Assert(stdout, qt.Equals, "")
}

// TestSchemaInspectAzureDBToken_DefaultPolicyLoadsTheProject is the control:
// the same project loads under the default profile, so the refusal above comes
// from the policy and not from the fixture.
func TestSchemaInspectAzureDBToken_DefaultPolicyLoadsTheProject(t *testing.T) {
	c := qt.New(t)
	config := writeAzureDBTokenProject(t)

	stdout, _, err := runCompatWithPolicy(atlascompatpolicy.Full(),
		"schema", "inspect", "--config", "file://"+config, "--env", "dev")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "accounts")
}
