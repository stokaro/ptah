//go:build integration

package atlas_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/dbtarget"
)

// TestMigrateCheckpointSchemasEnvironmentLivePostgres is the control for the
// refusal TestAtlasForwardedSchemaEnvironment_FailurePath pins. The adapter
// reads PTAH_SCHEMAS in place of the native command, so a value it refuses
// never reaches the target; a value it accepts still has to scope the
// checkpoint. Read and dropped, the variable would leave the run on the dev
// URL's own scope, `public`, and the checkpoint would create neither table.
func TestMigrateCheckpointSchemasEnvironmentLivePostgres(t *testing.T) {
	c := qt.New(t)
	dbURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	suffix := uniqueScopeSuffix()
	devURL := createDisposableDatabase(c, dbURL, "ptah_checkpoint_env_dev_"+suffix)
	appSchema := "ptah_checkpoint_app_" + suffix
	auditSchema := "ptah_checkpoint_audit_" + suffix
	dir := c.TempDir()
	history := fmt.Sprintf(
		"CREATE SCHEMA %[1]s;\nCREATE SCHEMA %[2]s;\nCREATE TABLE %[1]s.users (id integer);\nCREATE TABLE %[2]s.logs (id integer);\n",
		appSchema, auditSchema,
	)
	c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_init.sql"), []byte(history), 0o600), qt.IsNil)
	runCheckpointCompat(c, "migrate", "hash", "--dir", "file://"+filepath.ToSlash(dir))
	unsetCompatSchemaVariables(c)
	c.Setenv("PTAH_SCHEMAS", appSchema)

	runCheckpointCompat(c, "migrate", "checkpoint", "--dev-url", devURL, "--dir", "file://"+filepath.ToSlash(dir), "scoped")

	written, err := filepath.Glob(filepath.Join(dir, "*_scoped.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 1)
	checkpoint, err := os.ReadFile(written[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(checkpoint), qt.Contains, fmt.Sprintf(`CREATE TABLE "%s"."users"`, appSchema))
	c.Assert(string(checkpoint), qt.Not(qt.Contains), auditSchema)
}

// runCheckpointCompat runs the compatibility tree and fails the test when the
// command fails, printing what it wrote.
func runCheckpointCompat(c *qt.C, args ...string) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
}
