//go:build integration

package atlas_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestMigrateCheckpointProjectSchemasLivePostgres pins that `migrate
// checkpoint` reads `env { schemas }`. The history creates two schemas and the
// env names one of them, so the checkpoint creates that schema's table and not
// the other. A verb that ignores the list leaves the run on the dev URL's own
// scope, `public`, and the checkpoint creates neither table.
func TestMigrateCheckpointProjectSchemasLivePostgres(t *testing.T) {
	c := qt.New(t)
	dbURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	suffix := uniqueScopeSuffix()
	devURL := createDisposableDatabase(c, dbURL, "ptah_checkpoint_project_dev_"+suffix)
	appSchema := "ptah_checkpoint_project_app_" + suffix
	auditSchema := "ptah_checkpoint_project_audit_" + suffix
	root := c.TempDir()
	c.Assert(os.Mkdir(filepath.Join(root, "migrations"), 0o700), qt.IsNil)
	history := fmt.Sprintf(
		"CREATE SCHEMA %[1]s;\nCREATE SCHEMA %[2]s;\nCREATE TABLE %[1]s.users (id integer);\nCREATE TABLE %[2]s.logs (id integer);\n",
		appSchema, auditSchema,
	)
	c.Assert(os.WriteFile(filepath.Join(root, "migrations", "20240101000000_init.sql"), []byte(history), 0o600), qt.IsNil)
	project := fmt.Sprintf(
		"env \"local\" {\n  dev = %q\n  migration {\n    dir = \"file://migrations\"\n  }\n  schemas = [%q]\n}\n",
		devURL, appSchema,
	)
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
	c.Setenv("PTAH_SCHEMA", "")
	c.Setenv("PTAH_SCHEMAS", "")
	c.Chdir(root)
	runCheckpointCompat(c, "migrate", "hash", "--env", "local")

	runCheckpointCompat(c, "migrate", "checkpoint", "--env", "local", "scoped")

	written, err := filepath.Glob(filepath.Join(root, "migrations", "*_scoped.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 1)
	checkpoint, err := os.ReadFile(written[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(checkpoint), qt.Contains, fmt.Sprintf(`CREATE TABLE "%s"."users"`, appSchema))
	c.Assert(string(checkpoint), qt.Not(qt.Contains), auditSchema)
}

// TestSchemaTestProjectSchemasLivePostgres pins that `schema test` reads
// `env { schemas }`. The desired schema holds two schemas and the env names one
// of them, so the throwaway database receives that schema alone, which the
// case reads back from the catalog. A verb that ignores the list applies both
// schemas, the case finds the second one and the run fails.
func TestSchemaTestProjectSchemasLivePostgres(t *testing.T) {
	c := qt.New(t)
	dbURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	suffix := uniqueScopeSuffix()
	devURL := createDisposableDatabase(c, dbURL, "ptah_schema_test_project_dev_"+suffix)
	appSchema := "ptah_schema_test_project_app_" + suffix
	auditSchema := "ptah_schema_test_project_audit_" + suffix
	root := c.TempDir()
	desired := fmt.Sprintf(
		"CREATE SCHEMA %[1]s;\nCREATE SCHEMA %[2]s;\nCREATE TABLE %[1]s.users (id integer);\nCREATE TABLE %[2]s.logs (id integer);\n",
		appSchema, auditSchema,
	)
	c.Assert(os.WriteFile(filepath.Join(root, "schema.sql"), []byte(desired), 0o600), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(root, "tests"), 0o700), qt.IsNil)
	testCase := fmt.Sprintf(
		"cases:\n  - name: scoped\n    steps:\n"+
			"      - assert:\n          query: \"SELECT count(*) FROM information_schema.schemata WHERE schema_name = '%[1]s'\"\n          scalar: \"1\"\n"+
			"      - assert:\n          query: \"SELECT count(*) FROM information_schema.schemata WHERE schema_name = '%[2]s'\"\n          scalar: \"0\"\n",
		appSchema, auditSchema,
	)
	c.Assert(os.WriteFile(filepath.Join(root, "tests", "scope.yaml"), []byte(testCase), 0o600), qt.IsNil)
	project := fmt.Sprintf(
		"env \"local\" {\n  dev = %q\n  schema {\n    src = \"file://schema.sql\"\n  }\n  schemas = [%q]\n}\n",
		devURL, appSchema,
	)
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
	c.Setenv("PTAH_SCHEMA", "")
	c.Chdir(root)

	runCheckpointCompat(c, "schema", "test", "--env", "local", "tests")
}

// TestSchemaTestProjectSchemasUnqualifiedTableLivePostgres pins that a
// selection naming the throwaway database's default schema keeps a table the
// desired schema leaves unqualified. PostgreSQL creates `users` in `public`,
// and the case reads it back from there. Without the default the selection
// keeps no table and the run is refused before the case runs.
func TestSchemaTestProjectSchemasUnqualifiedTableLivePostgres(t *testing.T) {
	c := qt.New(t)
	dbURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	devURL := createDisposableDatabase(c, dbURL, "ptah_schema_test_unqualified_dev_"+uniqueScopeSuffix())
	root := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "schema.sql"), []byte("CREATE TABLE users (id integer);\n"), 0o600), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(root, "tests"), 0o700), qt.IsNil)
	testCase := "cases:\n  - name: unqualified\n    steps:\n" +
		"      - assert:\n          query: \"SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users'\"\n          scalar: \"1\"\n"
	c.Assert(os.WriteFile(filepath.Join(root, "tests", "scope.yaml"), []byte(testCase), 0o600), qt.IsNil)
	project := fmt.Sprintf(
		"env \"local\" {\n  dev = %q\n  schema {\n    src = \"file://schema.sql\"\n  }\n  schemas = [\"public\"]\n}\n",
		devURL,
	)
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
	c.Setenv("PTAH_SCHEMA", "")
	c.Chdir(root)

	runCheckpointCompat(c, "schema", "test", "--env", "local", "tests")
}
