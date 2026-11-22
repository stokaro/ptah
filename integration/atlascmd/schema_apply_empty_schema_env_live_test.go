//go:build integration

package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/envbinding"
)

// emptySchemaEnvFixture is a target database holding a table in `public` and
// one in `audit`, a dev database, and a desired schema that declares only the
// `public` table. A run over every schema plans a DROP for `audit.logs`.
type emptySchemaEnvFixture struct {
	targetURL  string
	devURL     string
	schemaPath string
}

// newEmptySchemaEnvFixture creates the databases and the desired schema file.
func newEmptySchemaEnvFixture(c *qt.C) emptySchemaEnvFixture {
	c.Helper()
	dbURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	suffix := uniqueScopeSuffix()
	fixture := emptySchemaEnvFixture{
		targetURL:  createDisposableDatabase(c, dbURL, "ptah_empty_schema_env_"+suffix),
		devURL:     createDisposableDatabase(c, dbURL, "ptah_empty_schema_env_dev_"+suffix),
		schemaPath: filepath.Join(c.TempDir(), "schema.sql"),
	}
	seedDatabase(c, fixture.targetURL,
		`CREATE SCHEMA audit`,
		`CREATE TABLE public.users (id integer)`,
		`CREATE TABLE audit.logs (id integer)`,
	)
	c.Assert(os.WriteFile(fixture.schemaPath, []byte("CREATE TABLE users (id integer);\n"), 0o600), qt.IsNil)
	return fixture
}

// unsetCompatSchemaVariables removes PTAH_SCHEMA and PTAH_SCHEMAS for the rest
// of the test. Set but empty, either one is refused.
func unsetCompatSchemaVariables(c *qt.C) {
	c.Helper()
	for _, name := range []string{"PTAH_SCHEMA", "PTAH_SCHEMAS"} {
		c.Setenv(name, "")
		c.Assert(os.Unsetenv(name), qt.IsNil)
	}
}

// TestSchemaApplyEmptySchemaVariableLivePostgres_FailurePath pins that
// `schema apply` with PTAH_SCHEMA set but empty is refused and changes
// nothing. Read as unset, the variable would select every schema, and the run
// would drop `audit.logs`, which the desired schema does not declare;
// TestSchemaApplySchemaVariableLivePostgres_HappyPath measures that drop.
func TestSchemaApplyEmptySchemaVariableLivePostgres_FailurePath(t *testing.T) {
	c := qt.New(t)
	fixture := newEmptySchemaEnvFixture(c)
	unsetCompatSchemaVariables(c)
	c.Setenv("PTAH_SCHEMA", "")

	out, err := runCompatSchemaApply(fixture.targetURL, fixture.devURL, fixture.schemaPath)

	c.Assert(err, qt.ErrorIs, envbinding.ErrEmptyValue)
	c.Assert(err, qt.ErrorMatches,
		`PTAH_SCHEMA is set but empty: give it a value, or unset it and leave --schema out to select every schema`)
	c.Assert(out, qt.Not(qt.Contains), "DROP")
	c.Assert(postgresSchemaTableExists(t, fixture.targetURL, "audit", "logs"), qt.IsTrue)
	c.Assert(postgresSchemaTableExists(t, fixture.targetURL, "public", "users"), qt.IsTrue)
}

// TestSchemaApplySchemaVariableLivePostgres_HappyPath is the control for the
// refusal. Unset, the variable selects every schema and the run drops
// `audit.logs`, which is what an empty variable read as unset would do. Set to
// `public`, it keeps the run to that schema and `audit.logs` stays.
func TestSchemaApplySchemaVariableLivePostgres_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		env           map[string]string
		wantAuditLogs bool
	}{
		{name: "unset", wantAuditLogs: false},
		{name: "public", env: map[string]string{"PTAH_SCHEMA": "public"}, wantAuditLogs: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newEmptySchemaEnvFixture(c)
			unsetCompatSchemaVariables(c)
			for name, value := range test.env {
				c.Setenv(name, value)
			}

			out, err := runCompatSchemaApply(fixture.targetURL, fixture.devURL, fixture.schemaPath)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(postgresSchemaTableExists(t, fixture.targetURL, "audit", "logs"), qt.Equals, test.wantAuditLogs)
			c.Assert(postgresSchemaTableExists(t, fixture.targetURL, "public", "users"), qt.IsTrue)
		})
	}
}
