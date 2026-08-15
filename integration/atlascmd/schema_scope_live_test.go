//go:build integration

package atlas_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/dbschema"
)

// livePostgresURLForScope gates the live schema-scope tests on the same
// environment variables as the other PostgreSQL live tests.
func livePostgresURLForScope(t *testing.T) string {
	t.Helper()
	dbURL := os.Getenv("POSTGRES_TEST_DSN")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("POSTGRES_TEST_DSN or TEST_DATABASE_URL not set")
	}
	if !strings.HasPrefix(dbURL, "postgres://") && !strings.HasPrefix(dbURL, "postgresql://") {
		t.Skip("PostgreSQL URL required for schema scope live tests")
	}
	return dbURL
}

// createScopeSchemas provisions two uniquely named schemas with one table
// each and registers a cascading cleanup.
func createScopeSchemas(t *testing.T, dbURL string) (appSchema, auditSchema string) {
	t.Helper()
	c := qt.New(t)
	suffix := fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	appSchema = "ptah_scope_app_" + suffix
	auditSchema = "ptah_scope_audit_" + suffix
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+appSchema+" CASCADE")
		_, _ = conn.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+auditSchema+" CASCADE")
		dbschema.CloseAndWarn(conn)
	})
	for _, statement := range []string{
		"CREATE SCHEMA " + appSchema,
		"CREATE SCHEMA " + auditSchema,
		"CREATE TABLE " + appSchema + ".users (id SERIAL PRIMARY KEY)",
		"CREATE TABLE " + auditSchema + ".logs (id SERIAL PRIMARY KEY)",
	} {
		_, err := conn.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil)
	}
	return appSchema, auditSchema
}

func livePostgresColumnExists(t *testing.T, dbURL, schema, table, column string) bool {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3",
		schema, table, column).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

func livePostgresTableExists(t *testing.T, dbURL, schema, table string) bool {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
		schema, table).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

func TestSchemaApplySchemaScopeLivePostgres(t *testing.T) {
	c := qt.New(t)
	dbURL := livePostgresURLForScope(t)
	devURL := createDisposableDatabase(c, dbURL, "ptah_scope_apply_dev_"+uniqueScopeSuffix())
	appSchema, auditSchema := createScopeSchemas(t, dbURL)
	schemaPath := filepath.Join(t.TempDir(), "schema.sql")
	desired := "CREATE TABLE " + appSchema + ".users (\n  id SERIAL PRIMARY KEY,\n  email VARCHAR(255)\n);\n"
	c.Assert(os.WriteFile(schemaPath, []byte(desired), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", dbURL,
		"--dev-url", devURL,
		"--to", "file://" + schemaPath,
		"--schema", appSchema,
		"--auto-approve",
	})

	err := cmd.Execute()

	// The scoped apply adds the missing column inside the selected schema and
	// leaves the other schema (and everything else in the database) alone.
	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Schema apply completed successfully.")
	c.Assert(livePostgresColumnExists(t, dbURL, appSchema, "users", "email"), qt.IsTrue)
	c.Assert(livePostgresTableExists(t, dbURL, auditSchema, "logs"), qt.IsTrue)
}

func TestSchemaApplySchemaScopeCrossSchemaDependencyLivePostgres(t *testing.T) {
	c := qt.New(t)
	dbURL := livePostgresURLForScope(t)
	devURL := createDisposableDatabase(c, dbURL, "ptah_scope_dependency_dev_"+uniqueScopeSuffix())
	appSchema, auditSchema := createScopeSchemas(t, dbURL)
	schemaPath := filepath.Join(t.TempDir(), "schema.sql")
	// The desired state declares both tables; the schema scope selects only
	// the app schema, dropping the audit-side dependency target.
	desired := "CREATE TABLE " + auditSchema + ".logs (\n  id SERIAL PRIMARY KEY\n);\n" +
		"CREATE TABLE " + appSchema + ".users (\n  id SERIAL PRIMARY KEY,\n" +
		"  log_id INTEGER REFERENCES " + auditSchema + ".logs(id)\n);\n"
	c.Assert(os.WriteFile(schemaPath, []byte(desired), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", dbURL,
		"--dev-url", devURL,
		"--to", "file://" + schemaPath,
		"--schema", appSchema,
		"--dry-run",
	})

	err := cmd.Execute()

	// A selected table depending on a table outside the schema scope refuses
	// the plan with an explicit diagnostic instead of emitting incomplete SQL.
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "via a foreign key")
	c.Assert(err.Error(), qt.Contains, auditSchema+".logs")
}

func TestSchemaDiffSchemaScopeLivePostgres(t *testing.T) {
	c := qt.New(t)
	dbURL := livePostgresURLForScope(t)
	appSchema, auditSchema := createScopeSchemas(t, dbURL)
	schemaPath := filepath.Join(t.TempDir(), "schema.sql")
	desired := "CREATE TABLE " + appSchema + ".users (\n  id SERIAL PRIMARY KEY,\n  email VARCHAR(255)\n);\n"
	c.Assert(os.WriteFile(schemaPath, []byte(desired), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", dbURL,
		"--to", "file://" + schemaPath,
		"--schema", appSchema,
	})

	err := cmd.Execute()

	// The database-backed side is introspected live and both sides project to
	// the selected schema, so the diff only concerns the scoped table.
	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "email")
	c.Assert(out.String(), qt.Not(qt.Contains), auditSchema)
}

func TestSchemaDiffSchemaScopeKeepsDatabaseWideExtensionLivePostgres(t *testing.T) {
	c := qt.New(t)
	dbURL := livePostgresURLForScope(t)
	appSchema, _ := createScopeSchemas(t, dbURL)
	schemaPath := filepath.Join(t.TempDir(), "schema.hcl")
	desired := `
schema "extensions" {}
extension "citext" {
  schema = schema.extensions
}

schema "` + appSchema + `" {}
table "users" {
  schema = schema.` + appSchema + `
  column "id" {
    type = serial
  }
  column "email" {
    type = sql("extensions.citext")
  }
  primary_key {
    columns = [column.id]
  }
}
`
	c.Assert(os.WriteFile(schemaPath, []byte(desired), 0o600), qt.IsNil)

	out := runCompatSchemaDiff(c,
		"--from", dbURL,
		"--to", "file://"+schemaPath,
		"--schema", appSchema,
		"--include", appSchema+".users",
	)

	// Extension installation placement is not object ownership. Selecting only
	// the app table retains citext as database-wide support, synthesizes its
	// schema precondition, and plans it before the selected table starts using
	// extensions.citext.
	schemaSQL := `CREATE SCHEMA IF NOT EXISTS "extensions"`
	extensionSQL := `CREATE EXTENSION "citext" WITH SCHEMA "extensions"`
	c.Assert(out, qt.Contains, schemaSQL)
	c.Assert(out, qt.Contains, extensionSQL)
	c.Assert(out, qt.Contains, `"email" extensions.citext`)
	c.Assert(strings.Index(out, schemaSQL) < strings.Index(out, extensionSQL), qt.IsTrue)
}

func TestSchemaApplyNonExtensionScopeDoesNotDropUnmentionedExtensionLivePostgres(t *testing.T) {
	c := qt.New(t)
	adminURL := livePostgresURLForScope(t)
	suffix := uniqueScopeSuffix()
	targetURL := createDisposableDatabase(c, adminURL, "ptah_scope_apply_support_target_"+suffix)
	devURL := createDisposableDatabase(c, adminURL, "ptah_scope_apply_support_dev_"+suffix)
	seedDatabase(c, targetURL,
		`CREATE SCHEMA app`,
		`CREATE TABLE app.users (id bigint PRIMARY KEY)`,
		`CREATE EXTENSION pgcrypto`,
	)
	schemaPath := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(schemaPath, []byte(`schema "elsewhere" {}`), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", targetURL,
		"--dev-url", devURL,
		"--to", "file://" + schemaPath,
		"--schema", "app",
		"--include", "app.users",
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, `DROP TABLE IF EXISTS "app"."users" CASCADE`)
	c.Assert(out.String(), qt.Not(qt.Contains), "DROP EXTENSION")
}

// createScopeInspectSchema provisions one uniquely named schema holding the
// PostgreSQL-only object kinds the include projection has to reason about: an
// enum used by a kept column, a SERIAL-owned sequence, an independent table,
// and a dependent table joined by a foreign key.
func createScopeInspectSchema(t *testing.T, dbURL string) string {
	t.Helper()
	c := qt.New(t)
	name := fmt.Sprintf("ptah_inspect_inc_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+name+" CASCADE")
		dbschema.CloseAndWarn(conn)
	})
	for _, statement := range []string{
		"CREATE SCHEMA " + name,
		"CREATE TYPE " + name + ".user_state AS ENUM ('on', 'off')",
		"CREATE TABLE " + name + ".users (id SERIAL PRIMARY KEY, state " + name + ".user_state)",
		"CREATE TABLE " + name + ".posts (id SERIAL PRIMARY KEY, author_id INTEGER REFERENCES " + name + ".users(id))",
		"CREATE TABLE " + name + ".archive (id SERIAL PRIMARY KEY)",
	} {
		_, err := conn.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil)
	}
	return name
}

func TestSchemaInspectIncludeLivePostgres(t *testing.T) {
	dbURL := livePostgresURLForScope(t)
	schemaName := createScopeInspectSchema(t, dbURL)

	t.Run("qualified selection keeps the table and the type it uses", func(t *testing.T) {
		c := qt.New(t)
		stdout, stderr, err := runCompatInspect(
			"--url", dbURL, "--schema", schemaName, "--include", schemaName+".users")

		// The enum the kept column uses rides along; the unrelated table and
		// the dependent table do not.
		c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
		c.Assert(stdout, qt.Contains, `table "users"`)
		c.Assert(stdout, qt.Contains, "user_state")
		c.Assert(stdout, qt.Not(qt.Contains), `table "archive"`)
		c.Assert(stdout, qt.Not(qt.Contains), `table "posts"`)
	})

	t.Run("bare name matches inside the schema universe", func(t *testing.T) {
		c := qt.New(t)
		stdout, stderr, err := runCompatInspect(
			"--url", dbURL, "--schema", schemaName, "--include", "users")

		c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
		c.Assert(stdout, qt.Contains, `table "users"`)
		c.Assert(stdout, qt.Not(qt.Contains), `table "archive"`)
	})

	t.Run("selection dropping a foreign key target is refused", func(t *testing.T) {
		c := qt.New(t)
		stdout, _, err := runCompatInspect(
			"--url", dbURL, "--schema", schemaName, "--include", "posts")

		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Contains, "via a foreign key")
		c.Assert(err.Error(), qt.Contains, schemaName+".users")
		// The refusal replaces the render: no schema output was produced.
		c.Assert(stdout, qt.Equals, "")
	})

	t.Run("matching everything keeps every table in the schema universe", func(t *testing.T) {
		c := qt.New(t)
		stdout, stderr, err := runCompatInspect(
			"--url", dbURL, "--schema", schemaName, "--include", "*")

		c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
		c.Assert(stdout, qt.Contains, `table "users"`)
		c.Assert(stdout, qt.Contains, `table "posts"`)
		c.Assert(stdout, qt.Contains, `table "archive"`)
		c.Assert(stdout, qt.Contains, "user_state")
	})
}
