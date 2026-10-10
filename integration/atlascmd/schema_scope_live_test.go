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

	"ptah.run/dbschema"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

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
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
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
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
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
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
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
		"--dev-url", createDisposableDatabase(c, dbURL, "ptah_scope_diff_dev_"+uniqueScopeSuffix()),
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
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
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
		"--dev-url", createDisposableDatabase(c, dbURL, "ptah_scope_ext_dev_"+uniqueScopeSuffix()),
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

// TestSchemaApplyNonExtensionScopeDoesNotDropUnmentionedExtensionLivePostgres
// selects one table of a target that also holds an extension: the plan drops
// the table and keeps the extension.
//
// The rehearsal rebuilds the target on a dev database of a server the run does
// not own. The extension carries the comment its control file sets, which
// CREATE EXTENSION writes by itself, so the rebuild writes no COMMENT ON
// EXTENSION, which the baseline guard would refuse there.
func TestSchemaApplyNonExtensionScopeDoesNotDropUnmentionedExtensionLivePostgres(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
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

// extensionCommentApply applies a scope that leaves app.users out to a target
// whose pgcrypto extension carries a comment of its own, rehearsing it on a
// scratch dev database of the same server, and returns what the command
// printed and its error.
func extensionCommentApply(c *qt.C, devURL string) (string, error) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	suffix := uniqueScopeSuffix()
	targetURL := createDisposableDatabase(c, adminURL, "ptah_scope_apply_comment_target_"+suffix)
	if devURL == "" {
		devURL = createDisposableDatabase(c, adminURL, "ptah_scope_apply_comment_dev_"+suffix)
	}
	seedDatabase(c, targetURL,
		`CREATE SCHEMA app`,
		`CREATE TABLE app.users (id bigint PRIMARY KEY)`,
		`CREATE EXTENSION pgcrypto`,
		`COMMENT ON EXTENSION pgcrypto IS 'hashing for app.users'`,
	)
	schemaPath := filepath.Join(c.TempDir(), "schema.hcl")
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
	return out.String(), err
}

// extensionCommentRefusal is how the rehearsal refuses the target's own
// comment on pgcrypto on a dev server the run does not provision.
const extensionCommentRefusal = `(?s).*baseline statement \d+ \(COMMENT ON EXTENSION "pgcrypto" IS 'hashing for app.users'\) cannot be rehearsed: ` +
	`postgres rehearsal baseline refuses COMMENT ON global metadata because its effects cannot be confined to the dev database realm; ` +
	`use a docker:// or docker\+<driver>:// dev URL, since a server declared disposable keeps this after the run; ` +
	`the plan was not applied to the target database`

// TestSchemaApplyRefusesAnExtensionCommentInTheBaselineLivePostgres holds the
// rehearsal's rebuild of the target to the dev database's realm. The target's
// extension carries a comment of its own, which the rebuild writes after
// creating the extension. The cleanup does not restore such a comment, so on a
// server the run does not provision the rehearsal refuses before any
// statement of the rebuild runs, names the statement, and names the docker
// URL as the way to a server the run discards.
func TestSchemaApplyRefusesAnExtensionCommentInTheBaselineLivePostgres(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(devdocker.DisposableServerEnvVar)(c)

	_, err := extensionCommentApply(c, "")

	c.Assert(err, qt.ErrorMatches, extensionCommentRefusal)
}

// TestSchemaApplyRefusesAnExtensionCommentOnADeclaredDevServerLivePostgres
// pins that declaring the server disposable does not lift the refusal above:
// such a server outlives the run, and its reset keeps the extension and the
// comment with it for the next run to read.
func TestSchemaApplyRefusesAnExtensionCommentOnADeclaredDevServerLivePostgres(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)

	_, err := extensionCommentApply(c, "")

	c.Assert(err, qt.ErrorMatches, extensionCommentRefusal)
}

// TestSchemaApplyWritesAnExtensionCommentOnAProvisionedDevServerLivePostgres
// is the control for the refusals above: on a dev server the run provisions
// from a docker URL, and removes with everything on it, the rebuild writes the
// comment.
func TestSchemaApplyWritesAnExtensionCommentOnAProvisionedDevServerLivePostgres(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(devdocker.DisposableServerEnvVar)(c)

	out, err := extensionCommentApply(c, "docker://postgres/18/dev")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `DROP TABLE IF EXISTS "app"."users" CASCADE`)
	c.Assert(out, qt.Not(qt.Contains), "DROP EXTENSION")
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
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
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
