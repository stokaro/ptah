//go:build integration

package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// The directories the secret tests write into: a secret at the top of one,
// and one in a directory below it.
const (
	secretsSchema       = "ptah_ydb_secrets"     // #nosec G101 -- a directory name, not a credential
	secretsNestedSchema = "ptah_ydb_secrets/ext" // #nosec G101 -- a directory name, not a credential
)

var secretsSchemas = []string{secretsSchema, secretsNestedSchema}

// The variables the secret tests' values come from, and the values, each
// carrying a marker no output may show.
const (
	secretPasswordEnv   = "PTAH_SECRET_LIVE_PG_PASSWORD"    // #nosec G101 -- a variable name, not a credential
	secretPasswordValue = "pg-SENTINEL-v1 'quoted' \\ back" // #nosec G101 -- a made-up value every output is searched for
	secretKeyEnv        = "PTAH_SECRET_LIVE_S3_KEY"         // #nosec G101 -- a variable name, not a credential
	secretKeyValue      = "s3-SENTINEL-v1"                  // #nosec G101 -- a made-up value every output is searched for
)

// secretsDeclaration declares the two secrets.
func secretsDeclaration() *schemamodel.Database {
	return &schemamodel.Database{Secrets: []schemamodel.Secret{
		{Name: "pg_password", Schema: secretsSchema, ValueEnv: secretPasswordEnv},
		{Name: "s3.key", Schema: secretsNestedSchema, ValueEnv: secretKeyEnv},
	}}
}

// dropSecrets drops every secret in the directories a test owns.
func dropSecrets(c *qt.C, conn *dbschema.DatabaseConnection, schemas []string) {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(context.Background(), conn, schemas)
	c.Assert(err, qt.IsNil)
	for _, secret := range live.Secrets {
		statement := "DROP SECRET " + sqlident.Quote("ydb", secret.Schema+"/"+secret.Name)
		c.Assert(conn.Writer().ExecuteSQL(context.Background(), statement), qt.IsNil)
	}
}

// TestYDBSecrets_RoundTrip_NothingLeftToPlan is the secrets family's round
// trip on the line that has them: two secrets rendered with references to
// their variables, applied through the connection that defines each value,
// read back by their paths, compared with nothing left to plan, and the same
// declaration applied again with nothing planned after it.
func TestYDBSecrets_RoundTrip_NothingLeftToPlan(t *testing.T) {
	t.Setenv(secretPasswordEnv, secretPasswordValue)
	t.Setenv(secretKeyEnv, secretKeyValue)
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "26.2"))
	dropSecrets(c, conn, secretsSchemas)
	c.Cleanup(func() { dropSecrets(c, conn, secretsSchemas) })

	first := planAgainst(c, conn, secretsDeclaration(), secretsSchemas)
	apply(c, conn, first)

	c.Assert(first, qt.DeepEquals, []string{
		"CREATE SECRET `ptah_ydb_secrets/pg_password` WITH (value = $PTAH_SECRET_LIVE_PG_PASSWORD)",
		"CREATE SECRET `ptah_ydb_secrets/ext/s3.key` WITH (value = $PTAH_SECRET_LIVE_S3_KEY)",
	})
	c.Assert(readScoped(c, conn, secretsSchemas).Secrets, qt.DeepEquals, []catalog.Secret{
		{Name: "pg_password", Schema: secretsSchema},
		{Name: "s3.key", Schema: secretsNestedSchema},
	})
	c.Assert(planAgainst(c, conn, secretsDeclaration(), secretsSchemas), qt.HasLen, 0)
	apply(c, conn, planAgainst(c, conn, secretsDeclaration(), secretsSchemas))
	c.Assert(planAgainst(c, conn, secretsDeclaration(), secretsSchemas), qt.HasLen, 0)
}

// TestYDBSecrets_RotatedOnlyWhenAsked changes the value a variable holds:
// nothing is planned, since the server never returns the value, until the
// rotation is asked for, which plans one ALTER SECRET the server takes. A
// secret the declaration leaves out is then dropped, and nothing is left to
// plan or to read.
func TestYDBSecrets_RotatedOnlyWhenAsked(t *testing.T) {
	t.Setenv(secretPasswordEnv, secretPasswordValue)
	t.Setenv(secretKeyEnv, secretKeyValue)
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "26.2"))
	dropSecrets(c, conn, secretsSchemas)
	c.Cleanup(func() { dropSecrets(c, conn, secretsSchemas) })
	apply(c, conn, planAgainst(c, conn, secretsDeclaration(), secretsSchemas))
	t.Setenv(secretPasswordEnv, "pg-SENTINEL-v2")

	unasked := planAgainst(c, conn, secretsDeclaration(), secretsSchemas)
	rotation := planRotating(c, conn, secretsDeclaration(), "ptah_ydb_secrets/pg_password")
	apply(c, conn, rotation)

	c.Assert(unasked, qt.HasLen, 0)
	c.Assert(rotation, qt.DeepEquals, []string{
		"ALTER SECRET `ptah_ydb_secrets/pg_password` WITH (value = $PTAH_SECRET_LIVE_PG_PASSWORD)",
	})

	kept := secretsDeclaration()
	kept.Secrets = kept.Secrets[:1]
	dropped := planAgainst(c, conn, kept, secretsSchemas)
	apply(c, conn, dropped)

	c.Assert(dropped, qt.DeepEquals, []string{"DROP SECRET `ptah_ydb_secrets/ext/s3.key`"})
	c.Assert(planAgainst(c, conn, kept, secretsSchemas), qt.HasLen, 0)
	c.Assert(readScoped(c, conn, secretsSchemas).Secrets, qt.DeepEquals, []catalog.Secret{
		{Name: "pg_password", Schema: secretsSchema},
	})
}

// TestYDBSecrets_FailurePath_RefusesWithoutSendingTheValue runs the statements
// a plan would against the server, where the connection refuses them: one
// whose variable is not set never reaches the server, and one the server
// refuses -- a secret over the path a table holds -- reports the statement as
// written, with the variable's name, and not the value the connection
// defined.
func TestYDBSecrets_FailurePath_RefusesWithoutSendingTheValue(t *testing.T) {
	t.Setenv(secretKeyEnv, secretKeyValue)
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "26.2"))
	dropTables(c, conn, secretsSchemas)
	dropSecrets(c, conn, secretsSchemas)
	c.Cleanup(func() {
		dropTables(c, conn, secretsSchemas)
		dropSecrets(c, conn, secretsSchemas)
	})
	c.Assert(conn.Writer().ExecuteSQL(c.Context(),
		"CREATE TABLE `ptah_ydb_secrets/taken` (id Int64 NOT NULL, PRIMARY KEY (id))"), qt.IsNil)

	unset := conn.Writer().ExecuteSQL(c.Context(),
		"CREATE SECRET `ptah_ydb_secrets/unset` WITH (value = $PTAH_SECRET_LIVE_UNSET)")
	refused := conn.Writer().ExecuteSQL(c.Context(),
		"CREATE SECRET `ptah_ydb_secrets/taken` WITH (value = $PTAH_SECRET_LIVE_S3_KEY)")

	c.Assert(unset, qt.ErrorMatches,
		"ydb: SQL execution failed: ydb: a secret's value comes from environment variable "+
			"PTAH_SECRET_LIVE_UNSET, which is not set\nSQL: CREATE SECRET .*")
	c.Assert(refused, qt.ErrorMatches, "(?s)ydb: SQL execution failed: .*unexpected path type.*\n"+
		"SQL: CREATE SECRET `ptah_ydb_secrets/taken` WITH \\(value = \\$PTAH_SECRET_LIVE_S3_KEY\\)")
	c.Assert(refused, qt.Not(qt.ErrorMatches), "(?s).*SENTINEL.*")
	c.Assert(readScoped(c, conn, secretsSchemas).Secrets, qt.HasLen, 0)
}

// TestYDBSecrets_FailurePath_RefusedOnALineWithout plans a declared secret
// against 25.1, which has only the deprecated secret object: the comparison
// refuses it by the secrets key before anything is planned.
func TestYDBSecrets_FailurePath_RefusedOnALineWithout(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "25.1"))
	info := conn.Info()

	diff, err := schemadiff.CompareWithDatabaseInfo(secretsDeclaration(), readScoped(c, conn, secretsSchemas), info, nil)

	c.Assert(err, qt.ErrorMatches,
		"secret ptah_ydb_secrets.pg_password, which requires target capability secrets, unavailable on this ydb target")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(diff, qt.IsNil)
	c.Assert(info.Capabilities.Has(capability.Secrets), qt.IsFalse)
}

// planRotating plans the declaration against the directories the secret tests
// own, asking the plan to rotate the secret at path.
func planRotating(c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database, path string) []string {
	c.Helper()
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabaseInfo(declared, readScoped(c, conn, secretsSchemas), info, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.RotateSecrets([]string{path}), qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		diff, info.Dialect, planner.Options{Capabilities: info.Capabilities},
	)
	c.Assert(err, qt.IsNil)
	return statements
}

// TestYDBSecrets_DropAllTablesDropsThem holds the cleanup to what the reader
// describes: a secret is described, so DropAllTables drops it and removes
// the column table beside it and the directories they leave empty.
func TestYDBSecrets_DropAllTablesDropsThem(t *testing.T) {
	c := qt.New(t)
	line := lineNamed(c, "26.2")
	conn := openYDB(c, line)
	c.Cleanup(func() {
		c.Assert(conn.Writer().ExecuteSQL(context.Background(), "DROP TABLE IF EXISTS `ptah_ydb_dropall_secrets/keep/olap`"),
			qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE SECRET `ptah_ydb_dropall_secrets/gone/pw` WITH (value = 'dropped')",
		"CREATE SECRET `ptah_ydb_dropall_secrets/keep/pw` WITH (value = 'dropped')",
		"CREATE TABLE `ptah_ydb_dropall_secrets/keep/olap` (`id` Int64 NOT NULL, PRIMARY KEY (`id`)) " +
			"PARTITION BY HASH(`id`) WITH (STORE = COLUMN)",
	} {
		c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil, qt.Commentf("execute: %s", statement))
	}

	c.Assert(conn.SchemaWriter().DropAllTables(c.Context()), qt.IsNil)

	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(live.Secrets, qt.HasLen, 0)
	c.Assert(live.Tables, qt.HasLen, 0)
	c.Assert(directoryNames(c, c.Context(), line), qt.Not(qt.Contains), "ptah_ydb_dropall_secrets")
}

// TestYDBSecrets_DropDirectoryDropsThem tears down a directory that holds a
// secret, as the capability probe's namespace does after its secret row.
func TestYDBSecrets_DropDirectoryDropsThem(t *testing.T) {
	c := qt.New(t)
	line := lineNamed(c, "26.2")
	conn := openYDB(c, line)
	dropper, ok := conn.SchemaWriter().(interface {
		DropDirectory(ctx context.Context, dir string) error
	})
	c.Assert(ok, qt.IsTrue, qt.Commentf("the YDB schema writer %T removes no directory", conn.SchemaWriter()))
	c.Cleanup(func() { c.Check(dropper.DropDirectory(context.Background(), "ptah_ydb_dropdir_secrets"), qt.IsNil) })
	for _, statement := range []string{
		"CREATE SECRET `ptah_ydb_dropdir_secrets/probe/sk_secret` WITH (value = 'probe')",
		"CREATE SECRET `ptah_ydb_dropdir_secrets/probe/deeper/pw` WITH (value = 'probe')",
		"CREATE TABLE `ptah_ydb_dropdir_secrets/keep/t` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
	} {
		c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil, qt.Commentf("execute: %s", statement))
	}

	c.Assert(dropper.DropDirectory(c.Context(), "ptah_ydb_dropdir_secrets/probe"), qt.IsNil)

	c.Assert(directoryNames(c, c.Context(), line, "ptah_ydb_dropdir_secrets"), qt.DeepEquals, []string{"keep"})
}
