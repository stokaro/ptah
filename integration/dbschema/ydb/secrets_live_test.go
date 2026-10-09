//go:build integration

package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine/builtin"
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

// secretsDeclaration declares the given secrets, of the two the tests use,
// from a source that describes the secret namespace: a secret it leaves out
// is one it asks to drop.
func secretsDeclaration(names ...string) *schemamodel.Database {
	all := map[string]schemaext.Object{
		"pg_password": ydbsecret.DesiredObject(secretsSchema, "pg_password", "", secretPasswordEnv),
		"s3.key":      ydbsecret.DesiredObject(secretsNestedSchema, "s3.key", "", secretKeyEnv),
	}
	if len(names) == 0 {
		names = []string{"pg_password", "s3.key"}
	}
	var objects []schemaext.Object
	for _, name := range names {
		objects = append(objects, all[name])
	}
	return &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
}

// liveSecrets names, by path, every secret a read observed.
func liveSecrets(c *qt.C, db *catalog.Database) []string {
	c.Helper()
	var paths []string
	for _, ref := range db.FeatureObjects.Select(func(ref objectidentity.ID) bool { return ref.Kind == objectidentity.Kind(ydbsecret.Kind) }).Refs() {
		paths = append(paths, ydbsecret.Display(ref.Schema.Source, ref.Name.Source))
	}
	return paths
}

// dropSecrets drops every secret in the directories a test owns.
func dropSecrets(c *qt.C, conn *dbschema.DatabaseConnection, schemas []string) {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(context.Background(), conn, schemas)
	c.Assert(err, qt.IsNil)
	for _, path := range liveSecrets(c, live) {
		statement := "DROP SECRET " + sqlident.Quote("ydb", path)
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
	c.Assert(liveSecrets(c, readScoped(c, conn, secretsSchemas)), qt.ContentEquals, []string{
		"ptah_ydb_secrets/pg_password", "ptah_ydb_secrets/ext/s3.key",
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

	kept := secretsDeclaration("pg_password")
	dropped := planAgainst(c, conn, kept, secretsSchemas)
	apply(c, conn, dropped)

	c.Assert(dropped, qt.DeepEquals, []string{"DROP SECRET `ptah_ydb_secrets/ext/s3.key`"})
	c.Assert(planAgainst(c, conn, kept, secretsSchemas), qt.HasLen, 0)
	c.Assert(liveSecrets(c, readScoped(c, conn, secretsSchemas)), qt.DeepEquals, []string{"ptah_ydb_secrets/pg_password"})
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
	c.Assert(liveSecrets(c, readScoped(c, conn, secretsSchemas)), qt.HasLen, 0)
}

// TestYDBSecrets_FailurePath_RefusedOnALineWithout plans a declared secret
// against 25.1, which has only the deprecated secret object: the comparison
// refuses it by the secrets key before anything is planned.
func TestYDBSecrets_FailurePath_RefusedOnALineWithout(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "25.1"))
	info := conn.Info()

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), secretsDeclaration(), readScoped(c, conn, secretsSchemas), info, nil, must.Must(builtin.New()))

	c.Assert(err, qt.ErrorMatches,
		"secret ptah_ydb_secrets/pg_password, which requires target capability secrets, unavailable on this ydb target")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(diff, qt.IsNil)
	c.Assert(info.Capabilities.Has(capability.Secrets), qt.IsFalse)
}

// planRotating plans the declaration against the directories the secret tests
// own, asking the plan to rotate the secret at path.
func planRotating(c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database, path string) []string {
	c.Helper()
	info := conn.Info()
	opts := &config.CompareOptions{FeatureRequests: must.Must(ydbsecret.RotationRequests([]string{path}))}
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), declared, readScoped(c, conn, secretsSchemas), info, opts, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
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
	c.Assert(liveSecrets(c, live), qt.HasLen, 0)
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

// The root secrets the limit test makes: one whose name holds a dot, which
// the source leaves unmanaged, and one the source describes by leaving it out.
const (
	secretsLimitKept    = "ptah_ydb_secrets_limit.pw"    // #nosec G101 -- a secret's path, not a credential
	secretsLimitDropped = "ptah_ydb_secrets_limit_other" // #nosec G101 -- a secret's path, not a credential
)

// dropRootSecrets drops the root secrets the limit test makes, and no other.
func dropRootSecrets(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	for _, path := range liveSecrets(c, readScoped(c, conn, []string{""})) {
		if path != secretsLimitKept && path != secretsLimitDropped {
			continue
		}
		c.Assert(conn.Writer().ExecuteSQL(context.Background(), "DROP SECRET "+sqlident.Quote("ydb", path)), qt.IsNil)
	}
}

// TestYDBSecrets_LimitNamesADottedRootSecret reads a Go source that leaves the
// root secret ptah_ydb_secrets_limit.pw unmanaged and plans it against a
// database that holds it: the limit names that path, not pw in a directory
// ptah_ydb_secrets_limit, so the secret is left alone. The source still
// describes every other secret, so another root secret is dropped.
func TestYDBSecrets_LimitNamesADottedRootSecret(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "26.2"))
	dropRootSecrets(c, conn)
	c.Cleanup(func() { dropRootSecrets(c, conn) })
	for _, path := range []string{secretsLimitKept, secretsLimitDropped} {
		c.Assert(conn.Writer().ExecuteSQL(c.Context(), "CREATE SECRET "+sqlident.Quote("ydb", path)+" WITH (value = 'probe')"), qt.IsNil)
	}
	source, err := goschema.ParseSource("limits.go",
		"package entities\n//ptah:schema:notdescribed kind=\"secret\" name=\""+secretsLimitKept+"\"\ntype Unmanaged struct{}\n")
	c.Assert(err, qt.IsNil)
	declared := &schemamodel.Database{FeatureCoverage: source.FeatureCoverage.SelectKinds([]schemaext.Kind{ydbsecret.Kind})}

	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), declared, readScoped(c, conn, []string{""}), conn.Info(), nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.FeatureChanges, qt.DeepEquals, []schemaext.ChangeRecord{
		{Subject: ydbsecret.Ref("", secretsLimitDropped), Value: &ydbdiff.Secret{Before: &ydbsecret.Observed{}}},
	})
}

// TestYDBSecrets_CreatedBeneathADroppedTable plans a secret beneath the path of
// a table the plan drops. YDB needs the directory above a secret to hold no
// other object, so CREATE SECRET runs after DROP TABLE, and the server takes
// both; nothing is left to plan after.
func TestYDBSecrets_CreatedBeneathADroppedTable(t *testing.T) {
	t.Setenv(secretPasswordEnv, secretPasswordValue)
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "26.2"))
	schemas := []string{secretsSchema, secretsSchema + "/holder"}
	dropSecrets(c, conn, schemas)
	dropTables(c, conn, schemas)
	c.Cleanup(func() {
		dropSecrets(c, conn, schemas)
		dropTables(c, conn, schemas)
	})
	c.Assert(conn.Writer().ExecuteSQL(c.Context(),
		"CREATE TABLE `ptah_ydb_secrets/holder` (id Int64 NOT NULL, PRIMARY KEY (id))"), qt.IsNil)
	declared := &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(ydbsecret.DesiredObject(secretsSchema+"/holder", "pw", "", secretPasswordEnv))),
		FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}

	plan := planAgainst(c, conn, declared, schemas)
	apply(c, conn, plan)

	c.Assert(plan, qt.DeepEquals, []string{
		"DROP TABLE `ptah_ydb_secrets/holder`",
		"CREATE SECRET `ptah_ydb_secrets/holder/pw` WITH (value = $PTAH_SECRET_LIVE_PG_PASSWORD)",
	})
	c.Assert(liveSecrets(c, readScoped(c, conn, schemas)), qt.DeepEquals, []string{"ptah_ydb_secrets/holder/pw"})
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)
}

// planFailure plans the declaration against the directories a test owns and
// returns why the plan was refused.
func planFailure(c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database, schemas []string) error {
	c.Helper()
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), declared, readScoped(c, conn, schemas), info, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()), diff, info.Dialect, planner.Options{Capabilities: info.Capabilities})
	c.Assert(statements, qt.IsNil)
	return err
}

// absoluteSecretSource declares the secret ptah_ydb_external/abs_pw, unless
// secret is false, and a PostgreSQL source at location whose password the
// secret at path holds.
func absoluteSecretSource(secret bool, location, path string) *schemamodel.Database {
	declared := &schemamodel.Database{
		FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
		ExternalDataSources: []schemamodel.ExternalDataSource{{Name: "warehouse", Schema: externalSchema, SourceType: "PostgreSQL",
			Location: location, AuthMethod: "BASIC",
			Options: map[string]string{"DATABASE_NAME": "app", "LOGIN": "reader", "PASSWORD_SECRET_PATH": path}}},
	}
	if secret {
		declared.FeatureObjects = must.Must(schemaext.NewObjects(ydbsecret.DesiredObject(externalSchema, "abs_pw", "", externalSecretEnv)))
	}
	return declared
}

// TestYDBSecrets_ReadThroughAnAbsolutePath declares a data source that names
// its secret by an absolute path, as YDB stores it. The path is read against
// the database the plan runs in: the secret is created before the source, the
// two compare equal after, and a plan that drops the secret while it creates
// the source again is refused, as is a path outside the database.
func TestYDBSecrets_ReadThroughAnAbsolutePath(t *testing.T) {
	t.Setenv(externalSecretEnv, "probe")
	c := qt.New(t)
	line := lineNamed(c, "26.2")
	setClusterFlags(c, line, externalSourcesOn)
	conn := openYDB(c, line)
	c.Cleanup(func() {
		externalTeardown(c, conn, nil)
		dropSecrets(c, conn, externalSchemas)
	})
	root := readScoped(c, conn, externalSchemas).DatabasePath
	declared := absoluteSecretSource(true, "pg.invalid:5432", root+"/"+externalSchema+"/abs_pw")

	first := planAgainst(c, conn, declared, externalSchemas)
	apply(c, conn, first)

	c.Assert(first, qt.HasLen, 2)
	c.Assert(first[0], qt.Equals, "CREATE SECRET `ptah_ydb_external/abs_pw` WITH (value = $PTAH_SECRET_LIVE_EXTERNAL_PG)")
	c.Assert(planAgainst(c, conn, declared, externalSchemas), qt.HasLen, 0)
	c.Assert(planFailure(c, conn, absoluteSecretSource(false, "pg2.invalid:5432", root+"/"+externalSchema+"/abs_pw"), externalSchemas),
		qt.ErrorMatches, ".*secret ptah_ydb_external/abs_pw is dropped while a statement of this plan reads it by its path.*")
	c.Assert(planFailure(c, conn, absoluteSecretSource(true, "pg2.invalid:5432", "/elsewhere/abs_pw"), externalSchemas),
		qt.ErrorMatches, `.*secret path "/elsewhere/abs_pw" is outside the database `+root+`.*`)
}

// TestYDBSecrets_NotRefusedWithoutTheKeyWhenNothingRuns compares the secret a
// database holds against a target without the secrets key, as a 25.3 server
// with EnableSchemaSecrets on lists one: a declaration that keeps it and a
// source with no claim on it plan no secret statement, so neither is refused.
// On 25.1 a source that describes every secret and declares none plans
// nothing either.
func TestYDBSecrets_NotRefusedWithoutTheKeyWhenNothingRuns(t *testing.T) {
	t.Setenv(secretPasswordEnv, secretPasswordValue)
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "26.2"))
	dropSecrets(c, conn, secretsSchemas)
	c.Cleanup(func() { dropSecrets(c, conn, secretsSchemas) })
	apply(c, conn, planAgainst(c, conn, secretsDeclaration("pg_password"), secretsSchemas))
	info := conn.Info()
	info.Capabilities = capability.YDB253()

	for _, declared := range []*schemamodel.Database{secretsDeclaration("pg_password"), {}} {
		diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), declared, readScoped(c, conn, secretsSchemas), info, nil, must.Must(builtin.New()))
		c.Assert(err, qt.IsNil)
		c.Assert(diff.FeatureChanges, qt.HasLen, 0)
	}

	old := openYDB(c, lineNamed(c, "25.1"))
	described := &schemamodel.Database{FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))}
	c.Assert(planAgainst(c, old, described, secretsSchemas), qt.HasLen, 0)
}

// TestYDBSecrets_FailurePath_RefusedWithoutTheKeyForAStatement compares the
// same database against a target without the secrets key for a declaration
// that leaves the secret out: dropping it is a statement the target cannot
// run, so the comparison refuses it by name.
func TestYDBSecrets_FailurePath_RefusedWithoutTheKeyForAStatement(t *testing.T) {
	t.Setenv(secretPasswordEnv, secretPasswordValue)
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "26.2"))
	dropSecrets(c, conn, secretsSchemas)
	c.Cleanup(func() { dropSecrets(c, conn, secretsSchemas) })
	apply(c, conn, planAgainst(c, conn, secretsDeclaration("pg_password"), secretsSchemas))
	info := conn.Info()
	info.Capabilities = capability.YDB253()
	described := &schemamodel.Database{FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))}

	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), described, readScoped(c, conn, secretsSchemas), info, nil, must.Must(builtin.New()))

	c.Assert(err, qt.ErrorMatches, "secret ptah_ydb_secrets/pg_password, which requires target capability secrets, unavailable on this ydb target")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(diff, qt.IsNil)
}
