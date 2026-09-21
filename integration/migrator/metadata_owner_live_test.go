//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// A metadata table Ptah did not create is refused before anything touches it.
//
// Ptah creates its metadata with CREATE TABLE IF NOT EXISTS and then writes to
// it, so a table already standing under that name is adopted whatever put it
// there. Where a lower-privileged role can create objects in the metadata
// schema -- PostgreSQL 14 and earlier grant CREATE on public to PUBLIC -- that
// role can pre-create the table, attach an invoker-rights trigger, and have
// Ptah's own write run the trigger body with the migration role's privileges.
//
// The trigger is the measurement rather than the rule: a rule, a default
// expression on a column Ptah does not write, and a row-level policy are other
// ways in, which is why the refusal reads ownership instead of enumerating
// what a table may carry (stokaro/ptah#3474).
func TestForeignMetadataTableIsRefusedLive(t *testing.T) {
	c := qt.New(t)
	fixture := newForeignMetadataFixture(t, "schema_migrations")

	err := fixture.migrator.Initialize(t.Context())

	c.Assert(err, qt.ErrorIs, migrator.ErrForeignMetadataTable)
	c.Assert(err, qt.ErrorMatches, `(?s).*owned by "ptah_squatter_.*" and this connection runs as .*`)
	// The trigger never ran, which is what the refusal is worth: a message
	// after the body executed would name a privilege that was already used.
	c.Assert(fixture.triggerRuns(c), qt.Equals, 0)
}

// The control: a table Ptah created itself is its own, so an ordinary run is
// untouched by the refusal. Without this the test above would also pass on a
// build that refused every metadata table.
func TestOwnMetadataTableIsAcceptedLive(t *testing.T) {
	c := qt.New(t)
	fixture := newForeignMetadataFixture(t, "")

	c.Assert(fixture.migrator.Initialize(t.Context()), qt.IsNil)
	c.Assert(fixture.migrator.MigrateUp(t.Context()), qt.IsNil)

	applied, err := fixture.migrator.GetAppliedMigrations(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(applied, qt.DeepEquals, []int64{1})
}

// A table an administrator created and handed to the application belongs to a
// role the application is a member of. Refusing that would report an
// arrangement somebody set up deliberately as an attack, so the check asks the
// server about membership rather than comparing two names.
func TestMetadataTableOwnedByAnInheritedRoleIsAcceptedLive(t *testing.T) {
	c := qt.New(t)
	fixture := newForeignMetadataFixture(t, "")
	admin := fixture.adminExec
	admin(c, `CREATE ROLE `+fixture.ownerRole)
	admin(c, `GRANT `+fixture.ownerRole+` TO `+fixture.migrationRole)
	admin(c, foreignMetadataRevisionTableDDL)
	admin(c, `ALTER TABLE public.schema_migrations OWNER TO `+fixture.ownerRole)
	admin(c, `GRANT ALL ON public.schema_migrations TO `+fixture.migrationRole)

	c.Assert(fixture.migrator.Initialize(t.Context()), qt.IsNil)
}

// The log is a record beside the work, so its refusal warns and the migration
// runs. Failing the run there would hand any role that can create a table in
// the metadata schema a way to stop every migration by creating that one; the
// revision table has no such choice, because Ptah cannot record what it did
// without it.
func TestForeignMigrationLogTableDoesNotStopTheMigrationLive(t *testing.T) {
	c := qt.New(t)
	fixture := newForeignMetadataFixture(t, "schema_migrations_log")

	c.Assert(fixture.migrator.MigrateUp(t.Context()), qt.IsNil)

	// The work landed and the squatter's trigger did not run. Reading the log
	// is refused on its own, which is
	// [TestForeignMigrationLogTableIsRefusedOnARead].
	c.Assert(fixture.triggerRuns(c), qt.Equals, 0)
	applied, err := fixture.migrator.GetAppliedMigrations(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(applied, qt.DeepEquals, []int64{1})
}

// foreignMetadataFixture is a scratch database with two login roles: one that
// only creates objects, and the one migrations run as.
type foreignMetadataFixture struct {
	migrator      *migrator.Migrator
	adminConn     *dbschema.DatabaseConnection
	ownerRole     string
	migrationRole string
	squatterRole  string
	migrationURL  string
	adminExec     func(c *qt.C, statement string)
}

// foreignMetadataMigrations is the one migration every case in this file
// applies; what varies is the metadata table standing in front of it.
func foreignMetadataMigrations() fstest.MapFS {
	return fstest.MapFS{
		"0000000001_widgets.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE widgets (id integer PRIMARY KEY);\n"),
		},
		"0000000001_widgets.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE widgets;\n")},
	}
}

// dryRunMigrator is the same migrator asked to simulate, which is the path
// that returns before the writes and still reads the metadata table.
func (f *foreignMetadataFixture) dryRunMigrator(c *qt.C) *migrator.Migrator {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), f.migrationURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	conn.SchemaWriter().SetDryRun(true)
	mig, err := migrator.NewFSMigrator(conn, foreignMetadataMigrations())
	c.Assert(err, qt.IsNil)
	return mig
}

// triggerRuns counts what the squatter's trigger recorded, which is zero on
// every run that refused the table before writing to it.
func (f *foreignMetadataFixture) triggerRuns(c *qt.C) int {
	c.Helper()
	var count int
	c.Assert(f.adminConn.QueryRowContext(
		context.Background(), "SELECT count(*) FROM public.ptah_squat_evidence").Scan(&count), qt.IsNil)
	return count
}

// foreignMetadataRevisionTableDDL is the revision table's own shape, copied so
// the squat is accepted by CREATE TABLE IF NOT EXISTS and by every later read.
// A wrong shape would be refused for being the wrong shape, which is not what
// these tests measure.
const foreignMetadataRevisionTableDDL = `CREATE TABLE public.schema_migrations (
  version bigint NOT NULL,
  description text NOT NULL,
  applied_at timestamp without time zone NOT NULL,
  state character varying(32) NOT NULL DEFAULT 'applied',
  applied integer NOT NULL DEFAULT 1,
  total integer NOT NULL DEFAULT 1,
  error text,
  error_stmt text,
  execution_time_ms bigint NOT NULL DEFAULT 0,
  checksum character varying(64) NOT NULL DEFAULT '',
  PRIMARY KEY (version)
)`

const foreignMetadataLogTableDDL = `CREATE TABLE public.schema_migrations_log (
  run_id character varying(64) NOT NULL,
  seq bigint NOT NULL,
  operation character varying(32) NOT NULL,
  version bigint NOT NULL,
  state character varying(32) NOT NULL,
  actor character varying(256),
  actor_source character varying(32) NOT NULL,
  checksum character varying(64),
  logged_at timestamp without time zone NOT NULL,
  error text,
  PRIMARY KEY (run_id, seq)
)`

// newForeignMetadataFixture provisions the scratch database and, when squat
// names a table, pre-creates that table as the squatter with a trigger that
// records the role it ran as.
func newForeignMetadataFixture(t *testing.T, squat string) *foreignMetadataFixture {
	t.Helper()
	c := qt.New(t)
	stamp := time.Now().UnixNano()
	fixture := &foreignMetadataFixture{
		ownerRole:     fmt.Sprintf("ptah_owner_%d", stamp),
		migrationRole: fmt.Sprintf("ptah_migrator_%d", stamp),
		squatterRole:  fmt.Sprintf("ptah_squatter_%d", stamp),
	}
	database := fmt.Sprintf("ptah_owner_check_%d", stamp)
	serverURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	provisionForeignMetadataDatabase(c, t, serverURL, database, fixture)

	fixture.adminConn = openForeignMetadataConnection(c, t, serverURL, database, "")
	fixture.adminExec = func(c *qt.C, statement string) {
		c.Helper()
		_, err := fixture.adminConn.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	fixture.adminExec(c, "CREATE TABLE public.ptah_squat_evidence (note text)")
	fixture.adminExec(c, "GRANT INSERT, SELECT ON public.ptah_squat_evidence TO PUBLIC")
	fixture.adminExec(c, foreignMetadataTriggerFunctionDDL)
	// PostgreSQL 15 and later revoke CREATE on public from PUBLIC, so the
	// grants are what make this test about the refusal rather than about the
	// server's default. The exposure is a schema both roles can create in,
	// however it came to be one.
	fixture.adminExec(c, "GRANT CREATE, USAGE ON SCHEMA public TO "+fixture.squatterRole)
	fixture.adminExec(c, "GRANT CREATE, USAGE ON SCHEMA public TO "+fixture.migrationRole)

	squatForeignMetadataTable(c, t, serverURL, database, fixture, squat)

	fixture.migrationURL = foreignMetadataURL(c, serverURL, database, fixture.migrationRole)
	conn := openForeignMetadataConnection(c, t, serverURL, database, fixture.migrationRole)
	mig, err := migrator.NewFSMigrator(conn, foreignMetadataMigrations())
	c.Assert(err, qt.IsNil)
	fixture.migrator = mig
	return fixture
}

// foreignMetadataTriggerFunctionDDL records the role the trigger body observes.
// SECURITY INVOKER is the default and is named to say what the measurement is.
const foreignMetadataTriggerFunctionDDL = `CREATE FUNCTION public.ptah_squat_probe() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER AS $$
BEGIN
  INSERT INTO public.ptah_squat_evidence (note) VALUES ('ran as ' || current_user);
  RETURN NEW;
END;
$$`

func provisionForeignMetadataDatabase(
	c *qt.C,
	t *testing.T,
	serverURL, database string,
	fixture *foreignMetadataFixture,
) {
	c.Helper()
	admin, err := dbschema.ConnectToDatabase(context.Background(), serverURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	for _, statement := range []string{
		`CREATE DATABASE "` + database + `"`,
		`CREATE ROLE ` + fixture.migrationRole + ` LOGIN PASSWORD 'ptah_password'`,
		`CREATE ROLE ` + fixture.squatterRole + ` LOGIN PASSWORD 'ptah_password'`,
		`GRANT ALL ON DATABASE "` + database + `" TO ` + fixture.migrationRole,
		`GRANT ALL ON DATABASE "` + database + `" TO ` + fixture.squatterRole,
	} {
		_, execErr := admin.ExecContext(context.Background(), statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	t.Cleanup(func() { dropForeignMetadataFixture(serverURL, database, fixture) })
}

// squatForeignMetadataTable creates one metadata table as the squatter, with
// the trigger attached. An empty name leaves the database clean, which is what
// the control runs against.
func squatForeignMetadataTable(
	c *qt.C,
	t *testing.T,
	serverURL, database string,
	fixture *foreignMetadataFixture,
	squat string,
) {
	c.Helper()
	ddl := map[string][]string{
		"": nil,
		"schema_migrations": {
			foreignMetadataRevisionTableDDL,
			`CREATE TRIGGER ptah_squat_trg BEFORE INSERT ON public.schema_migrations
			 FOR EACH ROW EXECUTE FUNCTION public.ptah_squat_probe()`,
			`GRANT ALL ON public.schema_migrations TO PUBLIC`,
		},
		"partitioned": {
			`CREATE TABLE public.schema_migrations (
			   version bigint NOT NULL,
			   description text NOT NULL,
			   applied_at timestamp without time zone NOT NULL
			 ) PARTITION BY RANGE (version)`,
			`GRANT ALL ON public.schema_migrations TO PUBLIC`,
		},
		"schema_migrations_log": {
			foreignMetadataLogTableDDL,
			`CREATE TRIGGER ptah_squat_trg BEFORE INSERT ON public.schema_migrations_log
			 FOR EACH ROW EXECUTE FUNCTION public.ptah_squat_probe()`,
			`GRANT ALL ON public.schema_migrations_log TO PUBLIC`,
		},
	}[squat]
	conn := openForeignMetadataConnection(c, t, serverURL, database, fixture.squatterRole)
	for _, statement := range ddl {
		_, err := conn.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

func openForeignMetadataConnection(
	c *qt.C,
	t *testing.T,
	serverURL, database, role string,
) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(
		context.Background(), foreignMetadataURL(c, serverURL, database, role))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// foreignMetadataURL addresses the scratch database as the named role, or as
// the account the registry handed over when the name is empty.
func foreignMetadataURL(c *qt.C, serverURL, database, role string) string {
	c.Helper()
	parsed, err := url.Parse(serverURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + database
	parsed.User = foreignMetadataUser(parsed.User, role)
	return parsed.String()
}

func foreignMetadataUser(existing *url.Userinfo, role string) *url.Userinfo {
	if role == "" {
		return existing
	}
	return url.UserPassword(role, "ptah_password")
}

// dropForeignMetadataFixture removes the database and the roles. The roles
// outlive the database, so dropping only the database would leave a login
// account behind on a shared server.
func dropForeignMetadataFixture(serverURL, database string, fixture *foreignMetadataFixture) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	admin, err := dbschema.ConnectToDatabase(ctx, serverURL)
	if err != nil {
		return
	}
	defer dbschema.CloseAndWarn(admin)
	for _, statement := range []string{
		`DROP DATABASE IF EXISTS "` + database + `" WITH (FORCE)`,
		`DROP ROLE IF EXISTS ` + fixture.migrationRole,
		`DROP ROLE IF EXISTS ` + fixture.squatterRole,
		`DROP ROLE IF EXISTS ` + fixture.ownerRole,
	} {
		_, _ = admin.ExecContext(ctx, statement)
	}
}

// A dry run reads the existing metadata table, so it is refused too. A foreign
// table can attach a policy or a default expression the server evaluates during
// a SELECT, and a refusal that covered only the writes would describe a
// protection the read path does not have.
func TestForeignMetadataTableIsRefusedOnADryRunLive(t *testing.T) {
	c := qt.New(t)
	fixture := newForeignMetadataFixture(t, "schema_migrations")

	err := fixture.dryRunMigrator(c).Initialize(t.Context())

	c.Assert(err, qt.ErrorIs, migrator.ErrForeignMetadataTable)
	c.Assert(fixture.triggerRuns(c), qt.Equals, 0)
}

// Reading the log is refused for the same reason, and terminally: a caller
// that asked for the log gets nothing either way, and should be told which
// table it declined to read rather than shown an empty list.
func TestForeignMigrationLogTableIsRefusedOnARead(t *testing.T) {
	c := qt.New(t)
	fixture := newForeignMetadataFixture(t, "schema_migrations_log")

	_, err := fixture.migrator.MigrationLog(t.Context(), 0)

	c.Assert(err, qt.ErrorIs, migrator.ErrForeignMetadataTable)
	c.Assert(fixture.triggerRuns(c), qt.Equals, 0)
}

// A partitioned table holds the name the same way an ordinary one does, and
// CREATE TABLE IF NOT EXISTS adopts it. A lookup narrowed to ordinary tables
// would report it absent and hand it the adoption this refusal exists to stop.
func TestForeignPartitionedMetadataTableIsRefusedLive(t *testing.T) {
	c := qt.New(t)
	fixture := newForeignMetadataFixture(t, "partitioned")

	err := fixture.migrator.Initialize(t.Context())

	c.Assert(err, qt.ErrorIs, migrator.ErrForeignMetadataTable)
}

// The override is resolved before Initialize's early returns, so a malformed
// value fails the run rather than lying dormant until an invocation happens to
// reach the branch that reads it.
//
// The second call is the measurement: it returns on the memoized result before
// any query, so a variable read only by the ownership lookup would never be
// parsed and a typo would select the default instead of failing closed.
func TestForeignMetadataOverrideIsValidatedEarlyLive(t *testing.T) {
	c := qt.New(t)
	fixture := newForeignMetadataFixture(t, "")
	c.Assert(fixture.migrator.Initialize(t.Context()), qt.IsNil)

	t.Setenv(migrator.AllowForeignMetadataTableEnvVar, "perhaps")
	err := fixture.migrator.Initialize(t.Context())

	c.Assert(err, qt.ErrorMatches, `(?s).*`+migrator.AllowForeignMetadataTableEnvVar+`.*`)
}

// The override is resolved on the log read path too, before the return that
// reports an absent table. A database with no log is the invocation a variable
// read further down would never reach.
func TestForeignMetadataOverrideIsValidatedOnTheLogReadLive(t *testing.T) {
	c := qt.New(t)
	fixture := newForeignMetadataFixture(t, "")
	t.Setenv(migrator.AllowForeignMetadataTableEnvVar, "perhaps")

	_, err := fixture.migrator.MigrationLog(t.Context(), 0)

	c.Assert(err, qt.ErrorMatches, `(?s).*`+migrator.AllowForeignMetadataTableEnvVar+`.*`)
}
