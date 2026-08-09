package migrator_test

// Revision-completion failures, one case per DDL transaction capability class
// (issue #999).
//
// Ptah writes a migration's body and the revision row that records the
// migration as applied. What a failure of the second write leaves behind is
// decided by the target's DDL transaction contract, and internal/ddltx is the
// enumeration of those contracts. Every class named there is driven here:
//
//	ddltx.Transactional  -> SQLite (in process) and live PostgreSQL
//	ddltx.ImplicitCommit -> live MySQL and live MariaDB
//	ddltx.NoTransaction  -> live ClickHouse
//
// TestRevisionCompletionClasses_EveryClassIsCovered fails if a class exists
// with no case in this file, so the matrix cannot quietly shrink to the
// dialects that happened to be easy.
//
// The fault is always the same fault -- the write that sets state='applied'
// fails, and nothing else does -- but it has four spellings because the
// targets do. Three take a BEFORE UPDATE trigger that rejects the applied
// state. ClickHouse has no triggers at all and its constraints only fire on
// INSERT, so it takes the one property of the revision table that separates
// the two writes: completion assigns applied_at and failure does not, so
// putting applied_at in the sorting key makes exactly the completion write
// fail with CANNOT_UPDATE_COLUMN while the dirty-state write still lands.
// Dropping the revision table would also fail the completion write, but it
// destroys the dirty-state record this test exists to read.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/ddltx"
	"go.5x5.cz/ptah/migration/migrator"
)

// revisionCompletionNames carries the per-run object names. Every live case
// runs against a shared development database, so the names are unique per run
// and dropped on both sides of the test.
type revisionCompletionNames struct {
	revisions string
	body      string
	fault     string
}

func newRevisionCompletionNames(prefix string) revisionCompletionNames {
	suffix := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	return revisionCompletionNames{
		revisions: "rev999_" + suffix,
		body:      "body999_" + suffix,
		fault:     "fault999_" + suffix,
	}
}

// revisionCompletionTarget is the reusable fault-injection seam issue #999
// asks for. A target supplies the four dialect-specific pieces and the shared
// driver below supplies the scenario, so adding a dialect cannot change what
// is asserted.
type revisionCompletionTarget struct {
	name  string
	class ddltx.Class
	// connect returns a live connection or skips the test.
	connect func(t *testing.T) *dbschema.DatabaseConnection
	// faultConn returns the connection the fault is installed and removed
	// over, plus its cleanup. Most targets reuse the migrator's connection;
	// the MySQL family needs an administrative one, because a server with
	// binary logging on refuses CREATE TRIGGER to a user without SUPER and the
	// integration matrix runs migrations as an ordinary user.
	faultConn func(t *testing.T, conn *dbschema.DatabaseConnection) (*dbschema.DatabaseConnection, func())
	// createBody renders the migration body and its rollback.
	createBody func(names revisionCompletionNames) (up, down string)
	// installFault makes the write that records a migration applied fail. It
	// runs before Initialize, because on ClickHouse the fault is a property of
	// the revision table itself.
	installFault func(c *qt.C, conn *dbschema.DatabaseConnection, names revisionCompletionNames)
	// removeFault undoes installFault, standing in for the operator repairing
	// whatever broke the revision write.
	removeFault func(c *qt.C, conn *dbschema.DatabaseConnection, names revisionCompletionNames)
	// bodyPresent reads the catalog. Rendered SQL is not evidence; this asks
	// the server whether the table exists.
	bodyPresent func(c *qt.C, conn *dbschema.DatabaseConnection, names revisionCompletionNames) bool
	// dropObjects removes everything the case created.
	dropObjects func(conn *dbschema.DatabaseConnection, names revisionCompletionNames)
}

func TestRevisionCompletionFailure_SQLiteTransactional(t *testing.T) {
	runRevisionCompletionScenario(t, sqliteRevisionCompletionTarget(), retryAfterFixingTheRevisionWrite())
}

func TestRevisionCompletionFailure_PostgresTransactionalLive(t *testing.T) {
	runRevisionCompletionScenario(t, postgresRevisionCompletionTarget(), retryAfterFixingTheRevisionWrite())
}

func TestRevisionCompletionFailure_MySQLImplicitCommitLive(t *testing.T) {
	runRevisionCompletionScenario(t, mySQLRevisionCompletionTarget(), retryAfterFixingTheRevisionWrite())
}

func TestRevisionCompletionFailure_MariaDBImplicitCommitLive(t *testing.T) {
	runRevisionCompletionScenario(t, mariaDBRevisionCompletionTarget(), retryAfterFixingTheRevisionWrite())
}

func TestRevisionCompletionFailure_ClickHouseNoTransactionLive(t *testing.T) {
	runRevisionCompletionScenario(t, clickHouseRevisionCompletionTarget(), retryAfterFixingTheRevisionWrite())
}

// The repair cases below take the other way out. Repair signs the migration off
// as applied without running anything, so it belongs to the classes whose body
// survived the failure and to no others -- which is what
// TestRevisionCompletionRepair_CoversExactlyTheSurvivingBodyClasses pins.

func TestRevisionCompletionRepair_MySQLImplicitCommitLive(t *testing.T) {
	runRevisionCompletionScenario(t, mySQLRevisionCompletionTarget(), markAppliedAfterTheBodyCommitted())
}

func TestRevisionCompletionRepair_MariaDBImplicitCommitLive(t *testing.T) {
	runRevisionCompletionScenario(t, mariaDBRevisionCompletionTarget(), markAppliedAfterTheBodyCommitted())
}

func TestRevisionCompletionRepair_ClickHouseNoTransactionLive(t *testing.T) {
	runRevisionCompletionScenario(t, clickHouseRevisionCompletionTarget(), markAppliedAfterTheBodyCommitted())
}

// The two cases below separate the surviving-body classes, which the
// single-statement bodies above cannot. They exist because internal/ddltx
// answers two different questions -- did any of the body survive, and did all
// of it -- and the MySQL family answers them differently.
//
// A MySQL-family body of DDL followed by DML keeps both statements: the server
// commits before the DDL, which ends the migrator's transaction, and the DML
// that follows therefore runs in autocommit. A body of DML alone never reaches
// an implicit commit and rolls back whole. Only the second shape can tell a
// correct implementation from one that reads "the body is durable on this
// class" as "the whole body is applied", so both are pinned.

func TestRevisionCompletionFailure_MySQLKeepsDDLAndTrailingDMLLive(t *testing.T) {
	runRevisionCompletionAfterDDL(t, mySQLRevisionCompletionTarget())
}

func TestRevisionCompletionFailure_MariaDBKeepsDDLAndTrailingDMLLive(t *testing.T) {
	runRevisionCompletionAfterDDL(t, mariaDBRevisionCompletionTarget())
}

func TestRevisionCompletionFailure_MySQLRollsBackADMLOnlyBodyLive(t *testing.T) {
	runRevisionCompletionDMLOnly(t, mySQLRevisionCompletionTarget())
}

func TestRevisionCompletionFailure_MariaDBRollsBackADMLOnlyBodyLive(t *testing.T) {
	runRevisionCompletionDMLOnly(t, mariaDBRevisionCompletionTarget())
}

func runRevisionCompletionAfterDDL(t *testing.T, target revisionCompletionTarget) {
	c := qt.New(t)
	ctx := context.Background()
	fixture := newRevisionCompletionFixture(t, c, target, "ddl")

	up := fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY) ENGINE=InnoDB; INSERT INTO %s (id) VALUES (1)",
		fixture.names.body,
		fixture.names.body,
	)
	down := fmt.Sprintf("DROP TABLE %s", fixture.names.body)
	mig := fixture.begin(c, up, down)

	err := mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "failed to record migration 1")

	// The implicit commit before the CREATE ends the transaction, so the INSERT
	// after it autocommits and both statements are durable.
	c.Assert(target.bodyPresent(c, fixture.conn, fixture.names), qt.IsTrue)
	c.Assert(revisionCompletionRowCount(c, fixture.conn, fixture.names.body), qt.Equals, int64(1))

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 2)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 2)

	fixture.recoverWithRetry(c, up, down)
	c.Assert(revisionCompletionRowCount(c, fixture.conn, fixture.names.body), qt.Equals, int64(1))
}

func runRevisionCompletionDMLOnly(t *testing.T, target revisionCompletionTarget) {
	c := qt.New(t)
	ctx := context.Background()
	fixture := newRevisionCompletionFixture(t, c, target, "dml")

	// The table is created outside the migration, so the body contains no DDL
	// and never triggers an implicit commit.
	execRevisionCompletionSQL(c, fixture.faultConn, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY) ENGINE=InnoDB",
		fixture.names.body,
	))

	up := fmt.Sprintf(
		"INSERT INTO %s (id) VALUES (1); INSERT INTO %s (id) VALUES (2)",
		fixture.names.body,
		fixture.names.body,
	)
	down := fmt.Sprintf("DELETE FROM %s", fixture.names.body)
	mig := fixture.begin(c, up, down)

	err := mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "failed to record migration 1")

	// Nothing in this body is DDL, so the rollback took all of it. A revision
	// claiming otherwise would send the retry past rows that are not there.
	c.Assert(revisionCompletionRowCount(c, fixture.conn, fixture.names.body), qt.Equals, int64(0))

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 2)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 0)

	fixture.recoverWithRetry(c, up, down)
	c.Assert(revisionCompletionRowCount(c, fixture.conn, fixture.names.body), qt.Equals, int64(2))
}

// revisionCompletionFixture holds the connections and names shared by the two
// MySQL-family shape cases above.
type revisionCompletionFixture struct {
	target    revisionCompletionTarget
	conn      *dbschema.DatabaseConnection
	faultConn *dbschema.DatabaseConnection
	names     revisionCompletionNames
}

func newRevisionCompletionFixture(
	t *testing.T,
	c *qt.C,
	target revisionCompletionTarget,
	shape string,
) revisionCompletionFixture {
	t.Helper()
	conn := target.connect(t)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	faultConn, closeFaultConn := target.faultConn(t, conn)
	t.Cleanup(closeFaultConn)

	names := newRevisionCompletionNames(target.name + "_" + shape)
	target.dropObjects(faultConn, names)
	t.Cleanup(func() { target.dropObjects(faultConn, names) })

	return revisionCompletionFixture{target: target, conn: conn, faultConn: faultConn, names: names}
}

func (f revisionCompletionFixture) begin(c *qt.C, up, down string) *migrator.Migrator {
	c.Helper()
	mig := revisionCompletionMigrator(f.conn, f.names, up, down)
	f.target.installFault(c, f.faultConn, f.names)
	c.Assert(mig.Initialize(context.Background()), qt.IsNil)
	return mig
}

func (f revisionCompletionFixture) recoverWithRetry(c *qt.C, up, down string) {
	c.Helper()
	f.target.removeFault(c, f.faultConn, f.names)
	recovery := retryAfterFixingTheRevisionWrite()
	c.Assert(recovery.run(revisionCompletionMigrator(f.conn, f.names, up, down)), qt.IsNil)

	status, err := revisionCompletionMigrator(f.conn, f.names, up, down).GetMigrationStatus(context.Background())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{1})
}

func revisionCompletionRowCount(c *qt.C, conn *dbschema.DatabaseConnection, table string) int64 {
	c.Helper()
	var count int64
	c.Assert(conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table).Scan(&count), qt.IsNil)
	return count
}

// TestRevisionCompletionClasses_EveryClassIsCovered is the anti-silence guard.
// A matrix that reads as complete while a whole class has no case is the
// failure mode issue #999 names, so the classes the cases above declare are
// compared against every class internal/ddltx defines.
func TestRevisionCompletionClasses_EveryClassIsCovered(t *testing.T) {
	c := qt.New(t)

	covered := map[ddltx.Class][]string{}
	for _, target := range allRevisionCompletionTargets() {
		covered[target.class] = append(covered[target.class], target.name)
	}

	tests := []struct {
		name  string
		class ddltx.Class
	}{
		{name: "transactional", class: ddltx.Transactional},
		{name: "implicit commit", class: ddltx.ImplicitCommit},
		{name: "no transaction", class: ddltx.NoTransaction},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(covered[test.class], qt.Not(qt.HasLen), 0)
		})
	}
	c.Assert(covered, qt.HasLen, 3)
}

// TestRevisionCompletionClasses_ClassMatchesTheDialect keeps a case from
// declaring a class its dialect does not have, which would let the guard above
// pass while the scenario asserted the wrong invariant.
func TestRevisionCompletionClasses_ClassMatchesTheDialect(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
		class   ddltx.Class
	}{
		{name: "sqlite", dialect: "sqlite", class: ddltx.Transactional},
		{name: "postgres", dialect: "postgres", class: ddltx.Transactional},
		{name: "mysql", dialect: "mysql", class: ddltx.ImplicitCommit},
		{name: "mariadb", dialect: "mariadb", class: ddltx.ImplicitCommit},
		{name: "clickhouse", dialect: "clickhouse", class: ddltx.NoTransaction},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(ddltx.ClassOf(test.dialect), qt.Equals, test.class)
		})
	}
	c.Assert(allRevisionCompletionTargets(), qt.HasLen, len(tests))
}

// TestRevisionCompletionRepair_CoversExactlyTheSurvivingBodyClasses keeps the
// repair cases honest in both directions: a surviving-body target with no
// repair case would leave the manual recovery unproven, and a transactional
// target with one would be asserting that signing off a rolled-back body is
// fine.
func TestRevisionCompletionRepair_CoversExactlyTheSurvivingBodyClasses(t *testing.T) {
	c := qt.New(t)

	repaired := map[string]bool{}
	for _, target := range revisionCompletionRepairTargets() {
		repaired[target.name] = true
	}

	for _, target := range allRevisionCompletionTargets() {
		c.Run(target.name, func(c *qt.C) {
			c.Assert(
				repaired[target.name],
				qt.Equals,
				ddltx.BodySurvivesRevisionCompletionFailure(target.class),
			)
		})
	}
}

func allRevisionCompletionTargets() []revisionCompletionTarget {
	return []revisionCompletionTarget{
		sqliteRevisionCompletionTarget(),
		postgresRevisionCompletionTarget(),
		mySQLRevisionCompletionTarget(),
		mariaDBRevisionCompletionTarget(),
		clickHouseRevisionCompletionTarget(),
	}
}

// revisionCompletionRepairTargets lists the targets a
// TestRevisionCompletionRepair_* case exists for above.
func revisionCompletionRepairTargets() []revisionCompletionTarget {
	return []revisionCompletionTarget{
		mySQLRevisionCompletionTarget(),
		mariaDBRevisionCompletionTarget(),
		clickHouseRevisionCompletionTarget(),
	}
}

func mySQLRevisionCompletionTarget() revisionCompletionTarget {
	return mySQLFamilyRevisionCompletionTarget("mysql", "MYSQL_ADMIN_TEST_URL", "MYSQL_TEST_URL", "MYSQL_URL")
}

func mariaDBRevisionCompletionTarget() revisionCompletionTarget {
	return mySQLFamilyRevisionCompletionTarget(
		"mariadb",
		"MARIADB_ADMIN_TEST_URL",
		"MARIADB_TEST_URL",
		"MARIADB_URL",
	)
}

// revisionCompletionRecovery is one supported way out of a dirty revision left
// by a failed completion write. Which ones apply is decided by the class, not
// by the dialect.
type revisionCompletionRecovery struct {
	name string
	run  func(mig *migrator.Migrator) error
}

// retryAfterFixingTheRevisionWrite is the automatic recovery. The dirty row is
// reused and the run resumes at applied+1, which is the whole body on a
// transactional target (nothing survived) and past the durable prefix on the
// two classes where something did.
func retryAfterFixingTheRevisionWrite() revisionCompletionRecovery {
	return revisionCompletionRecovery{
		name: "allow-dirty retry",
		run: func(mig *migrator.Migrator) error {
			return mig.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{AllowDirty: true})
		},
	}
}

// markAppliedAfterTheBodyCommitted is the manual recovery, and it is only
// correct where the body survived: it records the migration applied without
// running anything, so on a transactional target -- whose body rolled back with
// the revision -- it would sign off a schema change that is not in the
// database. The matrix drives it for the surviving-body classes only, which is
// the repair guidance issue #999 asks to keep aligned with dialect semantics.
func markAppliedAfterTheBodyCommitted() revisionCompletionRecovery {
	return revisionCompletionRecovery{
		name: "repair",
		run: func(mig *migrator.Migrator) error {
			return mig.RepairMigration(context.Background(), migrator.RepairMigrationOptions{Version: 1})
		},
	}
}

// appliedAfterRevisionCompletionFailure is the progress a dirty revision must
// carry once the completion write has failed.
//
// The bodies in this file are a single DDL statement, so "the durable prefix"
// and "the whole body" are the same number and the class predicate answers for
// both surviving classes. A body mixing DDL and DML would separate them on the
// MySQL family, where only the statements up to the last implicit commit
// survive; that is measured by TestRolledBackProgress_* rather than here.
func appliedAfterRevisionCompletionFailure(class ddltx.Class, total int) int {
	if ddltx.BodySurvivesRevisionCompletionFailure(class) {
		return total
	}
	return 0
}

// runRevisionCompletionScenario drives one target through the whole scenario:
// the completion write fails, the class's body-survival rule is checked against
// the catalog, the dirty revision is read back, the plain retry is refused, and
// the supplied recovery is driven to a consistent head.
func runRevisionCompletionScenario(
	t *testing.T,
	target revisionCompletionTarget,
	recovery revisionCompletionRecovery,
) {
	c := qt.New(t)
	ctx := context.Background()
	conn := target.connect(t)
	defer dbschema.CloseAndWarn(conn)
	faultConn, closeFaultConn := target.faultConn(t, conn)
	defer closeFaultConn()

	names := newRevisionCompletionNames(target.name)
	target.dropObjects(faultConn, names)
	t.Cleanup(func() { target.dropObjects(faultConn, names) })

	up, down := target.createBody(names)
	mig := revisionCompletionMigrator(conn, names, up, down)

	target.installFault(c, faultConn, names)
	c.Assert(mig.Initialize(ctx), qt.IsNil)

	err := mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "failed to record migration 1")

	// The body-survival answer is the class's, not the dialect's, so it is read
	// from internal/ddltx rather than restated here per target.
	c.Assert(
		target.bodyPresent(c, conn, names),
		qt.Equals,
		ddltx.BodySurvivesRevisionCompletionFailure(target.class),
	)

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Version, qt.Equals, int64(1))
	c.Assert(status.DirtyRevision.State, qt.Equals, "failed")
	c.Assert(status.DirtyRevision.Total, qt.Equals, 1)
	// The progress number is the load-bearing half of "an accurate dirty state":
	// a retry resumes at applied+1, so a class whose body is already durable and
	// still reports zero sends the retry back over SQL the database has run.
	c.Assert(
		status.DirtyRevision.Applied,
		qt.Equals,
		appliedAfterRevisionCompletionFailure(target.class, status.DirtyRevision.Total),
	)
	// The diagnostic has to point at the metadata write. On the classes whose
	// body survives, a dirty row next to a committed table is only readable
	// correctly if the stored error names the revision table rather than the
	// migration body. See docs/system_design.md.
	c.Assert(status.DirtyRevision.Error, qt.Contains, names.revisions)
	c.Assert(status.DirtyRevision.Error, qt.Not(qt.Contains), names.body)

	retryErr := mig.MigrateUp(ctx)
	c.Assert(retryErr, qt.IsNotNil)
	c.Assert(migrator.IsDirtyMigration(retryErr), qt.IsTrue)

	target.removeFault(c, faultConn, names)
	c.Assert(recovery.run(revisionCompletionMigrator(conn, names, up, down)), qt.IsNil)

	finalStatus, err := revisionCompletionMigrator(conn, names, up, down).GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(finalStatus.DirtyRevision, qt.IsNil)
	c.Assert(finalStatus.AppliedMigrations, qt.DeepEquals, []int64{1})
	c.Assert(target.bodyPresent(c, conn, names), qt.IsTrue)
}

// reuseMigratorConnection is the default fault connection: the target installs
// its fault over the same connection the migrator uses.
func reuseMigratorConnection(
	_ *testing.T,
	conn *dbschema.DatabaseConnection,
) (*dbschema.DatabaseConnection, func()) {
	return conn, func() {}
}

func mySQLFamilyFaultConnection(
	dialect string,
	envNames ...string,
) func(*testing.T, *dbschema.DatabaseConnection) (*dbschema.DatabaseConnection, func()) {
	return func(t *testing.T, _ *dbschema.DatabaseConnection) (*dbschema.DatabaseConnection, func()) {
		t.Helper()
		adminConn, err := dbschema.ConnectToDatabase(t.Context(), mySQLFamilyTestURL(t, dialect, envNames...))
		qt.Assert(t, err, qt.IsNil)
		return adminConn, func() { dbschema.CloseAndWarn(adminConn) }
	}
}

func revisionCompletionMigrator(
	conn *dbschema.DatabaseConnection,
	names revisionCompletionNames,
	up, down string,
) *migrator.Migrator {
	return migrator.NewMigrator(
		conn,
		migrator.NewRegisteredMigrationProvider(
			migrator.CreateMigrationFromSQL(1, "revision completion", up, down),
		),
	).
		WithMigrationsTable("", names.revisions).
		WithMigrationLockTimeout(20 * time.Second)
}

func sqliteRevisionCompletionTarget() revisionCompletionTarget {
	return revisionCompletionTarget{
		name:      "sqlite",
		class:     ddltx.Transactional,
		faultConn: reuseMigratorConnection,
		connect: func(t *testing.T) *dbschema.DatabaseConnection {
			t.Helper()
			conn, err := dbschema.ConnectToDatabase(
				t.Context(),
				"sqlite://"+filepath.Join(t.TempDir(), "revision-completion.db"),
			)
			qt.Assert(t, err, qt.IsNil)
			return conn
		},
		createBody: func(names revisionCompletionNames) (string, string) {
			return fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY);", names.body),
				fmt.Sprintf("DROP TABLE %s;", names.body)
		},
		installFault: func(c *qt.C, conn *dbschema.DatabaseConnection, names revisionCompletionNames) {
			c.Helper()
			execRevisionCompletionSQL(c, conn, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'applied',
    applied INTEGER NOT NULL DEFAULT 1,
    total INTEGER NOT NULL DEFAULT 1,
    error TEXT NULL,
    error_stmt TEXT NULL,
    execution_time_ms BIGINT NOT NULL DEFAULT 0,
    checksum VARCHAR(64) NOT NULL DEFAULT ''
)`, names.revisions))
			execRevisionCompletionSQL(c, conn, fmt.Sprintf(`CREATE TRIGGER %s
BEFORE UPDATE OF state ON %s
WHEN NEW.state = 'applied'
BEGIN
	SELECT RAISE(ABORT, 'reject applied revision');
END`, names.fault, names.revisions))
		},
		removeFault: func(c *qt.C, conn *dbschema.DatabaseConnection, names revisionCompletionNames) {
			c.Helper()
			execRevisionCompletionSQL(c, conn, "DROP TRIGGER "+names.fault)
		},
		bodyPresent: func(c *qt.C, conn *dbschema.DatabaseConnection, names revisionCompletionNames) bool {
			c.Helper()
			var count int64
			c.Assert(conn.QueryRowContext(
				context.Background(),
				"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
				names.body,
			).Scan(&count), qt.IsNil)
			return count > 0
		},
		dropObjects: dropRevisionCompletionObjects("DROP TRIGGER IF EXISTS %[3]s", "DROP TABLE IF EXISTS %[2]s", "DROP TABLE IF EXISTS %[1]s"),
	}
}

func postgresRevisionCompletionTarget() revisionCompletionTarget {
	return revisionCompletionTarget{
		name:      "postgres",
		class:     ddltx.Transactional,
		faultConn: reuseMigratorConnection,
		connect: func(t *testing.T) *dbschema.DatabaseConnection {
			t.Helper()
			conn, err := dbschema.ConnectToDatabase(t.Context(), postgresTestURL(t))
			qt.Assert(t, err, qt.IsNil)
			return conn
		},
		createBody: func(names revisionCompletionNames) (string, string) {
			return fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.body),
				fmt.Sprintf("DROP TABLE %s", names.body)
		},
		installFault: func(c *qt.C, conn *dbschema.DatabaseConnection, names revisionCompletionNames) {
			c.Helper()
			execRevisionCompletionSQL(c, conn, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger AS $fault$
BEGIN
	IF NEW.state = 'applied' THEN
		RAISE EXCEPTION 'reject applied revision';
	END IF;
	RETURN NEW;
END;
$fault$ LANGUAGE plpgsql`, names.fault))
			execRevisionCompletionSQL(c, conn, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'applied',
    applied INTEGER NOT NULL DEFAULT 1,
    total INTEGER NOT NULL DEFAULT 1,
    error TEXT NULL,
    error_stmt TEXT NULL,
    execution_time_ms BIGINT NOT NULL DEFAULT 0,
    checksum VARCHAR(64) NOT NULL DEFAULT ''
)`, names.revisions))
			execRevisionCompletionSQL(c, conn, fmt.Sprintf(
				"CREATE TRIGGER %s BEFORE UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION %s()",
				names.fault,
				names.revisions,
				names.fault,
			))
		},
		removeFault: func(c *qt.C, conn *dbschema.DatabaseConnection, names revisionCompletionNames) {
			c.Helper()
			execRevisionCompletionSQL(c, conn, fmt.Sprintf("DROP TRIGGER %s ON %s", names.fault, names.revisions))
		},
		bodyPresent: postgresRevisionCompletionBodyPresent,
		dropObjects: dropRevisionCompletionObjects(
			"DROP TABLE IF EXISTS %[2]s",
			"DROP TABLE IF EXISTS %[1]s",
			"DROP FUNCTION IF EXISTS %[3]s()",
		),
	}
}

func postgresRevisionCompletionBodyPresent(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	names revisionCompletionNames,
) bool {
	c.Helper()
	var count int64
	c.Assert(conn.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = current_schema() AND table_name = $1 AND table_type = 'BASE TABLE'`,
		names.body,
	).Scan(&count), qt.IsNil)
	return count > 0
}

func mySQLFamilyRevisionCompletionTarget(dialect string, adminEnv string, envNames ...string) revisionCompletionTarget {
	return revisionCompletionTarget{
		name:  dialect,
		class: ddltx.ImplicitCommit,
		connect: func(t *testing.T) *dbschema.DatabaseConnection {
			t.Helper()
			conn, err := dbschema.ConnectToDatabase(t.Context(), mySQLFamilyTestURL(t, dialect, envNames...))
			qt.Assert(t, err, qt.IsNil)
			return conn
		},
		faultConn: mySQLFamilyFaultConnection(dialect, append([]string{adminEnv}, envNames...)...),
		createBody: func(names revisionCompletionNames) (string, string) {
			return fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY) ENGINE=InnoDB", names.body),
				fmt.Sprintf("DROP TABLE %s", names.body)
		},
		installFault: func(c *qt.C, conn *dbschema.DatabaseConnection, names revisionCompletionNames) {
			c.Helper()
			execRevisionCompletionSQL(c, conn, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'applied',
    applied INTEGER NOT NULL DEFAULT 1,
    total INTEGER NOT NULL DEFAULT 1,
    error TEXT NULL,
    error_stmt TEXT NULL,
    execution_time_ms BIGINT NOT NULL DEFAULT 0,
    checksum VARCHAR(64) NOT NULL DEFAULT ''
) ENGINE=InnoDB`, names.revisions))
			execRevisionCompletionSQL(c, conn, fmt.Sprintf(`CREATE TRIGGER %s
BEFORE UPDATE ON %s
FOR EACH ROW
BEGIN
	IF NEW.state = 'applied' THEN
		SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'reject applied revision';
	END IF;
END`, names.fault, names.revisions))
		},
		removeFault: func(c *qt.C, conn *dbschema.DatabaseConnection, names revisionCompletionNames) {
			c.Helper()
			execRevisionCompletionSQL(c, conn, "DROP TRIGGER "+names.fault)
		},
		bodyPresent: mySQLFamilyRevisionCompletionBodyPresent,
		dropObjects: dropRevisionCompletionObjects(
			"DROP TRIGGER IF EXISTS %[3]s",
			"DROP TABLE IF EXISTS %[2]s",
			"DROP TABLE IF EXISTS %[1]s",
		),
	}
}

func mySQLFamilyRevisionCompletionBodyPresent(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	names revisionCompletionNames,
) bool {
	c.Helper()
	var count int64
	c.Assert(conn.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name = ? AND table_type = 'BASE TABLE'`,
		names.body,
	).Scan(&count), qt.IsNil)
	return count > 0
}

func clickHouseRevisionCompletionTarget() revisionCompletionTarget {
	return revisionCompletionTarget{
		name:      "clickhouse",
		class:     ddltx.NoTransaction,
		faultConn: reuseMigratorConnection,
		connect: func(t *testing.T) *dbschema.DatabaseConnection {
			t.Helper()
			dbURL := os.Getenv("CLICKHOUSE_URL")
			if dbURL == "" {
				t.Skip("CLICKHOUSE_URL not set")
			}
			conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
			qt.Assert(t, err, qt.IsNil)
			return conn
		},
		createBody: func(names revisionCompletionNames) (string, string) {
			return fmt.Sprintf("CREATE TABLE %s (id Int64) ENGINE = MergeTree ORDER BY id", names.body),
				fmt.Sprintf("DROP TABLE %s", names.body)
		},
		// The sorting key is the fault. ClickHouse rejects ALTER ... UPDATE of a
		// key column, and applied_at is written by the completion statement and
		// by no other revision write, so the completion write is the only one
		// that fails.
		installFault: func(c *qt.C, conn *dbschema.DatabaseConnection, names revisionCompletionNames) {
			c.Helper()
			execRevisionCompletionSQL(c, conn, clickHouseRevisionTableDDL(names.revisions, "(version, applied_at)"))
		},
		// Rebuilding the table with the sorting key Ptah would have created is
		// what an operator does to undo this, and it carries the dirty revision
		// row across so the recovery below still sees it.
		removeFault: func(c *qt.C, conn *dbschema.DatabaseConnection, names revisionCompletionNames) {
			c.Helper()
			repaired := names.revisions + "_repaired"
			execRevisionCompletionSQL(c, conn, "DROP TABLE IF EXISTS "+repaired)
			execRevisionCompletionSQL(c, conn, clickHouseRevisionTableDDL(repaired, "tuple(version)"))
			execRevisionCompletionSQL(c, conn, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", repaired, names.revisions))
			execRevisionCompletionSQL(c, conn, "DROP TABLE "+names.revisions)
			execRevisionCompletionSQL(c, conn, fmt.Sprintf("RENAME TABLE %s TO %s", repaired, names.revisions))
		},
		bodyPresent: clickHouseRevisionCompletionBodyPresent,
		dropObjects: dropRevisionCompletionObjects(
			"DROP TABLE IF EXISTS %[2]s",
			"DROP TABLE IF EXISTS %[1]s",
			"DROP TABLE IF EXISTS %[1]s_repaired",
		),
	}
}

// clickHouseRevisionTableDDL mirrors the ptah-layout revision table the
// migrator's CREATE TABLE IF NOT EXISTS produces on ClickHouse, with the
// sorting key as the only variable. Reproduce the reference shape with
// SHOW CREATE TABLE after a plain Initialize.
func clickHouseRevisionTableDDL(table, orderBy string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
    version Int64,
    description String,
    applied_at DateTime,
    state String DEFAULT 'applied',
    applied Int32 DEFAULT 1,
    total Int32 DEFAULT 1,
    error Nullable(String),
    error_stmt Nullable(String),
    execution_time_ms Int64 DEFAULT 0,
    checksum String DEFAULT ''
) ENGINE = MergeTree ORDER BY %s`, table, orderBy)
}

func clickHouseRevisionCompletionBodyPresent(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	names revisionCompletionNames,
) bool {
	c.Helper()
	var count uint64
	c.Assert(conn.QueryRowContext(
		context.Background(),
		"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = ?",
		names.body,
	).Scan(&count), qt.IsNil)
	return count > 0
}

// dropObjects deliberately ignores errors: it runs before the objects exist
// and again after some of them have been dropped by the scenario.
func dropRevisionCompletionObjects(templates ...string) func(*dbschema.DatabaseConnection, revisionCompletionNames) {
	return func(conn *dbschema.DatabaseConnection, names revisionCompletionNames) {
		for _, template := range templates {
			_, _ = conn.ExecContext(
				context.Background(),
				fmt.Sprintf(template, names.revisions, names.body, names.fault),
			)
		}
	}
}

func execRevisionCompletionSQL(c *qt.C, conn *dbschema.DatabaseConnection, statement string) {
	c.Helper()
	_, err := conn.ExecContext(context.Background(), statement)
	c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", strings.TrimSpace(statement)))
}
