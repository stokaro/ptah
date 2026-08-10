//go:build integration

package migrator_test

// Revision-completion failures, one case per DDL transaction capability class
// (issue #999).
//
// Ptah writes a migration's body and the revision row that records the
// migration as applied. What a failure of the second write leaves behind is
// decided by the target's DDL transaction contract, and internal/ddltx is the
// enumeration of those contracts. Every class named there is driven here and
// in revision_completion_class_live_test.go:
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

// ClickHouse recovers by repair rather than by the allow-dirty retry, and that
// is a limitation rather than a property of the class: see
// appliedAfterRevisionCompletionFailure.
// The repair cases below take the other way out. Repair signs the migration off
// as applied without running anything, so it belongs to the classes whose body
// survived the failure and to no others -- which is what
// TestRevisionCompletionRepair_CoversExactlyTheSurvivingBodyClasses pins.

// ClickHouse has no separate repair case because its failure case above already
// recovers by repair.

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

	for _, class := range ddltx.Classes() {
		c.Run(string(class), func(c *qt.C) {
			c.Assert(covered[class], qt.Not(qt.HasLen), 0)
		})
	}
	c.Assert(covered, qt.HasLen, len(ddltx.Classes()))
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
// implicit-commit class.
func retryAfterFixingTheRevisionWrite() revisionCompletionRecovery {
	return revisionCompletionRecovery{
		name: "allow-dirty retry",
		run: func(mig *migrator.Migrator) error {
			return mig.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{AllowDirty: true})
		},
	}
}

// appliedAfterRevisionCompletionFailure is the progress a dirty revision
// carries once the completion write has failed.
//
// Two of the three answers are the contract. A transactional target rolled the
// body back with the revision, so zero is right. An implicit-commit target
// reports the prefix its revision-row witness recorded, which for the
// single-DDL bodies here is the whole body.
//
// The no-transaction answer is a characterization, not a contract, and it is
// wrong. Every ClickHouse statement is durable the moment it runs, so the
// revision should say the body is fully applied; Ptah records zero, because the
// completion failure carries no statement index and nothing supplies one. The
// consequence was measured live: an --allow-dirty retry resumes at applied+1,
// re-issues the body's CREATE TABLE and fails with "table already exists", so
// repair is the only recovery this class has today. The number is asserted so
// that the fix has a test to flip rather than a silent behavior change; it is
// not a statement that zero is correct. See the issue this file was added under.
func appliedAfterRevisionCompletionFailure(class ddltx.Class, total int) int {
	if class == ddltx.ImplicitCommit {
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
