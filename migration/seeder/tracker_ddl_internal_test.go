package seeder

// White-box testing required: the statement that creates the tracker table
// leaves nothing behind but the table, so without a live server of each engine
// the only place to read which spelling SQL Server and Oracle are sent is
// trackerDDL. integration/seed_tracker_e2e_test.go runs both spellings through
// the shipped binary against live servers.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// TestTrackerDDL_SQLServerGuardsWithObjectIDAndStoresDatetime2 pins both
// defects stokaro/ptah#3287 found on SQL Server. T-SQL refuses CREATE TABLE IF
// NOT EXISTS at parse time with Msg 156, before any seed file is read. Behind
// that, TIMESTAMP is rowversion, so recording the first seed fails with Msg 273.
func TestTrackerDDL_SQLServerGuardsWithObjectIDAndStoresDatetime2(t *testing.T) {
	c := qt.New(t)

	ddl := trackerDDL(platform.SQLServer)

	c.Assert(ddl, qt.Not(qt.Contains), "IF NOT EXISTS")
	c.Assert(ddl, qt.Not(qt.Contains), "TIMESTAMP")
	c.Assert(ddl, qt.Equals, `IF OBJECT_ID(N'schema_seeds', N'U') IS NULL
BEGIN
    CREATE TABLE schema_seeds (
        seed_path VARCHAR(512) PRIMARY KEY,
        env VARCHAR(128) NOT NULL,
        checksum CHAR(64) NOT NULL,
        applied_at DATETIME2 NOT NULL
    )
END`)
}

// TestTrackerDDL_OracleCreatesFromAPLSQLBlock pins the Oracle half of
// stokaro/ptah#3287. Oracle 21 refuses CREATE TABLE IF NOT EXISTS with
// ORA-00922, so the tracker is created from a block that ignores ORA-00955 and
// raises every other error.
func TestTrackerDDL_OracleCreatesFromAPLSQLBlock(t *testing.T) {
	c := qt.New(t)

	ddl := trackerDDL(platform.Oracle)

	c.Assert(ddl, qt.Not(qt.Contains), "IF NOT EXISTS")
	c.Assert(ddl, qt.Equals, `BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE schema_seeds (
        seed_path VARCHAR(512) PRIMARY KEY,
        env VARCHAR(128) NOT NULL,
        checksum CHAR(64) NOT NULL,
        applied_at TIMESTAMP NOT NULL
    )';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -955 THEN
            RAISE;
        END IF;
END;`)
}

// TestTrackerDDL_PortableDialectsKeepTheIfNotExistsGuard is the control for the
// two tests above: the dialects that accept the portable statement are still
// sent it.
func TestTrackerDDL_PortableDialectsKeepTheIfNotExistsGuard(t *testing.T) {
	for _, dialect := range []string{
		platform.Postgres,
		platform.MySQL,
		platform.MariaDB,
		platform.SQLite,
		platform.CockroachDB,
		platform.YugabyteDB,
	} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(trackerDDL(dialect), qt.Equals, `CREATE TABLE IF NOT EXISTS schema_seeds (
    seed_path VARCHAR(512) PRIMARY KEY,
    env VARCHAR(128) NOT NULL,
    checksum CHAR(64) NOT NULL,
    applied_at TIMESTAMP NOT NULL
)`)
		})
	}
}

// TestSavepointStatements_SpellsWhatEachEngineTakes pins the statements that
// bracket one seed file under --idempotent.
//
// The portable spelling reached every engine, and two refuse it. Measured:
// SQL Server answers SAVEPOINT with `Could not find stored procedure
// 'SAVEPOINT'` (2812) and RELEASE SAVEPOINT with Msg 102; Oracle answers
// RELEASE SAVEPOINT with ORA-00900 (stokaro/ptah#3330).
func TestSavepointStatements_SpellsWhatEachEngineTakes(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		create   string
		rollback string
		release  string
	}{
		{
			name:     "sqlserver has its own pair and no release",
			dialect:  platform.SQLServer,
			create:   "SAVE TRANSACTION ptah_seed_file",
			rollback: "ROLLBACK TRANSACTION ptah_seed_file",
			release:  "",
		},
		{
			name:     "oracle takes the portable pair and no release",
			dialect:  platform.Oracle,
			create:   "SAVEPOINT ptah_seed_file",
			rollback: "ROLLBACK TO SAVEPOINT ptah_seed_file",
			release:  "",
		},
		{
			name:     "postgres takes all three",
			dialect:  platform.Postgres,
			create:   "SAVEPOINT ptah_seed_file",
			rollback: "ROLLBACK TO SAVEPOINT ptah_seed_file",
			release:  "RELEASE SAVEPOINT ptah_seed_file",
		},
		{
			name:     "mysql takes all three",
			dialect:  platform.MySQL,
			create:   "SAVEPOINT ptah_seed_file",
			rollback: "ROLLBACK TO SAVEPOINT ptah_seed_file",
			release:  "RELEASE SAVEPOINT ptah_seed_file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := savepointStatements(tt.dialect)
			c.Assert(got.create, qt.Equals, tt.create)
			c.Assert(got.rollback, qt.Equals, tt.rollback)
			c.Assert(got.release, qt.Equals, tt.release)
		})
	}
}

// TestTrackerDDL_SpannerAvoidsTheTypesTheEndpointLacks pins the statement
// Spanner is sent.
//
// Its PostgreSQL interface has no bpchar and no TIMESTAMP, and the tracker used
// both. The endpoint answered `Type <bpchar> is not supported. (SQLSTATE
// P0001)`, which is where `ptah seed` stopped (stokaro/ptah#3325).
func TestTrackerDDL_SpannerAvoidsTheTypesTheEndpointLacks(t *testing.T) {
	c := qt.New(t)

	ddl := trackerDDL(platform.Spanner)

	c.Assert(ddl, qt.Contains, "CREATE TABLE IF NOT EXISTS schema_seeds")
	c.Assert(ddl, qt.Contains, "checksum TEXT NOT NULL")
	c.Assert(ddl, qt.Contains, "applied_at TIMESTAMPTZ NOT NULL")
	c.Assert(ddl, qt.Not(qt.Contains), "CHAR(64)")
}
