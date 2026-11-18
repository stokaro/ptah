package migrator

// White-box testing required: whether a refused INSERT or a rewrite that
// matched nothing is another run's claim depends on what this run read before
// it wrote, and the race that makes the two differ needs a second run to write
// between one run's read and its write. No exported entry point offers that
// gap, so these tests read the plan, change the row the way the other run
// would, and drive the write with the plan they read.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	mssql "github.com/microsoft/go-mssqldb"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
)

// claimMigrations is one migration whose first statement runs and whose second
// fails until the table it writes exists, so the first run leaves a row that
// failed with one statement applied.
func claimMigrations() fstest.MapFS {
	return fstest.MapFS{
		"0000000001_repairs.up.sql": &fstest.MapFile{Data: []byte(
			"CREATE TABLE first_done (id INTEGER);\nINSERT INTO repairs (id) VALUES (1);\n",
		)},
		"0000000001_repairs.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE first_done;\n")},
	}
}

// claimMigrator returns a migrator over a SQLite database of its own in
// format, with an initialized revision table and no row in it.
func claimMigrator(c *qt.C, format RevisionTableFormat) *Migrator {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+filepath.Join(c.TempDir(), "claim.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	m, err := NewFSMigrator(conn, claimMigrations())
	c.Assert(err, qt.IsNil)
	m = m.WithRevisionTableFormat(format).WithTransactionMode(MigrationTxModeNone)
	c.Assert(m.Initialize(context.Background()), qt.IsNil)
	return m
}

// failedClaimMigrator returns a claim migrator whose first run failed at the
// second statement, so its revision row is dirty with one statement applied,
// and the plan a retry reads from that row.
func failedClaimMigrator(c *qt.C, format RevisionTableFormat) (*Migrator, upRetryPlan) {
	c.Helper()
	m := claimMigrator(c, format)
	c.Assert(m.MigrateUp(context.Background()), qt.IsNotNil)
	c.Assert(m.Initialize(context.Background()), qt.IsNil)
	plan, err := m.planUpRetry(context.Background(), m.migrationByVersion(1))
	c.Assert(err, qt.IsNil)
	c.Assert(plan.reuseRevision, qt.IsTrue)
	c.Assert(plan.readApplied, qt.Equals, 1)
	return m, plan
}

// execOn runs statement on m's own connection.
func execOn(c *qt.C, m *Migrator, statement string) {
	c.Helper()
	_, err := m.conn.ExecContext(context.Background(), statement)
	c.Assert(err, qt.IsNil)
}

// Connections a restart records on.
const (
	onRoot        = "the migrator's own connection"
	onPinned      = "a pinned session of a scoped migrator"
	onTransaction = "a copy bound to a transaction"
)

// restartOn restarts the row of migration 1 from plan on the connection on
// names, as the attempt that records there would.
func restartOn(c *qt.C, m *Migrator, on string, plan upRetryPlan) error {
	c.Helper()
	ctx := context.Background()
	migration := m.migrationByVersion(1)
	switch on {
	case onPinned:
		return m.conn.WithSession(ctx, func(session *dbschema.DatabaseConnection) error {
			scoped := *m
			scoped.conn = session
			return scoped.restartMigrationRevisionOn(ctx, session, migration, time.Now(), plan)
		})
	case onTransaction:
		tx, err := m.conn.SchemaWriter().BeginTransaction(ctx)
		c.Assert(err, qt.IsNil)
		restartErr := m.restartMigrationRevisionOn(ctx, m.conn.WithExecutor(tx), migration, time.Now(), plan)
		c.Assert(tx.Commit(), qt.IsNil)
		return restartErr
	default:
		return m.restartMigrationRevisionOn(ctx, m.conn, migration, time.Now(), plan)
	}
}

// A row nobody changed since this run read it passes the guard wherever the
// guard runs, and is skipped where the count would be a stand-in: a copy bound
// to a transaction answers zero rows whatever the UPDATE matched, so a guard
// there would report every restart as lost.
func TestRestartMigrationRevision_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		format    RevisionTableFormat
		on        string
		wantState string
	}{
		{name: "native, unchanged, own connection", format: RevisionTableFormatPtah, on: onRoot, wantState: "pending"},
		{name: "native, unchanged, pinned session", format: RevisionTableFormatPtah, on: onPinned, wantState: "pending"},
		{name: "native, unchanged, transaction", format: RevisionTableFormatPtah, on: onTransaction, wantState: "pending"},
		// The Atlas layout has no state column, so a restarted row with
		// statements left reads as an attempt that stopped.
		{name: "atlas, unchanged, own connection", format: RevisionTableFormatAtlas, on: onRoot, wantState: "failed"},
		{name: "atlas, unchanged, transaction", format: RevisionTableFormatAtlas, on: onTransaction, wantState: "failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			m, plan := failedClaimMigrator(c, test.format)

			err := restartOn(c, m, test.on, plan)

			c.Assert(err, qt.IsNil)
			revision, err := m.getMigrationRevision(context.Background(), m.migrationByVersion(1))
			c.Assert(err, qt.IsNil)
			c.Assert(revision.Error, qt.Equals, "")
			c.Assert(revision.Applied, qt.Equals, 1)
			c.Assert(revision.State, qt.Equals, test.wantState)
		})
	}
}

// A row another retry rewrote after this run read it is not rewritten again:
// another state, a further statement applied, another checksum, or, in the
// Atlas layout, an error another retry cleared. A pinned session counts the
// rows it matched, so it refuses as the migrator's own connection does.
func TestRestartMigrationRevision_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		format  RevisionTableFormat
		on      string
		rewrite string
		wantErr string
	}{
		{
			name:    "native, another state",
			format:  RevisionTableFormatPtah,
			on:      onRoot,
			rewrite: "UPDATE schema_migrations SET state = 'pending'",
			wantErr: `.*the row of migration 1 no longer has state "failed", 1 applied statement and the checksum this run read`,
		},
		{
			name:    "native, a further statement applied",
			format:  RevisionTableFormatPtah,
			on:      onRoot,
			rewrite: "UPDATE schema_migrations SET applied = 2",
			wantErr: `.*no longer has state "failed", 1 applied statement.*`,
		},
		{
			name:    "native, another checksum",
			format:  RevisionTableFormatPtah,
			on:      onRoot,
			rewrite: "UPDATE schema_migrations SET checksum = 'partial:h1:another'",
			wantErr: `.*and the checksum this run read`,
		},
		{
			name:    "native, another state, pinned session",
			format:  RevisionTableFormatPtah,
			on:      onPinned,
			rewrite: "UPDATE schema_migrations SET state = 'pending'",
			wantErr: `.*no longer has state "failed".*`,
		},
		{
			name:    "atlas, the error cleared",
			format:  RevisionTableFormatAtlas,
			on:      onRoot,
			rewrite: "UPDATE atlas_schema_revisions SET error = ''",
			wantErr: `.*the row of migration 1 no longer has the 1 applied statement and the error this run read`,
		},
		{
			name:    "atlas, a further statement applied",
			format:  RevisionTableFormatAtlas,
			on:      onRoot,
			rewrite: "UPDATE atlas_schema_revisions SET applied = 2",
			wantErr: `.*no longer has the 1 applied statement.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			m, plan := failedClaimMigrator(c, test.format)
			execOn(c, m, test.rewrite)

			err := restartOn(c, m, test.on, plan)

			c.Assert(err, qt.ErrorIs, ErrRevisionClaimLost)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// A copy bound to a transaction skips the guard in both layouts, so a stale
// read there is rewritten rather than refused. What makes it safe is the
// transaction, which the lock or the engine serializes, not the guard.
func TestRestartMigrationRevision_TransactionSkipsTheGuard(t *testing.T) {
	tests := []struct {
		name    string
		format  RevisionTableFormat
		rewrite string
	}{
		{name: "native", format: RevisionTableFormatPtah, rewrite: "UPDATE schema_migrations SET state = 'pending'"},
		{name: "atlas", format: RevisionTableFormatAtlas, rewrite: "UPDATE atlas_schema_revisions SET error = ''"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			m, plan := failedClaimMigrator(c, test.format)
			execOn(c, m, test.rewrite)

			err := restartOn(c, m, onTransaction, plan)

			c.Assert(err, qt.IsNil)
		})
	}
}

// The guard runs where the count is the driver's: on the migrator's own
// connection, and not on a copy bound to a transaction or on an engine whose
// UPDATE counts no rows.
func TestRestartCounts(t *testing.T) {
	c := qt.New(t)
	m := claimMigrator(c, RevisionTableFormatPtah)
	tx, err := m.conn.SchemaWriter().BeginTransaction(context.Background())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = tx.Rollback() })
	bound := m.conn.WithExecutor(tx)

	tests := []struct {
		name    string
		dialect string
		conn    *dbschema.DatabaseConnection
		want    bool
	}{
		{name: "the root connection of a counting engine", dialect: platform.SQLite, conn: m.conn, want: true},
		{name: "a copy bound to a transaction", dialect: platform.SQLite, conn: bound},
		{name: "an engine whose UPDATE counts no rows", dialect: platform.ClickHouse, conn: m.conn},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(restartCounts(revisionTableFor(test.dialect), m.conn, test.conn), qt.Equals, test.want)
		})
	}
	c.Assert(m.countsRevisionRestart(m.conn), qt.IsTrue)
	c.Assert(m.countsRevisionRestart(bound), qt.IsFalse)
}

// Server answers that say a row already holds the key.
var duplicateKeys = []struct {
	name string
	err  error
}{
	{name: "PostgreSQL", err: &pgconn.PgError{Code: "23505"}},
	{name: "MySQL", err: &mysqldriver.MySQLError{Number: 1062}},
	{name: "SQL Server primary key", err: mssql.Error{Number: 2627}},
	{name: "SQL Server unique index", err: mssql.Error{Number: 2601}},
}

// recordRow writes the revision row of migration 1 the way a run that holds
// it would, and reads it back to say it is there.
func recordRow(c *qt.C, m *Migrator) {
	c.Helper()
	migration := m.migrationByVersion(1)
	c.Assert(m.beginMigrationRevisionOn(context.Background(), m.conn, migration, time.Now(),
		upRetryPlan{readNoRow: true, resumeFrom: 1}), qt.IsNil)
}

// On SQLite the INSERT itself meets the row: the driver reports the duplicate
// with its own extended result code, which is what lets the claim read it.
// Without that code the run returns the bare UNIQUE refusal. The Atlas layout
// keeps no state, and a row with fewer applied statements than its total
// reads as failed.
func TestBeginMigrationRevision_SQLiteDuplicateIsAClaimLost(t *testing.T) {
	tests := []struct {
		format    RevisionTableFormat
		wantState string
	}{
		{format: RevisionTableFormatPtah, wantState: "pending"},
		{format: RevisionTableFormatAtlas, wantState: "failed"},
	}
	for _, test := range tests {
		t.Run(string(test.format), func(t *testing.T) {
			c := qt.New(t)
			m := claimMigrator(c, test.format)
			recordRow(c, m)

			err := m.beginMigrationRevisionOn(context.Background(), m.conn, m.migrationByVersion(1), time.Now(),
				upRetryPlan{readNoRow: true, resumeFrom: 1})

			c.Assert(err, qt.ErrorIs, ErrRevisionClaimLost)
			c.Assert(err, qt.ErrorMatches, `(?s).*the row of migration 1 has state "`+test.wantState+`".*UNIQUE constraint failed.*`)
		})
	}
}

// A run that read no row, and whose INSERT the server refused as a duplicate
// key over a row that is there now, lost the claim to the run that wrote it.
func TestSettleRevisionInsert_ClaimLost(t *testing.T) {
	for _, test := range duplicateKeys {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			m := claimMigrator(c, RevisionTableFormatPtah)
			recordRow(c, m)
			insertErr := fmt.Errorf("record: %w", test.err)

			err := m.settleRevisionInsert(context.Background(), m.conn, m.migrationByVersion(1),
				upRetryPlan{readNoRow: true, resumeFrom: 1}, insertErr)

			c.Assert(err, qt.ErrorIs, ErrRevisionClaimLost)
			c.Assert(err, qt.ErrorIs, insertErr)
			c.Assert(err, qt.ErrorMatches, `.*the row of migration 1 has state "pending".*`)
		})
	}
}

// Every other refused INSERT over a row that is there is reported as the
// server answered it: one from a run that read a row there, or did not read
// the table, as a dry run or a legacy table does not, which is a migration
// applied again rather than a race; a refusal that is not a duplicate key; and
// an error no server answered.
func TestSettleRevisionInsert_LeavesTheErrorAsItIs(t *testing.T) {
	tests := []struct {
		name      string
		readNoRow bool
		insertErr error
	}{
		{name: "a run that read a row or did not read", insertErr: &pgconn.PgError{Code: "23505"}},
		{name: "a refused privilege", readNoRow: true, insertErr: &pgconn.PgError{Code: "42501"}},
		{name: "an exceeded quota", readNoRow: true, insertErr: &pgconn.PgError{Code: "53100"}},
		{name: "a MySQL deadlock", readNoRow: true, insertErr: &mysqldriver.MySQLError{Number: 1213}},
		{name: "a SQL Server deadlock victim", readNoRow: true, insertErr: &mssql.Error{Number: 1205}},
		{name: "an error no server answered", readNoRow: true, insertErr: errors.New("connection reset by peer")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			m := claimMigrator(c, RevisionTableFormatPtah)
			recordRow(c, m)

			err := m.settleRevisionInsert(context.Background(), m.conn, m.migrationByVersion(1),
				upRetryPlan{readNoRow: test.readNoRow, resumeFrom: 1}, test.insertErr)

			c.Assert(err, qt.Equals, test.insertErr)
		})
	}
}

// A duplicate key with no row to show for it is not a claim another run made,
// such as a key another table holds, and stands as the server answered it.
func TestSettleRevisionInsert_DuplicateKeyWithoutARow(t *testing.T) {
	c := qt.New(t)
	m := claimMigrator(c, RevisionTableFormatPtah)
	insertErr := &pgconn.PgError{Code: "23505"}

	err := m.settleRevisionInsert(context.Background(), m.conn, m.migrationByVersion(1),
		upRetryPlan{readNoRow: true, resumeFrom: 1}, insertErr)

	c.Assert(err, qt.Equals, error(insertErr))
}

// The plan says it found no row only when it read the table and found none:
// a clean row is a migration applied again, and a dirty one is a retry.
func TestPlanUpRetry_ReadNoRow(t *testing.T) {
	c := qt.New(t)
	m := claimMigrator(c, RevisionTableFormatPtah)
	migration := m.migrationByVersion(1)

	empty, err := m.planUpRetry(context.Background(), migration)
	c.Assert(err, qt.IsNil)
	c.Assert(empty.readNoRow, qt.IsTrue)
	c.Assert(empty.reuseRevision, qt.IsFalse)

	recordRow(c, m)
	execOn(c, m, "UPDATE schema_migrations SET state = 'applied', applied = total")
	clean, err := m.planUpRetry(context.Background(), migration)
	c.Assert(err, qt.IsNil)
	c.Assert(clean.readNoRow, qt.IsFalse)
	c.Assert(clean.reuseRevision, qt.IsFalse)

	legacy := *m
	legacy.legacyRevisionTable = true
	unread, err := legacy.planUpRetry(context.Background(), migration)
	c.Assert(err, qt.IsNil)
	c.Assert(unread.readNoRow, qt.IsFalse)
}
