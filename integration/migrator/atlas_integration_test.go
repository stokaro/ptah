//go:build integration

package migrator_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestAtlasFormat_PostgresIntegration(t *testing.T) {
	runAtlasFormatIntegration(t, postgresTestURL(t))
}

func TestAtlasFormat_MySQLIntegration(t *testing.T) {
	runAtlasFormatIntegration(t, mysqlAtlasTestURL(t))
}

func TestAtlasExactRevisionIdentityCollation_MySQLIntegration(t *testing.T) {
	runAtlasExactRevisionIdentityCollationIntegration(t, mysqlAtlasTestURL(t))
}

func TestAtlasExactRevisionIdentityCollation_MariaDBIntegration(t *testing.T) {
	runAtlasExactRevisionIdentityCollationIntegration(t, mariaDBAtlasTestURL(t))
}

func TestAtlasExactRevisionIdentityCollation_ExistingMySQLTableRefusesAliasesIntegration(t *testing.T) {
	runAtlasRevisionIdentityCollationRefusalIntegration(t, mysqlAtlasTestURL(t), "utf8mb4_0900_ai_ci")
}

func TestAtlasExactRevisionIdentityCollation_ExistingMariaDBTableRefusesAliasesIntegration(t *testing.T) {
	runAtlasRevisionIdentityCollationRefusalIntegration(t, mariaDBAtlasTestURL(t), "utf8mb4_general_ci")
}

func TestAtlasExactRevisionIdentityCollation_SQLServerIntegration(t *testing.T) {
	runAtlasExactRevisionIdentityCollationIntegration(t, sqlServerTestURL(t))
}

func TestAtlasExactRevisionIdentityCollation_ExistingSQLServerTableRefusesAliasesIntegration(t *testing.T) {
	runAtlasRevisionIdentityCollationSQLServerRefusalIntegration(t, sqlServerTestURL(t))
}

func TestAtlasTxtarDown_PostgresIntegration(t *testing.T) {
	runAtlasTxtarDownIntegration(t, postgresTestURL(t))
}

func TestAtlasTemplate_PostgresIntegration(t *testing.T) {
	runAtlasTemplateIntegration(t, postgresTestURL(t))
}

func TestAtlasRevisionTable_PostgresIntegration(t *testing.T) {
	runAtlasRevisionTableIntegration(t, postgresTestURL(t))
}

func TestAtlasRevisionTable_PostgresUsesTimestamptzIntegration(t *testing.T) {
	runAtlasRevisionTableTimestampTypeIntegration(t, postgresTestURL(t))
}

func TestAtlasRevisionTable_CockroachDBUsesTimestamptzIntegration(t *testing.T) {
	runAtlasRevisionTableTimestampTypeIntegration(t, cockroachDBAtlasTestURL(t))
}

func TestAtlasRevisionTable_YugabyteDBUsesTimestamptzIntegration(t *testing.T) {
	runAtlasRevisionTableTimestampTypeIntegration(t, yugabyteDBAtlasTestURL(t))
}

func runAtlasRevisionTableTimestampTypeIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanupIssue275(t, conn)
	defer cleanupIssue275(t, conn)

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_create_users.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.Initialize(t.Context()), qt.IsNil)

	var dataType string
	err = conn.QueryRowContext(
		t.Context(),
		`SELECT data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'atlas_schema_revisions'
  AND column_name = 'executed_at'`,
	).Scan(&dataType)
	c.Assert(err, qt.IsNil)
	c.Assert(dataType, qt.Equals, "timestamp with time zone")
}

func TestAtlasTxtarDown_MySQLIntegration(t *testing.T) {
	runAtlasTxtarDownIntegration(t, mysqlAtlasTestURL(t))
}

func TestAtlasRevisionMetadata_PostgresIntegration(t *testing.T) {
	runAtlasRevisionMetadataIntegration(t, postgresTestURL(t))
}

func TestAtlasRevisionMetadata_MySQLIntegration(t *testing.T) {
	runAtlasRevisionMetadataIntegration(t, mysqlAtlasTestURL(t))
}

func TestAtlasDotMetadata_PostgresIntegration(t *testing.T) {
	runAtlasDotMetadataIntegration(t, postgresTestURL(t))
}

func TestAtlasDotMetadata_MySQLIntegration(t *testing.T) {
	runAtlasDotMetadataIntegration(t, mysqlAtlasTestURL(t))
}

func TestAtlasTxtarChecks_PostgresIntegration(t *testing.T) {
	runAtlasTxtarChecksIntegration(t, postgresTestURL(t), migrator.RevisionTableFormatAtlas)
}

func TestAtlasTxtarChecks_PostgresSessionLockReleasedIntegration(t *testing.T) {
	runAtlasTxtarChecksPostgresSessionLockIntegration(t, postgresTestURL(t))
}

func TestAtlasTxtarChecks_PostgresEscapeStringIntegration(t *testing.T) {
	runAtlasTxtarChecksPostgresEscapeStringIntegration(t, postgresTestURL(t))
}

func TestAtlasTxtarChecks_MySQLIntegration(t *testing.T) {
	runAtlasTxtarChecksIntegration(t, mysqlAtlasTestURL(t), migrator.RevisionTableFormatAtlas)
}

func TestAtlasTxtarChecks_MySQLSessionLockReleasedIntegration(t *testing.T) {
	runAtlasTxtarChecksMySQLSessionLockIntegration(t, mysqlAtlasTestURL(t))
}

func TestAtlasTxtarChecks_MySQLCommentSemanticsIntegration(t *testing.T) {
	runAtlasTxtarChecksMySQLCommentSemanticsIntegration(
		t,
		mysqlAtlasTestURL(t),
		"/*M! SELECT 0 */ SELECT 1",
	)
}

func TestAtlasTxtarChecks_MySQLVersionGuardBoundaryIntegration(t *testing.T) {
	runAtlasTxtarChecksVersionGuardBoundaryIntegration(t, mysqlAtlasTestURL(t))
}

func TestAtlasTxtarChecks_MySQLExecutableCommentEscapeIntegration(t *testing.T) {
	runAtlasTxtarChecksMySQLExecutableCommentEscapeIntegration(
		t,
		mysqlAtlasTestURL(t)+"?multiStatements=true",
	)
}

func TestAtlasTxtarChecks_MySQLShortNumericPrefixRejectsNonSelectIntegration(t *testing.T) {
	runAtlasTxtarChecksShortNumericPrefixRejectsNonSelectIntegration(t, mysqlAtlasTestURL(t))
}

func TestAtlasTxtarChecks_MariaDBIntegration(t *testing.T) {
	runAtlasTxtarChecksIntegration(t, mariaDBAtlasTestURL(t), migrator.RevisionTableFormatAtlas)
}

func TestAtlasTxtarChecks_MariaDBCommentSemanticsIntegration(t *testing.T) {
	runAtlasTxtarChecksMySQLCommentSemanticsIntegration(
		t,
		mariaDBAtlasTestURL(t),
		"/*!50700 SELECT 0 */ SELECT 1",
	)
}

func TestAtlasTxtarChecks_MariaDBVersionGuardBoundaryIntegration(t *testing.T) {
	runAtlasTxtarChecksVersionGuardBoundaryIntegration(t, mariaDBAtlasTestURL(t))
}

func TestAtlasTxtarChecks_MariaDBShortNumericPrefixRejectsNonSelectIntegration(t *testing.T) {
	runAtlasTxtarChecksShortNumericPrefixRejectsNonSelectIntegration(t, mariaDBAtlasTestURL(t))
}

// ClickHouse is measured on RevisionTableFormatAtlas like every other dialect.
// It used to pass RevisionTableFormatPtah, which is the format ptah-compat never
// selects, so the row stayed green while the Atlas revision path was completely
// broken on this dialect (#950).
func TestAtlasTxtarChecks_ClickHouseIntegration(t *testing.T) {
	runAtlasTxtarChecksIntegration(t, dbtarget.URL(t, dbtarget.ClickHouse), migrator.RevisionTableFormatAtlas)
}

func TestAtlasTxtarChecks_SQLServerIntegration(t *testing.T) {
	runAtlasTxtarChecksIntegration(t, sqlServerTestURL(t), migrator.RevisionTableFormatAtlas)
}

func TestAtlasTxtarChecks_SQLServerSequenceDoesNotAdvanceIntegration(t *testing.T) {
	runAtlasTxtarChecksSQLServerSequenceIntegration(t, sqlServerTestURL(t))
}

func TestAtlasTxtarChecks_CockroachDBIntegration(t *testing.T) {
	runAtlasTxtarChecksIntegration(t, cockroachDBAtlasTestURL(t), migrator.RevisionTableFormatAtlas)
}

func TestAtlasTxtarChecks_YugabyteDBIntegration(t *testing.T) {
	runAtlasTxtarChecksIntegration(t, yugabyteDBAtlasTestURL(t), migrator.RevisionTableFormatAtlas)
}

func TestAtlasRevisionMetadata_MariaDBIntegration(t *testing.T) {
	runAtlasRevisionMetadataIntegration(t, mariaDBAtlasTestURL(t))
}

func TestAtlasRevisionMetadata_SQLServerIntegration(t *testing.T) {
	runAtlasRevisionMetadataIntegration(t, sqlServerTestURL(t))
}

func TestAtlasRevisionMetadata_CockroachDBIntegration(t *testing.T) {
	runAtlasRevisionMetadataIntegration(t, cockroachDBAtlasTestURL(t))
}

func TestAtlasRevisionMetadata_YugabyteDBIntegration(t *testing.T) {
	runAtlasRevisionMetadataIntegration(t, yugabyteDBAtlasTestURL(t))
}

func TestDryRunRevisionState_PostgresIntegration(t *testing.T) {
	runDryRunRevisionStateIntegration(t, postgresTestURL(t), migrator.RevisionTableFormatAtlas)
}

func TestDryRunRevisionState_PostgresIntegration_SearchPath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	rawURL := postgresTestURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, rawURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	_, _ = admin.ExecContext(ctx, "DROP SCHEMA IF EXISTS ptah_issue_937_search_path CASCADE")
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA ptah_issue_937_search_path")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = admin.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS ptah_issue_937_search_path CASCADE")
	}()

	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", "ptah_issue_937_search_path")
	parsed.RawQuery = query.Encode()
	conn, err := dbschema.ConnectToDatabase(ctx, parsed.String())
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	fsys := fstest.MapFS{
		"1_probe.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
	}
	writer, err := migrator.NewFSMigrator(
		conn,
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	writer = writer.WithRevisionTableFormat(migrator.RevisionTableFormatPtah)
	c.Assert(writer.MigrateUp(ctx), qt.IsNil)
	conn.SchemaWriter().SetDryRun(true)
	reader, err := migrator.NewFSMigrator(
		conn,
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	reader = reader.WithRevisionTableFormat(migrator.RevisionTableFormatPtah)

	status, err := reader.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{1})
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
}

func TestDryRunRevisionState_PostgresIntegration_SearchPathIgnoresFallbackMetadata(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	rawURL := postgresTestURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, rawURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	_, _ = admin.ExecContext(ctx, "DROP SCHEMA IF EXISTS ptah_issue_937_current CASCADE")
	_, _ = admin.ExecContext(ctx, "DROP SCHEMA IF EXISTS ptah_issue_937_fallback CASCADE")
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA ptah_issue_937_current")
	c.Assert(err, qt.IsNil)
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA ptah_issue_937_fallback")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = admin.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS ptah_issue_937_current CASCADE")
		_, _ = admin.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS ptah_issue_937_fallback CASCADE")
	}()

	parsedFallback, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	fallbackQuery := parsedFallback.Query()
	fallbackQuery.Set("search_path", "ptah_issue_937_fallback")
	parsedFallback.RawQuery = fallbackQuery.Encode()
	fallbackConn, err := dbschema.ConnectToDatabase(ctx, parsedFallback.String())
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(fallbackConn)
	fSys := fstest.MapFS{
		"1_probe.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
	}
	fallbackWriter, err := migrator.NewFSMigrator(
		fallbackConn,
		fSys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	fallbackWriter = fallbackWriter.WithRevisionTableFormat(migrator.RevisionTableFormatPtah)
	c.Assert(fallbackWriter.MigrateUp(ctx), qt.IsNil)

	parsedCurrent, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	currentQuery := parsedCurrent.Query()
	currentQuery.Set("search_path", "ptah_issue_937_current,ptah_issue_937_fallback")
	parsedCurrent.RawQuery = currentQuery.Encode()
	currentConn, err := dbschema.ConnectToDatabase(ctx, parsedCurrent.String())
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(currentConn)
	currentConn.SchemaWriter().SetDryRun(true)
	reader, err := migrator.NewFSMigrator(
		currentConn,
		fSys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	reader = reader.WithRevisionTableFormat(migrator.RevisionTableFormatPtah)

	status, err := reader.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(0))
	c.Assert(status.AppliedMigrations, qt.HasLen, 0)
	c.Assert(status.PendingMigrations, qt.DeepEquals, []int64{1})
}

func TestDryRunRevisionState_PostgresIntegration_ExplicitSchema(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, postgresTestURL(t))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS ptah_issue_937_explicit CASCADE")
	_, err = conn.ExecContext(ctx, "CREATE SCHEMA ptah_issue_937_explicit")
	c.Assert(err, qt.IsNil)
	defer func() {
		conn.SchemaWriter().SetDryRun(false)
		_, _ = conn.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS ptah_issue_937_explicit CASCADE")
	}()
	fsys := fstest.MapFS{
		"1_probe.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
	}
	writer, err := migrator.NewFSMigrator(
		conn,
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	writer = writer.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithMigrationsTable("ptah_issue_937_explicit", "atlas_schema_revisions")
	c.Assert(writer.MigrateUp(ctx), qt.IsNil)
	conn.SchemaWriter().SetDryRun(true)
	reader, err := migrator.NewFSMigrator(
		conn,
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	reader = reader.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithMigrationsTable("ptah_issue_937_explicit", "atlas_schema_revisions")

	status, err := reader.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{1})
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
}

func TestDryRunRevisionState_MySQLIntegration(t *testing.T) {
	runDryRunRevisionStateIntegration(t, mysqlAtlasTestURL(t), migrator.RevisionTableFormatAtlas)
}

func TestDryRunRevisionState_MariaDBIntegration(t *testing.T) {
	runDryRunRevisionStateIntegration(t, mariaDBAtlasTestURL(t), migrator.RevisionTableFormatAtlas)
}

func TestDryRunRevisionState_SQLServerIntegration(t *testing.T) {
	runDryRunRevisionStateIntegration(t, sqlServerTestURL(t), migrator.RevisionTableFormatAtlas)
}

func TestDryRunRevisionState_CockroachDBIntegration(t *testing.T) {
	runDryRunRevisionStateIntegration(t, cockroachDBAtlasTestURL(t), migrator.RevisionTableFormatAtlas)
}

func TestDryRunRevisionState_YugabyteDBIntegration(t *testing.T) {
	runDryRunRevisionStateIntegration(t, yugabyteDBAtlasTestURL(t), migrator.RevisionTableFormatAtlas)
}

// This is the row #937 added and the one that surfaced #950. It measured
// RevisionTableFormatPtah, so it never exercised the Atlas revision DDL the
// failure lives in; it now measures the same format as every other dialect.
func TestDryRunRevisionState_ClickHouseIntegration(t *testing.T) {
	runDryRunRevisionStateIntegration(t, dbtarget.URL(t, dbtarget.ClickHouse), migrator.RevisionTableFormatAtlas)
}

func runDryRunRevisionStateIntegration(
	t *testing.T,
	dbURL string,
	revisionFormat migrator.RevisionTableFormat,
) {
	t.Helper()

	c := qt.New(t)
	ctx := t.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	cleanupIssue937(t, conn)
	defer cleanupIssue937(t, conn)

	fsys := fstest.MapFS{
		"1_probe.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
	}
	mig, err := migrator.NewFSMigrator(
		conn,
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(revisionFormat).
		WithMigrationsTable("", "ptah_issue_937_revisions")
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)

	conn.SchemaWriter().SetDryRun(true)
	dryRunMigrator, err := migrator.NewFSMigrator(
		conn,
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	dryRunMigrator = dryRunMigrator.WithRevisionTableFormat(revisionFormat).
		WithMigrationsTable("", "ptah_issue_937_revisions")
	status, err := dryRunMigrator.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{1})
	c.Assert(status.PendingMigrations, qt.HasLen, 0)

	conn.SchemaWriter().SetDryRun(false)
}

func TestAtlasSetSerializable_PostgresConcurrentInsertIntegration(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, postgresTestURL(t))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanupIssue819(t, conn)
	defer cleanupIssue819(t, conn)

	fsys := fstest.MapFS{
		"1_create_accounts.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
		"2_create_users.sql":    &fstest.MapFile{Data: []byte("SELECT 2;\n")},
		"3_add_audit.sql":       &fstest.MapFile{Data: []byte("SELECT 3;\n")},
	}
	mig, err := migrator.NewFSMigrator(
		conn,
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	_, err = mig.SetAtlasRevision(ctx, 2)
	c.Assert(err, qt.IsNil)

	_, err = conn.ExecContext(ctx, `CREATE FUNCTION ptah_issue_819_pause_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
	PERFORM pg_advisory_xact_lock(819819);
	RETURN OLD;
END
$$`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `CREATE TRIGGER ptah_issue_819_pause_delete
BEFORE DELETE ON atlas_schema_revisions
FOR EACH ROW EXECUTE FUNCTION ptah_issue_819_pause_delete()`)
	c.Assert(err, qt.IsNil)

	gate, err := conn.Conn(ctx)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = gate.ExecContext(context.Background(), "SELECT pg_advisory_unlock(819819)")
		c.Check(gate.Close(), qt.IsNil)
	}()
	_, err = gate.ExecContext(ctx, "SELECT pg_advisory_lock(819819)")
	c.Assert(err, qt.IsNil)

	setDone := make(chan issue819SetCall, 1)
	go func() {
		result, setErr := mig.SetAtlasRevision(ctx, 1)
		setDone <- issue819SetCall{Result: result, Err: setErr}
	}()
	waitForIssue819AdvisoryWait(c.TB, conn)

	concurrent, err := conn.Conn(ctx)
	c.Assert(err, qt.IsNil)
	defer func() {
		c.Check(concurrent.Close(), qt.IsNil)
	}()
	tx, err := concurrent.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	c.Assert(err, qt.IsNil)
	defer func() {
		_ = tx.Rollback()
	}()
	var description string
	err = tx.QueryRowContext(
		ctx,
		"SELECT description FROM atlas_schema_revisions WHERE version = '2'",
	).Scan(&description)
	c.Assert(err, qt.IsNil)
	c.Assert(description, qt.Equals, "create_users")
	_, err = tx.ExecContext(ctx, `INSERT INTO atlas_schema_revisions
(version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
VALUES ('3', 'external', 2, 1, 1, NOW(), 0, NULL, NULL, 'external', 'null'::jsonb, 'Atlas')`)
	c.Assert(err, qt.IsNil)
	c.Assert(tx.Commit(), qt.IsNil)

	_, err = gate.ExecContext(ctx, "SELECT pg_advisory_unlock(819819)")
	c.Assert(err, qt.IsNil)
	setCall := <-setDone
	c.Assert(setCall.Err, qt.IsNil)
	c.Assert(issue819RevisionVersions(setCall.Result.Removed), qt.DeepEquals, []int64{2, 3})
	c.Assert(readIssue819Revisions(c.TB, conn), qt.DeepEquals, []issue819Revision{
		{
			Version:           "1",
			Description:       "create_accounts",
			RevisionType:      6,
			ExecutedAtNonZero: true,
			Error:             sql.NullString{Valid: true},
			ErrorStatement:    sql.NullString{Valid: true},
			Hash:              issue819SQLHash("SELECT 1;\n"),
			PartialHashes:     sql.NullString{String: "null", Valid: true},
			OperatorVersion:   "Ptah",
		},
	})
}

// TestAtlasRevisionMetadata_ClickHouseIntegration is the apply-and-read subset of
// runAtlasRevisionMetadataIntegration. The full helper cannot be reused because it
// calls SetAtlasRevision, which ClickHouse refuses by design (see
// TestAtlasRevisionMetadata_ClickHouseRejectsSetIntegration).
//
// It pins both halves of #950 and fails for a different reason on each server
// version. On ClickHouse 24.x, `partial_hashes JSON NULL` is read as
// Nullable(JSON) and rejected during type analysis, so MigrateUp never gets past
// creating the revision table (code: 43, "Nested type JSON cannot be inside
// Nullable type"). On 25.x/26.x the same DDL is accepted, the Atlas JSON null is
// coerced into the JSON type and stored as `{}`, and the column can no longer be
// scanned into a string at all ("unsupported Scan, storing driver.Value type
// *chcol.JSON into type *string"). Asserting the whole row -- including
// PartialHashes, which Ptah itself never selects back -- is what makes the second
// half visible; an exit-code-only check passes on 26.x against the broken DDL.
func TestAtlasRevisionMetadata_ClickHouseIntegration(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(t, dbtarget.ClickHouse))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanupIssue819(t, conn)
	defer cleanupIssue819(t, conn)

	const firstSQL = "SELECT 1;\n"
	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_create_accounts.sql": &fstest.MapFile{Data: []byte(firstSQL)},
			"2_create_users.sql":    &fstest.MapFile{Data: []byte("SELECT 2;\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	err = mig.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{Amount: 1})
	c.Assert(err, qt.IsNil)

	c.Assert(readIssue819Revisions(c.TB, conn), qt.DeepEquals, []issue819Revision{
		{
			Version:               "1",
			Description:           "create_accounts",
			RevisionType:          2,
			Applied:               1,
			Total:                 1,
			ExecutedAtNonZero:     true,
			ExecutionTimePositive: true,
			Error:                 sql.NullString{Valid: true},
			ErrorStatement:        sql.NullString{Valid: true},
			Hash:                  issue819SQLHash(firstSQL),
			PartialHashes:         sql.NullString{String: "null", Valid: true},
			OperatorVersion:       "Ptah",
		},
	})

	version, err := mig.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(1))
}

func TestAtlasRevisionMetadata_ClickHouseRejectsSetIntegration(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbtarget.URL(t, dbtarget.ClickHouse))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_create_accounts.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	result, err := mig.SetAtlasRevision(t.Context(), 1)

	c.Assert(err, qt.ErrorMatches, "setting an Atlas revision is not supported for ClickHouse because revision history cannot be updated atomically")
	c.Assert(result, qt.DeepEquals, migrator.AtlasRevisionSetResult{})
}

type issue819SetCall struct {
	Result migrator.AtlasRevisionSetResult
	Err    error
}

func waitForIssue819AdvisoryWait(tb testing.TB, conn *dbschema.DatabaseConnection) {
	c := qt.New(tb)
	c.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		err := conn.QueryRowContext(
			context.Background(),
			`SELECT COUNT(*) FROM pg_locks
WHERE locktype = 'advisory' AND objid = 819819 AND NOT granted`,
		).Scan(&waiting)
		c.Assert(err, qt.IsNil)
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.Fatalf("timed out waiting for migrate set delete trigger")
}

func runAtlasFormatIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	cleanupIssue273(t, conn)
	defer cleanupIssue273(t, conn)
	createLegacyIssue273MetadataTable(t, conn)

	fsys := fstest.MapFS{
		"20220318104614_team_A.sql": &fstest.MapFile{Data: []byte("CREATE TABLE ptah_issue_273_teams (id INT PRIMARY KEY);\n")},
		"20220318104615_add_users.sql": &fstest.MapFile{Data: []byte(
			"CREATE TABLE ptah_issue_273_users (id INT PRIMARY KEY, team_id INT);\n",
		)},
		"20220318104616.sql": &fstest.MapFile{Data: []byte(
			"CREATE TABLE ptah_issue_273_audit (id INT PRIMARY KEY);\n",
		)},
	}
	mig, err := migrator.NewFSMigrator(conn, fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
	c.Assert(err, qt.IsNil)
	mig = mig.WithMigrationsTable("", "schema_migrations_issue_273")

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.TotalMigrations, qt.Equals, 3)
	c.Assert(status.PendingMigrations, qt.DeepEquals, []int64{20220318104614, 20220318104615, 20220318104616})

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(issue273UsersCount(t, conn), qt.Equals, 0)
	c.Assert(issue273Versions(t, conn), qt.DeepEquals, []int64{20220318104614, 20220318104615, 20220318104616})

	status, err = mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
	c.Assert(status.HasPendingChanges, qt.IsFalse)

	err = mig.MigrateDownTo(ctx, 20220318104615)
	c.Assert(err, qt.ErrorMatches, `.*migration 20220318104616 has no Atlas down migration; dynamic Atlas-style down migrations are not implemented yet.*`)
	var noDown *migrator.AtlasDownNotImplementedError
	c.Assert(err, qt.ErrorAs, &noDown)
	c.Assert(noDown.Version, qt.Equals, int64(20220318104616))
}

func runAtlasExactRevisionIdentityCollationIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanup := func() {
		for _, statement := range []string{
			"DROP TABLE IF EXISTS ptah_issue_1206_lower",
			"DROP TABLE IF EXISTS ptah_issue_1206_upper",
			"DROP TABLE IF EXISTS atlas_schema_revisions",
		} {
			_, _ = conn.ExecContext(context.Background(), statement)
		}
	}
	cleanup()
	defer cleanup()

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"10_upper.sql": &fstest.MapFile{Data: []byte("CREATE TABLE ptah_issue_1206_upper (id BIGINT PRIMARY KEY);\n")},
			"20_lower.sql": &fstest.MapFile{Data: []byte("CREATE TABLE ptah_issue_1206_lower (id BIGINT PRIMARY KEY);\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{10: "A", 20: "a"}),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.MigrateUp(t.Context()), qt.IsNil)

	rows, err := conn.QueryContext(t.Context(), "SELECT version FROM atlas_schema_revisions")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var versions []string
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	slices.Sort(versions)
	c.Assert(versions, qt.DeepEquals, []string{"A", "a"})
}

func runAtlasRevisionIdentityCollationRefusalIntegration(t *testing.T, dbURL, collation string) {
	t.Helper()

	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanup := func() {
		for _, statement := range []string{
			"DROP TABLE IF EXISTS ptah_issue_1206_lower",
			"DROP TABLE IF EXISTS ptah_issue_1206_upper",
			"DROP TABLE IF EXISTS atlas_schema_revisions",
		} {
			_, _ = conn.ExecContext(context.Background(), statement)
		}
	}
	cleanup()
	defer cleanup()

	_, err = conn.ExecContext(t.Context(), fmt.Sprintf(`CREATE TABLE atlas_schema_revisions (
    version VARCHAR(255) COLLATE %s PRIMARY KEY,
    description TEXT NOT NULL,
    type BIGINT NOT NULL DEFAULT 2,
    applied BIGINT NOT NULL DEFAULT 0,
    total BIGINT NOT NULL DEFAULT 0,
    executed_at TIMESTAMP NOT NULL,
    execution_time BIGINT NOT NULL,
    error TEXT NULL,
    error_stmt TEXT NULL,
    hash VARCHAR(255) NOT NULL,
    partial_hashes JSON NULL,
    operator_version VARCHAR(255) NOT NULL
) ENGINE=InnoDB`, collation))
	c.Assert(err, qt.IsNil)

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"10_upper.sql": &fstest.MapFile{Data: []byte("CREATE TABLE ptah_issue_1206_upper (id BIGINT PRIMARY KEY);\n")},
			"20_lower.sql": &fstest.MapFile{Data: []byte("CREATE TABLE ptah_issue_1206_lower (id BIGINT PRIMARY KEY);\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{10: "A", 20: "a"}),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.MigrateUp(t.Context()), qt.ErrorMatches,
		`failed to initialize migrations table: revision table cannot distinguish every exact Atlas identity under its configured version collation: 2 identities collapse to 1`)

	var bodyTables int
	c.Assert(conn.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name IN ('ptah_issue_1206_upper', 'ptah_issue_1206_lower')`).Scan(&bodyTables), qt.IsNil)
	c.Assert(bodyTables, qt.Equals, 0)
}

func runAtlasRevisionIdentityCollationSQLServerRefusalIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanup := func() {
		for _, statement := range []string{
			"DROP TABLE IF EXISTS ptah_issue_1206_lower",
			"DROP TABLE IF EXISTS ptah_issue_1206_upper",
			"DROP TABLE IF EXISTS atlas_schema_revisions",
		} {
			_, _ = conn.ExecContext(context.Background(), statement)
		}
	}
	cleanup()
	defer cleanup()

	_, err = conn.ExecContext(t.Context(), `CREATE TABLE atlas_schema_revisions (
    version NVARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS PRIMARY KEY,
    description NVARCHAR(MAX) NOT NULL,
    type BIGINT NOT NULL DEFAULT 2,
    applied BIGINT NOT NULL DEFAULT 0,
    total BIGINT NOT NULL DEFAULT 0,
    executed_at DATETIME2 NOT NULL,
    execution_time BIGINT NOT NULL,
    error NVARCHAR(MAX) NULL,
    error_stmt NVARCHAR(MAX) NULL,
    hash NVARCHAR(255) NOT NULL,
    partial_hashes NVARCHAR(MAX) NULL,
    operator_version NVARCHAR(255) NOT NULL
)`)
	c.Assert(err, qt.IsNil)

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"10_upper.sql": &fstest.MapFile{Data: []byte("CREATE TABLE ptah_issue_1206_upper (id BIGINT PRIMARY KEY);\n")},
			"20_lower.sql": &fstest.MapFile{Data: []byte("CREATE TABLE ptah_issue_1206_lower (id BIGINT PRIMARY KEY);\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{10: "A", 20: "a"}),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.MigrateUp(t.Context()), qt.ErrorMatches,
		`failed to initialize migrations table: revision table cannot distinguish every exact Atlas identity under its configured version collation: 2 identities collapse to 1`)

	var bodyTables int
	c.Assert(conn.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME IN ('ptah_issue_1206_upper', 'ptah_issue_1206_lower')`).Scan(&bodyTables), qt.IsNil)
	c.Assert(bodyTables, qt.Equals, 0)
}

func runAtlasTxtarDownIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	cleanupIssue290(t, conn)
	defer cleanupIssue290(t, conn)

	fsys := fstest.MapFS{
		"20240305171146_seed_widget.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- migration.sql --
CREATE TABLE ptah_issue_290_widgets (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL);
INSERT INTO ptah_issue_290_widgets (id, name) VALUES (1, 'Alice');

-- down.sql --
DROP TABLE ptah_issue_290_widgets;
`)},
	}
	mig, err := migrator.NewFSMigrator(conn, fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
	c.Assert(err, qt.IsNil)
	mig = mig.WithMigrationsTable("", "schema_migrations_issue_290")

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(issue290WidgetsCount(t, conn), qt.Equals, 1)
	c.Assert(issue290Versions(t, conn), qt.DeepEquals, []int64{20240305171146})

	err = mig.MigrateDownTo(ctx, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(issue290Versions(t, conn), qt.HasLen, 0)
	c.Assert(issue290WidgetTableExists(t, conn), qt.IsFalse)
}

func runAtlasTemplateIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	cleanupIssue299(t, conn)
	defer cleanupIssue299(t, conn)

	fsys := fstest.MapFS{
		"1.sql": &fstest.MapFile{Data: []byte(`{{- if eq .Env "dev" }}
CREATE TABLE ptah_issue_299_dev (id INT PRIMARY KEY);
{{- else }}
CREATE TABLE ptah_issue_299_prod (id INT PRIMARY KEY);
{{- end }}
`)},
		"2.sql": &fstest.MapFile{Data: []byte(`{{ template "shared/users" "dev" }}`)},
		"shared/users.sql": &fstest.MapFile{Data: []byte(`{{- define "shared/users" }}
CREATE TABLE ptah_issue_299_users_{{ $ }} (id INT PRIMARY KEY);
{{- end }}
`)},
	}
	mig, err := migrator.NewFSMigrator(
		conn,
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: "dev"}),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithMigrationsTable("", "schema_migrations_issue_299")

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(tableExists(t, conn, "ptah_issue_299_dev"), qt.IsTrue)
	c.Assert(tableExists(t, conn, "ptah_issue_299_prod"), qt.IsFalse)
	c.Assert(tableExists(t, conn, "ptah_issue_299_users_dev"), qt.IsTrue)
	c.Assert(issue299Versions(t, conn), qt.DeepEquals, []int64{1, 2})
}

func runAtlasRevisionTableIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	cleanupIssue275(t, conn)
	defer cleanupIssue275(t, conn)

	const firstVersion = int64(20240101120000)
	const secondVersion = int64(20240101120100)
	seedFS := fstest.MapFS{
		"atlas.sum": &fstest.MapFile{Data: []byte(
			"h1:directory\n" +
				"20240101120000_seed.sql h1:seedhash\n",
		)},
		"20240101120000_seed.sql": &fstest.MapFile{Data: []byte("CREATE TABLE ptah_issue_275_seed (id INT PRIMARY KEY);\n")},
	}
	seedMigrator, err := migrator.NewFSMigrator(
		conn,
		seedFS,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	seedMigrator = seedMigrator.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(seedMigrator.Initialize(ctx), qt.IsNil)
	_, err = conn.ExecContext(ctx, "CREATE TABLE ptah_issue_275_seed (id INT PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	insertRevision := sqlutil.Rebind(conn.Info().Dialect, `INSERT INTO atlas_schema_revisions
(version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, NULL, ?)`)
	_, err = conn.ExecContext(ctx, insertRevision,
		"20240101120000",
		"Seed",
		2,
		1,
		1,
		time.Now(),
		int64(100),
		"seedhash",
		"Atlas",
	)
	c.Assert(err, qt.IsNil)

	status, err := seedMigrator.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, firstVersion)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{firstVersion})
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
	c.Assert(status.HasPendingChanges, qt.IsFalse)

	nextFS := fstest.MapFS{
		"atlas.sum": &fstest.MapFile{Data: []byte(
			"h1:directory\n" +
				"20240101120000_seed.sql h1:seedhash\n" +
				"20240101120100_next.sql h1:nexthash\n",
		)},
		"20240101120000_seed.sql": &fstest.MapFile{Data: []byte("CREATE TABLE ptah_issue_275_seed (id INT PRIMARY KEY);\n")},
		"20240101120100_next.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- migration.sql --
CREATE TABLE ptah_issue_275_next (id INT PRIMARY KEY);

-- down.sql --
DROP TABLE ptah_issue_275_next;
`)},
	}
	nextMigrator, err := migrator.NewFSMigrator(
		conn,
		nextFS,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	nextMigrator = nextMigrator.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	err = nextMigrator.MigrateUp(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(tableExists(t, conn, "ptah_issue_275_next"), qt.IsTrue)
	c.Assert(issue275Revisions(t, conn), qt.DeepEquals, []issue275Revision{
		{Version: strconv.FormatInt(firstVersion, 10), Description: "Seed", RevisionType: 2, Applied: 1, Total: 1, Hash: "seedhash", OperatorVersion: "Atlas"},
		{Version: strconv.FormatInt(secondVersion, 10), Description: "next", RevisionType: 2, Applied: 1, Total: 1, Hash: "nexthash", OperatorVersion: "Ptah"},
	})

	err = nextMigrator.MigrateDownTo(ctx, firstVersion)
	c.Assert(err, qt.IsNil)
	c.Assert(tableExists(t, conn, "ptah_issue_275_next"), qt.IsFalse)
	c.Assert(issue275Revisions(t, conn), qt.DeepEquals, []issue275Revision{
		{Version: strconv.FormatInt(firstVersion, 10), Description: "Seed", RevisionType: 2, Applied: 1, Total: 1, Hash: "seedhash", OperatorVersion: "Atlas"},
	})
}

func runAtlasRevisionMetadataIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	cleanupIssue819(t, conn)
	defer cleanupIssue819(t, conn)

	const (
		firstSQL  = "SELECT 1;\n"
		secondSQL = "SELECT 2;\n"
		thirdSQL  = "SELECT 3;\n"
	)
	fsys := fstest.MapFS{
		"1_create_accounts.sql": &fstest.MapFile{Data: []byte(firstSQL)},
		"2_create_users.sql":    &fstest.MapFile{Data: []byte(secondSQL)},
		"3_add_audit.sql":       &fstest.MapFile{Data: []byte(thirdSQL)},
	}
	mig, err := migrator.NewFSMigrator(
		conn,
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	err = mig.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{Amount: 1})
	c.Assert(err, qt.IsNil)
	c.Assert(readIssue819Revisions(c.TB, conn), qt.DeepEquals, []issue819Revision{
		{
			Version:               "1",
			Description:           "create_accounts",
			RevisionType:          2,
			Applied:               1,
			Total:                 1,
			ExecutedAtNonZero:     true,
			ExecutionTimePositive: true,
			Error:                 sql.NullString{Valid: true},
			ErrorStatement:        sql.NullString{Valid: true},
			Hash:                  issue819SQLHash(firstSQL),
			PartialHashes:         sql.NullString{String: "null", Valid: true},
			OperatorVersion:       "Ptah",
		},
	})

	result, err := mig.SetAtlasRevision(ctx, 3)
	c.Assert(err, qt.IsNil)
	c.Assert(issue819RevisionVersions(result.Set), qt.DeepEquals, []int64{2, 3})
	c.Assert(result.Removed, qt.HasLen, 0)
	c.Assert(readIssue819Revisions(c.TB, conn), qt.DeepEquals, []issue819Revision{
		{
			Version:               "1",
			Description:           "create_accounts",
			RevisionType:          2,
			Applied:               1,
			Total:                 1,
			ExecutedAtNonZero:     true,
			ExecutionTimePositive: true,
			Error:                 sql.NullString{Valid: true},
			ErrorStatement:        sql.NullString{Valid: true},
			Hash:                  issue819SQLHash(firstSQL),
			PartialHashes:         sql.NullString{String: "null", Valid: true},
			OperatorVersion:       "Ptah",
		},
		{
			Version:           "2",
			Description:       "create_users",
			RevisionType:      6,
			ExecutedAtNonZero: true,
			Error:             sql.NullString{Valid: true},
			ErrorStatement:    sql.NullString{Valid: true},
			Hash:              issue819SQLHash(secondSQL),
			PartialHashes:     sql.NullString{String: "null", Valid: true},
			OperatorVersion:   "Ptah",
		},
		{
			Version:           "3",
			Description:       "add_audit",
			RevisionType:      6,
			ExecutedAtNonZero: true,
			Error:             sql.NullString{Valid: true},
			ErrorStatement:    sql.NullString{Valid: true},
			Hash:              issue819SQLHash(thirdSQL),
			PartialHashes:     sql.NullString{String: "null", Valid: true},
			OperatorVersion:   "Ptah",
		},
	})

	markIssue819RevisionDirty(c.TB, conn, "1")
	result, err = mig.SetAtlasRevision(ctx, 2)
	c.Assert(err, qt.IsNil)
	c.Assert(issue819RevisionVersions(result.Set), qt.DeepEquals, []int64{1})
	c.Assert(issue819RevisionVersions(result.Removed), qt.DeepEquals, []int64{3})
	c.Assert(readIssue819Revisions(c.TB, conn), qt.DeepEquals, []issue819Revision{
		{
			Version:               "1",
			Description:           "create_accounts",
			RevisionType:          6,
			Total:                 1,
			ExecutedAtNonZero:     true,
			ExecutionTimePositive: true,
			Error:                 sql.NullString{String: "broken", Valid: true},
			ErrorStatement:        sql.NullString{Valid: true},
			Hash:                  issue819SQLHash(firstSQL),
			PartialHashes:         sql.NullString{String: "null", Valid: true},
			OperatorVersion:       "Ptah",
		},
		{
			Version:           "2",
			Description:       "create_users",
			RevisionType:      6,
			ExecutedAtNonZero: true,
			Error:             sql.NullString{Valid: true},
			ErrorStatement:    sql.NullString{Valid: true},
			Hash:              issue819SQLHash(secondSQL),
			PartialHashes:     sql.NullString{String: "null", Valid: true},
			OperatorVersion:   "Ptah",
		},
	})

	cleanupIssue819(t, conn)
	baselineMigrator, err := migrator.NewFSMigrator(
		conn,
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	baselineMigrator = baselineMigrator.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	err = baselineMigrator.Baseline(ctx, 2)
	c.Assert(err, qt.IsNil)
	c.Assert(readIssue819Revisions(c.TB, conn), qt.DeepEquals, []issue819Revision{
		{
			Version:           "2",
			Description:       "create_users",
			RevisionType:      1,
			ExecutedAtNonZero: true,
			Error:             sql.NullString{Valid: true},
			ErrorStatement:    sql.NullString{Valid: true},
			PartialHashes:     sql.NullString{String: "null", Valid: true},
			OperatorVersion:   "Ptah",
		},
	})
}

func runAtlasDotMetadataIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanupIssue819(t, conn)
	defer cleanupIssue819(t, conn)

	fsys := fstest.MapFS{
		"1_create_accounts.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
		"2_create_users.sql":    &fstest.MapFile{Data: []byte("SELECT 2;\n")},
	}
	mig, err := migrator.NewFSMigrator(
		conn,
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{Amount: 1}), qt.IsNil)

	insertMetadata := sqlutil.Rebind(conn.Info().Dialect, `INSERT INTO atlas_schema_revisions
(version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, NULL, ?)`)
	_, err = conn.ExecContext(
		ctx,
		insertMetadata,
		".atlas_cloud_identifier",
		"metadata-id",
		2,
		0,
		0,
		time.Now(),
		int64(0),
		"",
		"Atlas CLI",
	)
	c.Assert(err, qt.IsNil)

	current, err := mig.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(current, qt.Equals, int64(1))
	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
	c.Assert(status.PendingMigrations, qt.DeepEquals, []int64{2})

	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	current, err = mig.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(current, qt.Equals, int64(2))

	result, err := mig.SetAtlasRevision(ctx, 1)
	c.Assert(err, qt.IsNil)
	c.Assert(issue819RevisionVersions(result.Removed), qt.DeepEquals, []int64{2})
	current, err = mig.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(current, qt.Equals, int64(1))

	var (
		description    string
		revisionType   int
		applied        int
		total          int
		errorValue     sql.NullString
		errorStatement sql.NullString
		hash           string
		partialHashes  sql.NullString
		operator       string
	)
	err = conn.QueryRowContext(
		ctx,
		`SELECT description, type, applied, total, error, error_stmt, hash, partial_hashes, operator_version
FROM atlas_schema_revisions WHERE version = '.atlas_cloud_identifier'`,
	).Scan(
		&description,
		&revisionType,
		&applied,
		&total,
		&errorValue,
		&errorStatement,
		&hash,
		&partialHashes,
		&operator,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(description, qt.Equals, "metadata-id")
	c.Assert(revisionType, qt.Equals, 2)
	c.Assert(applied, qt.Equals, 0)
	c.Assert(total, qt.Equals, 0)
	c.Assert(errorValue.Valid, qt.IsFalse)
	c.Assert(errorStatement.Valid, qt.IsFalse)
	c.Assert(hash, qt.Equals, "")
	c.Assert(partialHashes.Valid, qt.IsFalse)
	c.Assert(operator, qt.Equals, "Atlas CLI")
}

func runAtlasTxtarChecksIntegration(t *testing.T, dbURL string, revisionFormat migrator.RevisionTableFormat) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanupIssue819(t, conn)
	defer cleanupIssue819(t, conn)

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_checked.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/base.sql --
SELECT 1;

-- checks/alternatives.sql --
-- atlas:assert oneof
SELECT 0;
SELECT 1;

-- migration.sql --
SELECT 1;
`)},
			"2_invalid_cardinality.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/cardinality.sql --
SELECT 1 UNION ALL SELECT 1;

-- migration.sql --
SELECT * FROM ptah_check_body_must_not_run;
`)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(revisionFormat)

	c.Assert(mig.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{TargetVersion: 1}), qt.IsNil)
	current, err := mig.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(current, qt.Equals, int64(1))

	c.Assert(mig.MigrateUp(ctx), qt.ErrorMatches, `.*check assertion must return exactly one row, got more than 1.*`)
	current, err = mig.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(current, qt.Equals, int64(1))
}

func runAtlasTxtarChecksPostgresSessionLockIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanupIssue819(t, conn)
	defer cleanupIssue819(t, conn)
	const lockID = int64(964227)
	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_session_lock.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/session.sql --
SELECT pg_try_advisory_lock(964227);

-- migration.sql --
SELECT 1;
`)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)

	var acquired bool
	err = conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired)
	c.Assert(err, qt.IsNil)
	c.Assert(acquired, qt.IsTrue)
	var released bool
	err = conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", lockID).Scan(&released)
	c.Assert(err, qt.IsNil)
	c.Assert(released, qt.IsTrue)
}

func runAtlasTxtarChecksPostgresEscapeStringIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanupIssue819(t, conn)
	defer cleanupIssue819(t, conn)
	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_escape_string.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/escape.sql --
SELECT E'it\'s; one literal' = E'it\'s; one literal';

-- migration.sql --
SELECT 1;
`)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
}

func runAtlasTxtarChecksMySQLCommentSemanticsIntegration(
	t *testing.T,
	dbURL string,
	ignoredCommentAssertion string,
) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanupIssue819(t, conn)
	defer cleanupIssue819(t, conn)

	// The inert guard is derived from the connected server, never written as a
	// literal. This fixture used to hard-code /*!99999 ...*/ as "a version no
	// server will reach"; #791 moved the matrix from mysql:9.7 to mysql:26.7,
	// whose version id is 260700, so the guard opened and the DELETE in the
	// comment became live SQL that Ptah then correctly refused. A derived guard
	// cannot rot that way, and measureExecutableCommentGuard proves it is inert
	// on this exact server before the fixture leans on it.
	inertGuard := strconv.Itoa(inertExecutableCommentGuard(c.TB, ctx, conn))

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_whole_executable_comment.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/executable.sql --
/*! SELECT 1 */;

-- migration.sql --
SELECT 1;
`)},
			"2_ignored_dialect_comment.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/ignored.sql --
` + ignoredCommentAssertion + `;

-- migration.sql --
SELECT 1;
`)},
			"3_inert_version_guard.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/future.sql --
/*!` + inertGuard + ` DELETE FROM users */ SELECT 1;

-- migration.sql --
SELECT 1;
`)},
			"4_executable_expression.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/executable.sql --
SELECT 0 /*! + 1 */;

-- migration.sql --
SELECT 1;
`)},
			"5_short_numeric_prefix.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/numeric.sql --
SELECT /*!1234 + 1 */ = 1235;

-- migration.sql --
SELECT 1;
`)},
			"6_dash_dash_arithmetic.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/arithmetic.sql --
SELECT -1--1;

-- migration.sql --
SELECT 1;
`)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	c.Assert(mig.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{TargetVersion: 5}), qt.IsNil)
	c.Assert(mig.MigrateUp(ctx), qt.ErrorMatches, `.*checks/arithmetic.sql#1.*was not satisfied.*`)
	current, err := mig.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(current, qt.Equals, int64(5))
}

// serverExecutableCommentVersionID encodes a version banner the way MySQL and
// MariaDB encode their own version when they compare it against an executable
// comment's numeric guard: major*10000 + minor*100 + patch. MariaDB's 5.5.5-
// replication prefix is stripped first, and any build suffix ("-log",
// "-MariaDB-ubu2204") is discarded.
//
// It is deliberately written differently from the migrator's own encoder — that
// one scans digits, this one splits fields — so the two are a cross-check
// rather than a copy. Neither is trusted on its own either: every caller proves
// the number is exactly the connected server's version id by probing the server
// on both sides of it before a fixture depends on it.
func serverExecutableCommentVersionID(banner string) (int, bool) {
	numeric := banner
	if strings.Contains(strings.ToLower(numeric), "mariadb") {
		numeric = strings.TrimPrefix(numeric, "5.5.5-")
	}
	numeric, _, _ = strings.Cut(numeric, "-")
	parts := strings.Split(numeric, ".")
	if len(parts) != 3 {
		return 0, false
	}
	scale := [3]int{10000, 100, 1}
	total := 0
	for i, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil {
			return 0, false
		}
		total += value * scale[i]
	}
	return total, true
}

// measureExecutableCommentGuard asks the connected server what it does with a
// numeric executable-comment guard, rather than assuming. `SELECT 1 /*!N + 1 */`
// evaluates to 2 when the server honors guard N and 1 when it ignores it, so the
// return value reports the guard's effect in the only authority that matters.
func measureExecutableCommentGuard(tb testing.TB, ctx context.Context, conn *dbschema.DatabaseConnection, guard int) int {
	c := qt.New(tb)
	c.Helper()

	query := "SELECT 1 /*!" + strconv.Itoa(guard) + " + 1 */"
	var got int
	c.Assert(conn.QueryRowContext(ctx, query).Scan(&got), qt.IsNil, qt.Commentf("query %q", query))
	return got
}

// inertExecutableCommentGuard returns the smallest numeric guard the connected
// server ignores, and proves it is exactly that before returning.
//
// The proof is the pair of probes, and it is what makes the number trustworthy
// on a server version nobody has run yet. If serverExecutableCommentVersionID
// under-reports, the guard it calls inert is still honored and the second probe
// reads 2; if it over-reports, the guard one below is already ignored and the
// first probe reads 1. Only the true version id passes both, so no fixture can
// quietly inherit a guard that has stopped being inert — the failure mode #791
// exposed.
func inertExecutableCommentGuard(tb testing.TB, ctx context.Context, conn *dbschema.DatabaseConnection) int {
	c := qt.New(tb)
	c.Helper()

	banner := conn.Info().Version
	serverID, ok := serverExecutableCommentVersionID(banner)
	c.Assert(ok, qt.IsTrue, qt.Commentf("server version banner %q", banner))

	c.Assert(measureExecutableCommentGuard(c.TB, ctx, conn, serverID), qt.Equals, 2,
		qt.Commentf("server %q must honor a guard at its own version id %d", banner, serverID))
	c.Assert(measureExecutableCommentGuard(c.TB, ctx, conn, serverID+1), qt.Equals, 1,
		qt.Commentf("server %q must ignore a guard one above its version id %d", banner, serverID))

	return serverID + 1
}

// runAtlasTxtarChecksVersionGuardBoundaryIntegration pins Ptah's
// executable-comment version arithmetic to the connected server's arithmetic at
// the exact boundary between the two, on whatever version the matrix runs.
//
// The hard-coded /*!99999 ...*/ fixture only ever exercised the inert side of
// the boundary, and it did so by accident: 99999 happened to sit above every
// MySQL that existed when it was written. Here both sides are exercised
// deliberately and the server is measured first, so a release that changes the
// encoding — or a container bump that walks past a literal, as mysql:9.7 ->
// mysql:26.7 did — fails on the probe with the real numbers in the message
// instead of surfacing as a confusing refusal deep inside an unrelated
// assertion.
//
// The witness table carries rows, so the "live guard" migration also proves the
// refusal happens before the body reaches the server: had Ptah let it through,
// the server honors that guard by construction and the rows would be gone.
func runAtlasTxtarChecksVersionGuardBoundaryIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanupIssue819(t, conn)
	defer cleanupIssue819(t, conn)

	inertGuard := inertExecutableCommentGuard(c.TB, ctx, conn)
	liveGuard := inertGuard - 1

	_, err = conn.ExecContext(ctx, "DROP TABLE IF EXISTS ptah_check_version_guard")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, dropErr := conn.ExecContext(ctx, "DROP TABLE IF EXISTS ptah_check_version_guard")
		c.Check(dropErr, qt.IsNil)
	}()
	_, err = conn.ExecContext(ctx, "CREATE TABLE ptah_check_version_guard (id INT)")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, "INSERT INTO ptah_check_version_guard VALUES (1), (2), (3)")
	c.Assert(err, qt.IsNil)

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_inert_guard.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/inert.sql --
/*!` + strconv.Itoa(inertGuard) + ` DELETE FROM ptah_check_version_guard */ SELECT 1;

-- migration.sql --
SELECT 1;
`)},
			"2_live_guard.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/live.sql --
/*!` + strconv.Itoa(liveGuard) + ` DELETE FROM ptah_check_version_guard */ SELECT 1;

-- migration.sql --
SELECT 1;
`)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	c.Assert(mig.MigrateUp(ctx), qt.ErrorMatches, `.*checks/live.sql#1.*must be a read-only SELECT statement.*`)

	current, err := mig.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(current, qt.Equals, int64(1))

	var remaining int
	err = conn.QueryRowContext(ctx, "SELECT count(*) FROM ptah_check_version_guard").Scan(&remaining)
	c.Assert(err, qt.IsNil)
	c.Assert(remaining, qt.Equals, 3)
}

func runAtlasTxtarChecksMySQLExecutableCommentEscapeIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanupIssue819(t, conn)
	defer cleanupIssue819(t, conn)
	_, err = conn.ExecContext(ctx, "DROP TABLE IF EXISTS ptah_check_comment_escape")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, dropErr := conn.ExecContext(ctx, "DROP TABLE IF EXISTS ptah_check_comment_escape")
		c.Check(dropErr, qt.IsNil)
	}()

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_hidden_statements.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/escape.sql --
/*! SELECT 1; COMMIT; CREATE TABLE ptah_check_comment_escape (id INT) */;

-- migration.sql --
SELECT 1;
`)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(
		mig.MigrateUp(ctx),
		qt.ErrorMatches,
		`.*check assertion must be one read-only SELECT statement, got 3 statements.*`,
	)

	var created int
	err = conn.QueryRowContext(
		ctx,
		"SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'ptah_check_comment_escape'",
	).Scan(&created)
	c.Assert(err, qt.IsNil)
	c.Assert(created, qt.Equals, 0)
}

func runAtlasTxtarChecksShortNumericPrefixRejectsNonSelectIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanupIssue819(t, conn)
	defer cleanupIssue819(t, conn)
	_, err = conn.ExecContext(ctx, "DROP TABLE IF EXISTS ptah_check_numeric_body")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, dropErr := conn.ExecContext(ctx, "DROP TABLE IF EXISTS ptah_check_numeric_body")
		c.Check(dropErr, qt.IsNil)
	}()

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_numeric_body.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/numeric.sql --
/*!1234 SELECT 1 */;

-- migration.sql --
CREATE TABLE ptah_check_numeric_body (id INT);
`)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(
		mig.MigrateUp(ctx),
		qt.ErrorMatches,
		`.*check assertion must be a read-only SELECT statement.*`,
	)

	var created int
	err = conn.QueryRowContext(
		ctx,
		"SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'ptah_check_numeric_body'",
	).Scan(&created)
	c.Assert(err, qt.IsNil)
	c.Assert(created, qt.Equals, 0)
}

func runAtlasTxtarChecksMySQLSessionLockIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanupIssue819(t, conn)
	defer cleanupIssue819(t, conn)
	const lockName = "ptah_check_lock_964227"
	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_session_lock.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks/session.sql --
SELECT GET_LOCK('ptah_check_lock_964227', 0);

-- migration.sql --
SELECT 1;
`)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)

	var free int
	err = conn.QueryRowContext(ctx, "SELECT IS_FREE_LOCK(?)", lockName).Scan(&free)
	c.Assert(err, qt.IsNil)
	c.Assert(free, qt.Equals, 1)
}

func runAtlasTxtarChecksSQLServerSequenceIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	cleanupIssue819(t, conn)
	defer cleanupIssue819(t, conn)
	_, err = conn.ExecContext(ctx, "DROP SEQUENCE IF EXISTS dbo.ptah_check_sequence")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, dropErr := conn.ExecContext(ctx, "DROP SEQUENCE IF EXISTS dbo.ptah_check_sequence")
		c.Check(dropErr, qt.IsNil)
	}()
	_, err = conn.ExecContext(ctx, "CREATE SEQUENCE dbo.ptah_check_sequence AS BIGINT START WITH 41 INCREMENT BY 1")
	c.Assert(err, qt.IsNil)

	var before any
	err = conn.QueryRowContext(
		ctx,
		"SELECT current_value FROM sys.sequences WHERE name = 'ptah_check_sequence'",
	).Scan(&before)
	c.Assert(err, qt.IsNil)

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_checked.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- checks.sql --
SELECT NEXT VALUE FOR dbo.ptah_check_sequence;

-- migration.sql --
SELECT 1;
`)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*must not advance a SQL Server sequence with NEXT VALUE FOR.*`)

	var after any
	err = conn.QueryRowContext(
		ctx,
		"SELECT current_value FROM sys.sequences WHERE name = 'ptah_check_sequence'",
	).Scan(&after)
	c.Assert(err, qt.IsNil)
	c.Assert(after, qt.DeepEquals, before)

	current, err := mig.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(current, qt.Equals, int64(0))
}

type issue819Revision struct {
	Version               string
	Description           string
	RevisionType          int
	Applied               int
	Total                 int
	ExecutedAtNonZero     bool
	ExecutionTimePositive bool
	Error                 sql.NullString
	ErrorStatement        sql.NullString
	Hash                  string
	PartialHashes         sql.NullString
	OperatorVersion       string
}

func readIssue819Revisions(tb testing.TB, conn *dbschema.DatabaseConnection) []issue819Revision {
	c := qt.New(tb)
	c.Helper()

	rows, err := conn.QueryContext(
		context.Background(),
		`SELECT version, description, type, applied, total, executed_at, execution_time,
	error, error_stmt, hash, partial_hashes, operator_version
FROM atlas_schema_revisions
ORDER BY version`,
	)
	c.Assert(err, qt.IsNil)
	defer func() {
		c.Check(rows.Close(), qt.IsNil)
	}()

	revisions := make([]issue819Revision, 0)
	for rows.Next() {
		var revision issue819Revision
		var executedAt sql.NullString
		var executionTime int64
		err = rows.Scan(
			&revision.Version,
			&revision.Description,
			&revision.RevisionType,
			&revision.Applied,
			&revision.Total,
			&executedAt,
			&executionTime,
			&revision.Error,
			&revision.ErrorStatement,
			&revision.Hash,
			&revision.PartialHashes,
			&revision.OperatorVersion,
		)
		c.Assert(err, qt.IsNil)
		revision.ExecutedAtNonZero = executedAt.Valid && strings.TrimSpace(executedAt.String) != ""
		revision.ExecutionTimePositive = executionTime > 0
		revisions = append(revisions, revision)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return revisions
}

func issue819RevisionVersions(revisions []migrator.AtlasRevisionChange) []int64 {
	versions := make([]int64, 0, len(revisions))
	for _, revision := range revisions {
		versions = append(versions, revision.Version)
	}
	return versions
}

func issue819SQLHash(sqlText string) string {
	sum := sha256.Sum256([]byte(sqlText))
	return hex.EncodeToString(sum[:])
}

func markIssue819RevisionDirty(tb testing.TB, conn *dbschema.DatabaseConnection, version string) {
	c := qt.New(tb)
	c.Helper()

	query := sqlutil.Rebind(
		conn.Info().Dialect,
		`UPDATE atlas_schema_revisions SET type = 1, applied = 0, total = 1, error = 'broken' WHERE version = ?`,
	)
	_, err := conn.ExecContext(context.Background(), query, version)
	c.Assert(err, qt.IsNil)
}

// mysqlAtlasTestURL and mariaDBAtlasTestURL keep the scheme work dbtarget does
// not do for this family: it declares no scheme for MySQL or MariaDB, because a
// MySQL address is often a driver DSN carrying none, so a bare DSN still has to
// be given one and an address of the sibling engine still has to be refused.
func mysqlAtlasTestURL(t *testing.T) string {
	t.Helper()

	return mySQLFamilyAtlasURL(t, dbtarget.MySQL, "mysql")
}

func mariaDBAtlasTestURL(t *testing.T) string {
	t.Helper()

	return mySQLFamilyAtlasURL(t, dbtarget.MariaDB, "mariadb")
}

func mySQLFamilyAtlasURL(t *testing.T, engine dbtarget.Engine, dialect string) string {
	t.Helper()

	dbURL := dbtarget.URL(t, engine)
	if strings.Contains(dbURL, "@tcp(") && !strings.HasPrefix(dbURL, dialect+"://") {
		dbURL = dialect + "://" + dbURL
	}
	if !strings.HasPrefix(dbURL, dialect+"://") {
		t.Skipf("%s URL required for Atlas migration integration test", dialect)
	}
	return dbURL
}

// cockroachDBAtlasTestURL and yugabyteDBAtlasTestURL keep the rewrite dbtarget
// deliberately does not do: both engines accept a postgres:// address, and the
// migrator has to be told which family member it is talking to.
func cockroachDBAtlasTestURL(t *testing.T) string {
	t.Helper()

	return postgresFamilyAtlasURL(t, dbtarget.CockroachDB, "cockroachdb")
}

func yugabyteDBAtlasTestURL(t *testing.T) string {
	t.Helper()

	return postgresFamilyAtlasURL(t, dbtarget.YugabyteDB, "yugabytedb")
}

func postgresFamilyAtlasURL(t *testing.T, engine dbtarget.Engine, dialect string) string {
	t.Helper()

	dbURL := dbtarget.URL(t, engine)
	if rest, ok := strings.CutPrefix(dbURL, "postgresql://"); ok {
		dbURL = dialect + "://" + rest
	}
	if rest, ok := strings.CutPrefix(dbURL, "postgres://"); ok {
		dbURL = dialect + "://" + rest
	}
	if !strings.HasPrefix(dbURL, dialect+"://") {
		t.Skipf("%s URL required for Atlas migration integration test", dialect)
	}
	return dbURL
}

func cleanupIssue273(t *testing.T, conn *dbschema.DatabaseConnection) {
	t.Helper()

	for _, statement := range []string{
		"DROP TABLE IF EXISTS ptah_issue_273_audit",
		"DROP TABLE IF EXISTS ptah_issue_273_users",
		"DROP TABLE IF EXISTS ptah_issue_273_teams",
		"DROP TABLE IF EXISTS schema_migrations_issue_273",
	} {
		_, _ = conn.ExecContext(context.Background(), statement)
	}
}

func cleanupIssue299(t *testing.T, conn *dbschema.DatabaseConnection) {
	t.Helper()

	for _, statement := range []string{
		"DROP TABLE IF EXISTS ptah_issue_299_users_dev",
		"DROP TABLE IF EXISTS ptah_issue_299_prod",
		"DROP TABLE IF EXISTS ptah_issue_299_dev",
		"DROP TABLE IF EXISTS schema_migrations_issue_299",
	} {
		_, _ = conn.ExecContext(context.Background(), statement)
	}
}

func cleanupIssue275(t *testing.T, conn *dbschema.DatabaseConnection) {
	t.Helper()

	for _, statement := range []string{
		"DROP TABLE IF EXISTS ptah_issue_275_next",
		"DROP TABLE IF EXISTS ptah_issue_275_seed",
		"DROP TABLE IF EXISTS atlas_schema_revisions",
	} {
		_, _ = conn.ExecContext(context.Background(), statement)
	}
}

func cleanupIssue290(t *testing.T, conn *dbschema.DatabaseConnection) {
	t.Helper()

	for _, statement := range []string{
		"DROP TABLE IF EXISTS ptah_issue_290_widgets",
		"DROP TABLE IF EXISTS schema_migrations_issue_290",
	} {
		_, _ = conn.ExecContext(context.Background(), statement)
	}
}

func cleanupIssue819(t *testing.T, conn *dbschema.DatabaseConnection) {
	t.Helper()

	_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS atlas_schema_revisions")
	_, _ = conn.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS ptah_issue_819_pause_delete()")
}

func cleanupIssue937(t *testing.T, conn *dbschema.DatabaseConnection) {
	t.Helper()

	conn.SchemaWriter().SetDryRun(false)
	_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS ptah_issue_937_revisions")
}

func createLegacyIssue273MetadataTable(t *testing.T, conn *dbschema.DatabaseConnection) {
	c := qt.New(t)
	t.Helper()

	_, err := conn.ExecContext(
		context.Background(),
		`CREATE TABLE schema_migrations_issue_273 (
			version INTEGER PRIMARY KEY,
			description TEXT NOT NULL,
			applied_at TIMESTAMP NOT NULL
		)`,
	)
	c.Assert(err, qt.IsNil)
}

func issue273UsersCount(t *testing.T, conn *dbschema.DatabaseConnection) int {
	c := qt.New(t)
	t.Helper()

	var count int
	err := conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM ptah_issue_273_users").Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func issue273Versions(t *testing.T, conn *dbschema.DatabaseConnection) []int64 {
	c := qt.New(t)
	t.Helper()

	rows, err := conn.Query("SELECT version FROM schema_migrations_issue_273 ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var versions []int64
	for rows.Next() {
		var version int64
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

func issue290WidgetsCount(t *testing.T, conn *dbschema.DatabaseConnection) int {
	c := qt.New(t)
	t.Helper()

	var count int
	err := conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM ptah_issue_290_widgets").Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func issue290Versions(t *testing.T, conn *dbschema.DatabaseConnection) []int64 {
	c := qt.New(t)
	t.Helper()

	rows, err := conn.Query("SELECT version FROM schema_migrations_issue_290 ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var versions []int64
	for rows.Next() {
		var version int64
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

func issue299Versions(t *testing.T, conn *dbschema.DatabaseConnection) []int64 {
	c := qt.New(t)
	t.Helper()

	rows, err := conn.Query("SELECT version FROM schema_migrations_issue_299 ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var versions []int64
	for rows.Next() {
		var version int64
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

type issue275Revision struct {
	Version         string
	Description     string
	RevisionType    int
	Applied         int
	Total           int
	Hash            string
	OperatorVersion string
}

func issue275Revisions(t *testing.T, conn *dbschema.DatabaseConnection) []issue275Revision {
	c := qt.New(t)
	t.Helper()

	rows, err := conn.Query(`SELECT version, description, type, applied, total, hash, operator_version
FROM atlas_schema_revisions
ORDER BY CAST(version AS BIGINT)`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var revisions []issue275Revision
	for rows.Next() {
		var revision issue275Revision
		c.Assert(rows.Scan(
			&revision.Version,
			&revision.Description,
			&revision.RevisionType,
			&revision.Applied,
			&revision.Total,
			&revision.Hash,
			&revision.OperatorVersion,
		), qt.IsNil)
		revisions = append(revisions, revision)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return revisions
}

func issue290WidgetTableExists(t *testing.T, conn *dbschema.DatabaseConnection) bool {
	t.Helper()

	var count int
	err := conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM ptah_issue_290_widgets").Scan(&count)
	return err == nil
}
