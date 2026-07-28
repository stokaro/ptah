package migrator_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestAtlasFormat_PostgresIntegration(t *testing.T) {
	runAtlasFormatIntegration(t, postgresTestURL(t))
}

func TestAtlasFormat_MySQLIntegration(t *testing.T) {
	runAtlasFormatIntegration(t, mysqlAtlasTestURL(t))
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

func TestAtlasTxtarDown_MySQLIntegration(t *testing.T) {
	runAtlasTxtarDownIntegration(t, mysqlAtlasTestURL(t))
}

func TestAtlasRevisionMetadata_PostgresIntegration(t *testing.T) {
	runAtlasRevisionMetadataIntegration(t, postgresTestURL(t))
}

func TestAtlasRevisionMetadata_MySQLIntegration(t *testing.T) {
	runAtlasRevisionMetadataIntegration(t, mysqlAtlasTestURL(t))
}

func TestAtlasRevisionMetadata_MariaDBIntegration(t *testing.T) {
	runAtlasRevisionMetadataIntegration(t, mariaDBAtlasTestURL(t))
}

func TestAtlasRevisionMetadata_SQLServerIntegration(t *testing.T) {
	runAtlasRevisionMetadataIntegration(t, sqlServerAtlasTestURL(t))
}

func TestAtlasRevisionMetadata_CockroachDBIntegration(t *testing.T) {
	runAtlasRevisionMetadataIntegration(t, cockroachDBAtlasTestURL(t))
}

func TestAtlasRevisionMetadata_YugabyteDBIntegration(t *testing.T) {
	runAtlasRevisionMetadataIntegration(t, yugabyteDBAtlasTestURL(t))
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
	waitForIssue819AdvisoryWait(c, conn)

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
	c.Assert(readIssue819Revisions(c, conn), qt.DeepEquals, []issue819Revision{
		{
			Version:       "1",
			Description:   "create_accounts",
			RevisionType:  4,
			Hash:          issue819SQLHash("SELECT 1;\n"),
			PartialHashes: sql.NullString{String: "null", Valid: true},
		},
	})
}

func TestAtlasRevisionMetadata_ClickHouseRejectsSetIntegration(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), clickHouseAtlasTestURL(t))
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

func waitForIssue819AdvisoryWait(c *qt.C, conn *dbschema.DatabaseConnection) {
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
		{Version: strconv.FormatInt(secondVersion, 10), Description: "Next", RevisionType: 2, Applied: 1, Total: 1, Hash: "nexthash", OperatorVersion: "Ptah"},
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
	c.Assert(readIssue819Revisions(c, conn), qt.DeepEquals, []issue819Revision{
		{
			Version:      "1",
			Description:  "Create Accounts",
			RevisionType: 2,
			Applied:      1,
			Total:        1,
			Hash:         issue819SQLHash(firstSQL),
		},
	})

	result, err := mig.SetAtlasRevision(ctx, 3)
	c.Assert(err, qt.IsNil)
	c.Assert(issue819RevisionVersions(result.Set), qt.DeepEquals, []int64{2, 3})
	c.Assert(result.Removed, qt.HasLen, 0)
	c.Assert(readIssue819Revisions(c, conn), qt.DeepEquals, []issue819Revision{
		{
			Version:      "1",
			Description:  "Create Accounts",
			RevisionType: 2,
			Applied:      1,
			Total:        1,
			Hash:         issue819SQLHash(firstSQL),
		},
		{
			Version:       "2",
			Description:   "create_users",
			RevisionType:  4,
			Hash:          issue819SQLHash(secondSQL),
			PartialHashes: sql.NullString{String: "null", Valid: true},
		},
		{
			Version:       "3",
			Description:   "add_audit",
			RevisionType:  4,
			Hash:          issue819SQLHash(thirdSQL),
			PartialHashes: sql.NullString{String: "null", Valid: true},
		},
	})

	markIssue819RevisionDirty(c, conn, "1")
	result, err = mig.SetAtlasRevision(ctx, 2)
	c.Assert(err, qt.IsNil)
	c.Assert(issue819RevisionVersions(result.Set), qt.DeepEquals, []int64{1})
	c.Assert(issue819RevisionVersions(result.Removed), qt.DeepEquals, []int64{3})
	c.Assert(readIssue819Revisions(c, conn), qt.DeepEquals, []issue819Revision{
		{
			Version:      "1",
			Description:  "Create Accounts",
			RevisionType: 6,
			Total:        1,
			Error:        "broken",
			Hash:         issue819SQLHash(firstSQL),
		},
		{
			Version:       "2",
			Description:   "create_users",
			RevisionType:  4,
			Hash:          issue819SQLHash(secondSQL),
			PartialHashes: sql.NullString{String: "null", Valid: true},
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
	c.Assert(readIssue819Revisions(c, conn), qt.DeepEquals, []issue819Revision{
		{
			Version:       "2",
			Description:   "create_users",
			RevisionType:  1,
			PartialHashes: sql.NullString{String: "null", Valid: true},
		},
	})
}

type issue819Revision struct {
	Version       string
	Description   string
	RevisionType  int
	Applied       int
	Total         int
	Error         string
	Hash          string
	PartialHashes sql.NullString
}

func readIssue819Revisions(c *qt.C, conn *dbschema.DatabaseConnection) []issue819Revision {
	c.Helper()

	rows, err := conn.QueryContext(
		context.Background(),
		`SELECT version, description, type, applied, total, COALESCE(error, ''), hash, partial_hashes
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
		err = rows.Scan(
			&revision.Version,
			&revision.Description,
			&revision.RevisionType,
			&revision.Applied,
			&revision.Total,
			&revision.Error,
			&revision.Hash,
			&revision.PartialHashes,
		)
		c.Assert(err, qt.IsNil)
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

func markIssue819RevisionDirty(c *qt.C, conn *dbschema.DatabaseConnection, version string) {
	c.Helper()

	query := sqlutil.Rebind(
		conn.Info().Dialect,
		`UPDATE atlas_schema_revisions SET type = 1, applied = 0, total = 1, error = 'broken' WHERE version = ?`,
	)
	_, err := conn.ExecContext(context.Background(), query, version)
	c.Assert(err, qt.IsNil)
}

func mysqlAtlasTestURL(t *testing.T) string {
	t.Helper()

	dbURL := os.Getenv("MYSQL_TEST_DSN")
	if dbURL == "" {
		dbURL = os.Getenv("MYSQL_TEST_URL")
	}
	if dbURL == "" {
		t.Skip("MYSQL_TEST_DSN or MYSQL_TEST_URL not set")
	}
	if strings.Contains(dbURL, "@tcp(") && !strings.HasPrefix(dbURL, "mysql://") {
		dbURL = "mysql://" + dbURL
	}
	if !strings.HasPrefix(dbURL, "mysql://") {
		t.Skip("MySQL URL required for Atlas migration integration test")
	}
	return dbURL
}

func mariaDBAtlasTestURL(t *testing.T) string {
	t.Helper()

	dbURL := os.Getenv("MARIADB_TEST_DSN")
	if dbURL == "" {
		dbURL = os.Getenv("MARIADB_TEST_URL")
	}
	if dbURL == "" {
		t.Skip("MARIADB_TEST_DSN or MARIADB_TEST_URL not set")
	}
	if strings.Contains(dbURL, "@tcp(") && !strings.HasPrefix(dbURL, "mariadb://") {
		dbURL = "mariadb://" + dbURL
	}
	if !strings.HasPrefix(dbURL, "mariadb://") {
		t.Skip("MariaDB URL required for Atlas migration integration test")
	}
	return dbURL
}

func sqlServerAtlasTestURL(t *testing.T) string {
	t.Helper()

	dbURL := os.Getenv("PTAH_SQLSERVER_TEST_URL")
	if dbURL == "" {
		t.Skip("PTAH_SQLSERVER_TEST_URL not set")
	}
	return dbURL
}

func clickHouseAtlasTestURL(t *testing.T) string {
	t.Helper()

	dbURL := os.Getenv("CLICKHOUSE_URL")
	if dbURL == "" {
		t.Skip("CLICKHOUSE_URL not set")
	}
	return dbURL
}

func cockroachDBAtlasTestURL(t *testing.T) string {
	t.Helper()

	dbURL := os.Getenv("COCKROACHDB_TEST_DSN")
	if dbURL == "" {
		dbURL = os.Getenv("COCKROACHDB_URL")
	}
	if dbURL == "" {
		t.Skip("COCKROACHDB_TEST_DSN or COCKROACHDB_URL not set")
	}
	if rest, ok := strings.CutPrefix(dbURL, "postgresql://"); ok {
		dbURL = "cockroachdb://" + rest
	}
	if rest, ok := strings.CutPrefix(dbURL, "postgres://"); ok {
		dbURL = "cockroachdb://" + rest
	}
	if !strings.HasPrefix(dbURL, "cockroachdb://") {
		t.Skip("CockroachDB URL required for Atlas migration integration test")
	}
	return dbURL
}

func yugabyteDBAtlasTestURL(t *testing.T) string {
	t.Helper()

	dbURL := os.Getenv("YUGABYTEDB_TEST_DSN")
	if dbURL == "" {
		dbURL = os.Getenv("YUGABYTEDB_URL")
	}
	if dbURL == "" {
		t.Skip("YUGABYTEDB_TEST_DSN or YUGABYTEDB_URL not set")
	}
	if rest, ok := strings.CutPrefix(dbURL, "postgresql://"); ok {
		dbURL = "yugabytedb://" + rest
	}
	if rest, ok := strings.CutPrefix(dbURL, "postgres://"); ok {
		dbURL = "yugabytedb://" + rest
	}
	if !strings.HasPrefix(dbURL, "yugabytedb://") {
		t.Skip("YugabyteDB URL required for Atlas migration integration test")
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

func createLegacyIssue273MetadataTable(t *testing.T, conn *dbschema.DatabaseConnection) {
	t.Helper()

	_, err := conn.ExecContext(
		context.Background(),
		`CREATE TABLE schema_migrations_issue_273 (
			version INTEGER PRIMARY KEY,
			description TEXT NOT NULL,
			applied_at TIMESTAMP NOT NULL
		)`,
	)
	qt.Assert(t, err, qt.IsNil)
}

func issue273UsersCount(t *testing.T, conn *dbschema.DatabaseConnection) int {
	t.Helper()

	var count int
	err := conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM ptah_issue_273_users").Scan(&count)
	qt.Assert(t, err, qt.IsNil)
	return count
}

func issue273Versions(t *testing.T, conn *dbschema.DatabaseConnection) []int64 {
	t.Helper()

	rows, err := conn.Query("SELECT version FROM schema_migrations_issue_273 ORDER BY version")
	qt.Assert(t, err, qt.IsNil)
	defer rows.Close()

	var versions []int64
	for rows.Next() {
		var version int64
		qt.Assert(t, rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	qt.Assert(t, rows.Err(), qt.IsNil)
	return versions
}

func issue290WidgetsCount(t *testing.T, conn *dbschema.DatabaseConnection) int {
	t.Helper()

	var count int
	err := conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM ptah_issue_290_widgets").Scan(&count)
	qt.Assert(t, err, qt.IsNil)
	return count
}

func issue290Versions(t *testing.T, conn *dbschema.DatabaseConnection) []int64 {
	t.Helper()

	rows, err := conn.Query("SELECT version FROM schema_migrations_issue_290 ORDER BY version")
	qt.Assert(t, err, qt.IsNil)
	defer rows.Close()

	var versions []int64
	for rows.Next() {
		var version int64
		qt.Assert(t, rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	qt.Assert(t, rows.Err(), qt.IsNil)
	return versions
}

func issue299Versions(t *testing.T, conn *dbschema.DatabaseConnection) []int64 {
	t.Helper()

	rows, err := conn.Query("SELECT version FROM schema_migrations_issue_299 ORDER BY version")
	qt.Assert(t, err, qt.IsNil)
	defer rows.Close()

	var versions []int64
	for rows.Next() {
		var version int64
		qt.Assert(t, rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	qt.Assert(t, rows.Err(), qt.IsNil)
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
	t.Helper()

	rows, err := conn.Query(`SELECT version, description, type, applied, total, hash, operator_version
FROM atlas_schema_revisions
ORDER BY CAST(version AS BIGINT)`)
	qt.Assert(t, err, qt.IsNil)
	defer rows.Close()

	var revisions []issue275Revision
	for rows.Next() {
		var revision issue275Revision
		qt.Assert(t, rows.Scan(
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
	qt.Assert(t, rows.Err(), qt.IsNil)
	return revisions
}

func issue290WidgetTableExists(t *testing.T, conn *dbschema.DatabaseConnection) bool {
	t.Helper()

	var count int
	err := conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM ptah_issue_290_widgets").Scan(&count)
	return err == nil
}
