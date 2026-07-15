package migrator_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestAtlasFormat_PostgresIntegration(t *testing.T) {
	runAtlasFormatIntegration(t, postgresTestURL(t))
}

func TestAtlasFormat_MySQLIntegration(t *testing.T) {
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
	runAtlasFormatIntegration(t, dbURL)
}

func TestAtlasTxtarDown_PostgresIntegration(t *testing.T) {
	runAtlasTxtarDownIntegration(t, postgresTestURL(t))
}

func TestAtlasTxtarDown_MySQLIntegration(t *testing.T) {
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
	runAtlasTxtarDownIntegration(t, dbURL)
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

func cleanupIssue290(t *testing.T, conn *dbschema.DatabaseConnection) {
	t.Helper()

	for _, statement := range []string{
		"DROP TABLE IF EXISTS ptah_issue_290_widgets",
		"DROP TABLE IF EXISTS schema_migrations_issue_290",
	} {
		_, _ = conn.ExecContext(context.Background(), statement)
	}
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

func issue290WidgetTableExists(t *testing.T, conn *dbschema.DatabaseConnection) bool {
	t.Helper()

	var count int
	err := conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM ptah_issue_290_widgets").Scan(&count)
	return err == nil
}
