package migrator_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// Every migrator read entry point calls Initialize, so a dry run that skipped
// memoizing its metadata inspection re-inspected the table — and repeated the
// "[DRY RUN] Would initialize migrations metadata" narration — once per read.
// The regression was introduced with the dry-run revision-state fix
// (stokaro/ptah#963) and reported on stokaro/ptah#967.

func openInitializeOnceSQLite(t *testing.T) *dbschema.DatabaseConnection {
	t.Helper()
	conn, err := dbschema.ConnectToDatabase(
		context.Background(),
		"sqlite://"+filepath.Join(t.TempDir(), "initialize-once.db"),
	)
	qt.Assert(t, err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// countingHandler records the messages of every log record it receives.
type countingHandler struct {
	messages *[]string
}

func (countingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h countingHandler) Handle(_ context.Context, record slog.Record) error {
	*h.messages = append(*h.messages, record.Message)
	return nil
}

func (h countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h countingHandler) WithGroup(string) slog.Handler { return h }

func countMessages(messages []string, want string) int {
	count := 0
	for _, message := range messages {
		count += strings.Count(message, want)
	}
	return count
}

func TestDryRunInitializeNarratesMetadataOnce(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := openInitializeOnceSQLite(t)
	conn.SchemaWriter().SetDryRun(true)

	var messages []string
	mig := migrator.NewMigrator(
		conn,
		migrator.NewRegisteredMigrationProvider(
			migrator.CreateMigrationFromSQL(
				1,
				"create_users",
				"CREATE TABLE initialize_once_users (id INTEGER PRIMARY KEY);",
				"DROP TABLE initialize_once_users;",
			),
		),
	).WithLogger(slog.New(countingHandler{messages: &messages}))

	// GetMigrationStatus alone reads the version, the applied set, and the
	// revision rows; MigrateUp reads again after executing.
	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(0))
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)

	c.Assert(countMessages(messages, "[DRY RUN] Would initialize migrations metadata"), qt.Equals, 1)
}

func TestDryRunInitializeDoesNotSuppressRealInitialize(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := openInitializeOnceSQLite(t)
	conn.SchemaWriter().SetDryRun(true)

	mig := migrator.NewMigrator(
		conn,
		migrator.NewRegisteredMigrationProvider(
			migrator.CreateMigrationFromSQL(
				1,
				"create_users",
				"CREATE TABLE initialize_once_users (id INTEGER PRIMARY KEY);",
				"DROP TABLE initialize_once_users;",
			),
		),
	)

	c.Assert(mig.Initialize(ctx), qt.IsNil)

	// The memoized dry-run inspection must not be reused once the writer leaves
	// dry-run mode: the metadata table still has to be created for real.
	conn.SchemaWriter().SetDryRun(false)
	c.Assert(mig.Initialize(ctx), qt.IsNil)

	var tableCount int
	err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'",
	).Scan(&tableCount)
	c.Assert(err, qt.IsNil)
	c.Assert(tableCount, qt.Equals, 1)
}
