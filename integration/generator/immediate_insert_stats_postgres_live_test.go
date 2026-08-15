//go:build integration

package generator_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestGenerateMigration_ImmediateInsertsWithoutAnalyzeBuildConcurrentlyWithRealPostgres
// pins the literal scenario acceptance line 4 of stokaro/ptah#997 names: rows
// go in, nobody runs ANALYZE, and a migration is generated straight away.
//
// This is a different catalog state from the one
// TestGenerateMigration_UnknownRowStatisticsBuildConcurrentlyWithRealPostgres
// reaches, and only these two together cover the decision. There the
// cumulative counters were reset, so pg_class.reltuples and
// pg_stat_all_tables.n_live_tup BOTH report the numbers an empty table
// reports and the tri-state resolves through RowStatsUnknown. Here the
// counters are intact: reltuples is still -1 because only ANALYZE and VACUUM
// write it, while n_live_tup already carries all five thousand rows. The
// estimate has to read n_live_tup to see them, because reltuples alone is
// -1 and floors to the 0 an empty table reports -- and with n_live_tup above
// zero the relation no longer counts as statistics-unknown either, so nothing
// else in the decision can rescue it.
//
// pg_stat_force_next_flush runs on the same pinned connection as the INSERT
// because a backend accumulates its counters locally and only publishes them
// at a bounded interval; without the forced publication n_live_tup would be
// 0 or 5000 depending on how long the test took, and the row this pins would
// not be the row it claims to pin.
func TestGenerateMigration_ImmediateInsertsWithoutAnalyzeBuildConcurrentlyWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	adminURL := requireGeneratorPostgresURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	c.Assert(platform.NormalizeDialect(admin.Info().Dialect), qt.Equals, platform.Postgres)
	targetURL, targetDatabase := createGeneratorTestPostgres(c.TB, admin, adminURL, "ptah_generator_fresh_inserts")
	defer dropGeneratorTestPostgres(c.TB, admin, targetDatabase)
	target, err := dbschema.ConnectToDatabase(ctx, targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)

	setupImmediateInsertsWithoutAnalyze(c.TB, targetURL)

	// The precondition: five thousand rows, nothing has analyzed the table, and
	// the only place the row count survives is n_live_tup.
	var reltuples, liveTuples, actualRows int64
	c.Assert(target.QueryRowContext(ctx, `
		SELECT c.reltuples::bigint,
		       COALESCE(st.n_live_tup, 0),
		       (SELECT COUNT(*) FROM members)
		FROM pg_class c
		LEFT JOIN pg_stat_all_tables st ON st.relid = c.oid
		WHERE c.relname = 'members'
	`).Scan(&reltuples, &liveTuples, &actualRows), qt.IsNil)
	c.Assert(reltuples, qt.Equals, int64(-1))
	c.Assert(liveTuples, qt.Equals, int64(5000))
	c.Assert(actualRows, qt.Equals, int64(5000))

	dir := t.TempDir()
	entitiesDir := writeUnknownStatsIndexEntities(c.TB, dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)

	files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesDir: entitiesDir,
		DatabaseURL:   targetURL,
		MigrationName: "add_members_email_index",
		OutputDir:     migrationsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)
	c.Assert(files.Files, qt.HasLen, 1)
	c.Assert(files.Files[0].NoTransaction, qt.IsTrue)

	upSQL, err := os.ReadFile(files.Files[0].UpFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upSQL), qt.Contains, "-- +ptah no_transaction")
	c.Assert(string(upSQL), qt.Contains, `CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_members_email" ON "members" ("email");`)
	downSQL, err := os.ReadFile(files.Files[0].DownFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(downSQL), qt.Contains, "-- +ptah no_transaction")
	c.Assert(string(downSQL), qt.Contains, `DROP INDEX CONCURRENTLY IF EXISTS "idx_members_email";`)

	// Rendered is not applied: both halves are executed and the catalog is read
	// rather than the file, and the pair is replayed so a resumed concurrent
	// build meeting its own leftovers is covered too.
	provider, err := migrator.NewFSMigrationProvider(os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	migrations := migrator.NewMigrator(target, provider)
	c.Assert(migrations.MigrateUp(ctx), qt.IsNil)
	exists, valid := readGeneratorPostgresIndexState(c.TB, target, "idx_members_email")
	c.Assert(exists, qt.IsTrue)
	c.Assert(valid, qt.IsTrue)

	c.Assert(migrations.MigrateDown(ctx), qt.IsNil)
	exists, _ = readGeneratorPostgresIndexState(c.TB, target, "idx_members_email")
	c.Assert(exists, qt.IsFalse)

	c.Assert(migrations.MigrateUp(ctx), qt.IsNil)
	exists, valid = readGeneratorPostgresIndexState(c.TB, target, "idx_members_email")
	c.Assert(exists, qt.IsTrue)
	c.Assert(valid, qt.IsTrue)
}

// setupImmediateInsertsWithoutAnalyze fills a table and publishes the INSERT's
// cumulative counters without letting anything compute statistics for it.
//
// Autovacuum is disabled on the table so no background analyze can arrive
// between the insert and the read and turn reltuples into a real number, which
// would silently move the test onto the path the ordinary populated fixture
// already covers.
func setupImmediateInsertsWithoutAnalyze(tb testing.TB, targetURL string) {
	c := qt.New(tb)
	c.Helper()
	raw, err := sql.Open("pgx", targetURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(raw.Close(), qt.IsNil) }()
	conn, err := raw.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(conn.Close(), qt.IsNil) }()

	for _, statement := range []string{
		`CREATE TABLE members (
			id BIGINT NOT NULL,
			email TEXT NOT NULL
		) WITH (autovacuum_enabled = false)`,
		`INSERT INTO members SELECT g, 'user-' || g || '@example.com' FROM generate_series(1, 5000) AS g`,
		`SELECT pg_stat_force_next_flush()`,
	} {
		_, err = conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement failed: %s", statement))
	}
}
