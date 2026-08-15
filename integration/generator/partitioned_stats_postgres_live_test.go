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

// TestGenerateMigration_PartitionedParentAvoidsConcurrentIndexWithRealPostgres
// pins the whole round trip for a declaratively partitioned parent.
//
// The parent reports pg_class.reltuples for every row in every partition, so
// the populated-table heuristic selected it and emitted CREATE INDEX
// CONCURRENTLY -- which PostgreSQL answers with SQLSTATE 0A000 at execution
// time, after the migration file, its checksum and its commit already exist.
// The rollback half is refused the same way ("cannot drop partitioned index
// ... concurrently"), so both directions are executed here rather than
// inspected.
func TestGenerateMigration_PartitionedParentAvoidsConcurrentIndexWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	adminURL := requireGeneratorPostgresURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	c.Assert(platform.NormalizeDialect(admin.Info().Dialect), qt.Equals, platform.Postgres)
	targetURL, targetDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_generator_partitioned")
	defer dropGeneratorTestPostgres(c, admin, targetDatabase)
	target, err := dbschema.ConnectToDatabase(ctx, targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)

	_, err = target.ExecContext(ctx, `
		CREATE TABLE events (
			id BIGINT NOT NULL,
			tenant TEXT NOT NULL,
			created_at DATE NOT NULL
		) PARTITION BY RANGE (created_at);
		CREATE TABLE events_2026 PARTITION OF events
			FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
		INSERT INTO events
			SELECT g, 'tenant-' || (g % 7), DATE '2026-01-01' + (g % 300)
			FROM generate_series(1, 5000) AS g;
		ANALYZE;
	`)
	c.Assert(err, qt.IsNil)

	// The precondition the whole test rests on: the parent is relkind 'p' and
	// reports the partitions' rows as its own, so the heuristic sees a
	// populated table.
	var relkind string
	var estimatedRows int64
	c.Assert(target.QueryRowContext(ctx, `
		SELECT relkind::text, reltuples::bigint FROM pg_class WHERE relname = 'events'
	`).Scan(&relkind, &estimatedRows), qt.IsNil)
	c.Assert(relkind, qt.Equals, "p")
	c.Assert(estimatedRows, qt.Equals, int64(5000))

	dir := t.TempDir()
	entitiesDir := writePartitionedIndexEntities(c, dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)

	files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesDir: entitiesDir,
		DatabaseURL:   targetURL,
		MigrationName: "add_events_tenant_index",
		OutputDir:     migrationsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)
	c.Assert(files.Files, qt.HasLen, 1)
	c.Assert(files.Files[0].NoTransaction, qt.IsFalse)

	upSQL, err := os.ReadFile(files.Files[0].UpFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upSQL), qt.Contains, `CREATE INDEX IF NOT EXISTS "idx_events_tenant" ON "events" ("tenant");`)
	c.Assert(string(upSQL), qt.Not(qt.Contains), "CONCURRENTLY")
	downSQL, err := os.ReadFile(files.Files[0].DownFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(downSQL), qt.Contains, `DROP INDEX IF EXISTS "idx_events_tenant";`)
	c.Assert(string(downSQL), qt.Not(qt.Contains), "CONCURRENTLY")

	provider, err := migrator.NewFSMigrationProvider(os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	migrations := migrator.NewMigrator(target, provider)
	c.Assert(migrations.MigrateUp(ctx), qt.IsNil)
	exists, valid := readGeneratorPostgresIndexState(c, target, "idx_events_tenant")
	c.Assert(exists, qt.IsTrue)
	c.Assert(valid, qt.IsTrue)

	c.Assert(migrations.MigrateDown(ctx), qt.IsNil)
	exists, _ = readGeneratorPostgresIndexState(c, target, "idx_events_tenant")
	c.Assert(exists, qt.IsFalse)

	// up/down/up: the pair has to be replayable, not merely runnable once.
	c.Assert(migrations.MigrateUp(ctx), qt.IsNil)
	exists, valid = readGeneratorPostgresIndexState(c, target, "idx_events_tenant")
	c.Assert(exists, qt.IsTrue)
	c.Assert(valid, qt.IsTrue)
}

// TestGenerateMigration_UnknownRowStatisticsBuildConcurrentlyWithRealPostgres
// pins the other half of the same decision: a populated table whose statistics
// PostgreSQL cannot report.
//
// pg_class.reltuples is -1 until something analyzes a relation, and the
// cumulative counters read 0 after they are reset -- which happens on a
// crash-recovery restart, on an explicit reset, and for a table restored into a
// fresh cluster. GREATEST floored both to 0, so five thousand rows were
// indistinguishable from an empty table and earned a blocking CREATE INDEX.
func TestGenerateMigration_UnknownRowStatisticsBuildConcurrentlyWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	adminURL := requireGeneratorPostgresURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	c.Assert(platform.NormalizeDialect(admin.Info().Dialect), qt.Equals, platform.Postgres)
	targetURL, targetDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_generator_unknown_stats")
	defer dropGeneratorTestPostgres(c, admin, targetDatabase)
	target, err := dbschema.ConnectToDatabase(ctx, targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)

	setupUnknownRowStatistics(c, targetURL)

	// The precondition: the table holds 5000 rows and the catalog reports the
	// same numbers an empty table reports.
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
	c.Assert(liveTuples, qt.Equals, int64(0))
	c.Assert(actualRows, qt.Equals, int64(5000))

	dir := t.TempDir()
	entitiesDir := writeUnknownStatsIndexEntities(c, dir)
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
	c.Assert(string(downSQL), qt.Contains, `DROP INDEX CONCURRENTLY IF EXISTS "idx_members_email";`)

	provider, err := migrator.NewFSMigrationProvider(os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	migrations := migrator.NewMigrator(target, provider)
	c.Assert(migrations.MigrateUp(ctx), qt.IsNil)
	exists, valid := readGeneratorPostgresIndexState(c, target, "idx_members_email")
	c.Assert(exists, qt.IsTrue)
	c.Assert(valid, qt.IsTrue)

	c.Assert(migrations.MigrateDown(ctx), qt.IsNil)
	exists, _ = readGeneratorPostgresIndexState(c, target, "idx_members_email")
	c.Assert(exists, qt.IsFalse)

	// up/down/up over a non-transactional pair: both halves have to be
	// replayable, and a concurrent build that resumes onto its own leftovers is
	// exactly where that stops being true.
	c.Assert(migrations.MigrateUp(ctx), qt.IsNil)
	exists, valid = readGeneratorPostgresIndexState(c, target, "idx_members_email")
	c.Assert(exists, qt.IsTrue)
	c.Assert(valid, qt.IsTrue)
}

// TestGenerateMigration_EmptyNeverAnalyzedTableStaysTransactionalWithRealPostgres
// is the other side of the tri-state, and it is the common case rather than the
// exotic one.
//
// pg_class.reltuples is -1 for ANY relation nothing has analyzed, which
// includes every table that has never had a row inserted -- so "statistics are
// unusable" on its own marks a freshly created empty table as populated, and
// the concurrent build it then selects arrives as CREATE INDEX CONCURRENTLY in
// its own non-transactional migration file for a table with nothing in it. The
// storage the relation occupies is the fact that separates the two, because it
// is read from the file system rather than from statistics.
//
// The table carries a text column on purpose. That gives it a TOAST relation
// whose index occupies a page while the table itself occupies none, so
// pg_table_size -- the size function this reads as the obvious one -- reports
// 8192 for an empty table and cannot be the probe. The assertions below pin
// both numbers so the distinction cannot be quietly re-collapsed.
func TestGenerateMigration_EmptyNeverAnalyzedTableStaysTransactionalWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	adminURL := requireGeneratorPostgresURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	c.Assert(platform.NormalizeDialect(admin.Info().Dialect), qt.Equals, platform.Postgres)
	targetURL, targetDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_generator_empty_stats")
	defer dropGeneratorTestPostgres(c, admin, targetDatabase)
	target, err := dbschema.ConnectToDatabase(ctx, targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)

	_, err = target.ExecContext(ctx, `
		CREATE TABLE members (
			id BIGINT NOT NULL,
			email TEXT NOT NULL
		) WITH (autovacuum_enabled = false)
	`)
	c.Assert(err, qt.IsNil)

	// The precondition: the table is empty and nothing has analyzed it, so it
	// reports exactly the statistics a bulk-loaded table reports before its
	// first ANALYZE -- and its main fork, unlike pg_table_size, is empty too.
	var reltuples, liveTuples, mainFork, tableSize, actualRows int64
	c.Assert(target.QueryRowContext(ctx, `
		SELECT c.reltuples::bigint,
		       COALESCE(st.n_live_tup, 0),
		       pg_relation_size(c.oid),
		       pg_table_size(c.oid),
		       (SELECT COUNT(*) FROM members)
		FROM pg_class c
		LEFT JOIN pg_stat_all_tables st ON st.relid = c.oid
		WHERE c.relname = 'members'
	`).Scan(&reltuples, &liveTuples, &mainFork, &tableSize, &actualRows), qt.IsNil)
	c.Assert(reltuples, qt.Equals, int64(-1))
	c.Assert(liveTuples, qt.Equals, int64(0))
	c.Assert(actualRows, qt.Equals, int64(0))
	c.Assert(mainFork, qt.Equals, int64(0))
	c.Assert(tableSize > 0, qt.IsTrue, qt.Commentf("pg_table_size was %d", tableSize))

	dir := t.TempDir()
	entitiesDir := writeUnknownStatsIndexEntities(c, dir)
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
	c.Assert(files.Files[0].NoTransaction, qt.IsFalse)

	upSQL, err := os.ReadFile(files.Files[0].UpFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upSQL), qt.Contains, `CREATE INDEX IF NOT EXISTS "idx_members_email" ON "members" ("email");`)
	c.Assert(string(upSQL), qt.Not(qt.Contains), "CONCURRENTLY")
	c.Assert(string(upSQL), qt.Not(qt.Contains), "no_transaction")
	downSQL, err := os.ReadFile(files.Files[0].DownFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(downSQL), qt.Contains, `DROP INDEX IF EXISTS "idx_members_email";`)
	c.Assert(string(downSQL), qt.Not(qt.Contains), "CONCURRENTLY")

	provider, err := migrator.NewFSMigrationProvider(os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	migrations := migrator.NewMigrator(target, provider)
	c.Assert(migrations.MigrateUp(ctx), qt.IsNil)
	exists, valid := readGeneratorPostgresIndexState(c, target, "idx_members_email")
	c.Assert(exists, qt.IsTrue)
	c.Assert(valid, qt.IsTrue)

	c.Assert(migrations.MigrateDown(ctx), qt.IsNil)
	exists, _ = readGeneratorPostgresIndexState(c, target, "idx_members_email")
	c.Assert(exists, qt.IsFalse)

	c.Assert(migrations.MigrateUp(ctx), qt.IsNil)
	exists, valid = readGeneratorPostgresIndexState(c, target, "idx_members_email")
	c.Assert(exists, qt.IsTrue)
	c.Assert(valid, qt.IsTrue)
}

// setupUnknownRowStatistics populates a table and then leaves PostgreSQL with
// no usable statistics for it, deterministically.
//
// Everything runs on one pinned connection because the order matters:
// pg_stat_force_next_flush pushes the INSERT's counters out of this backend's
// local memory, and only then does pg_stat_reset have anything to clear that
// will stay cleared. Without the forced flush the pending counters are written
// back after the reset and the table reports 5000 live tuples again. Autovacuum
// is disabled on the table so nothing analyzes it behind the test's back.
func setupUnknownRowStatistics(c *qt.C, targetURL string) {
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
		`SELECT pg_stat_reset()`,
	} {
		_, err = conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement failed: %s", statement))
	}
}

func writePartitionedIndexEntities(c *qt.C, dir string) string {
	c.Helper()
	return writeGeneratorEntities(c, dir, `package entities

//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="id" type="BIGINT" not_null="true"
	ID int64

	//ptah:schema:field name="tenant" type="TEXT" not_null="true"
	//ptah:schema:index name="idx_events_tenant" fields="tenant"
	Tenant string

	//ptah:schema:field name="created_at" type="DATE" not_null="true"
	CreatedAt string
}

//ptah:schema:table name="events_2026"
type Event2026 struct {
	//ptah:schema:field name="id" type="BIGINT" not_null="true"
	ID int64

	//ptah:schema:field name="tenant" type="TEXT" not_null="true"
	Tenant string

	//ptah:schema:field name="created_at" type="DATE" not_null="true"
	CreatedAt string
}
`)
}

func writeUnknownStatsIndexEntities(c *qt.C, dir string) string {
	c.Helper()
	return writeGeneratorEntities(c, dir, `package entities

//ptah:schema:table name="members"
type Member struct {
	//ptah:schema:field name="id" type="BIGINT" not_null="true"
	ID int64

	//ptah:schema:field name="email" type="TEXT" not_null="true"
	//ptah:schema:index name="idx_members_email" fields="email"
	Email string
}
`)
}

func writeGeneratorEntities(c *qt.C, dir, content string) string {
	c.Helper()
	entitiesDir := filepath.Join(dir, "entities")
	c.Assert(os.MkdirAll(entitiesDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(entitiesDir, "schema.go"), []byte(content), 0o600), qt.IsNil)
	return entitiesDir
}
