//go:build integration

package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/generator"
)

// TestGenerateMigration_PartitionedParentRefusesRequestedConcurrentIndexBeforePublicationWithRealPostgres
// pins the refusal half of acceptance line 3 of stokaro/ptah#997 -- the half
// that fires when a project explicitly asks for concurrent builds and the
// index it asks for names a partitioned parent.
//
// The unit tests for this rule hand it a DBSchema with Partitioned set by the
// test, which proves the rule and nothing about the read that feeds it.
// information_schema.tables reports a declaratively partitioned parent as an
// ordinary BASE TABLE, so pg_class.relkind is the only place the distinction
// survives, and only a live parent can show that the value travels from the
// catalog through the reader into the decision. The relkind assertion below is
// the precondition that makes the refusal mean something.
//
// "Before publication" is the other half, and it is asserted as an absence:
// the output directory must still be empty. PostgreSQL answers a concurrent
// index statement on relkind 'p' with SQLSTATE 0A000 at execution time, so a
// refusal that arrived after the write would leave a migration file, its
// checksum and a commit behind, and the failure would land on a production
// database instead of on the developer who generated it.
func TestGenerateMigration_PartitionedParentRefusesRequestedConcurrentIndexBeforePublicationWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	adminURL := requireGeneratorPostgresURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	c.Assert(platform.NormalizeDialect(admin.Info().Dialect), qt.Equals, platform.Postgres)
	targetURL, targetDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_generator_partition_refusal")
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

	// The precondition the refusal rests on, read from the catalog rather than
	// from information_schema, which flattens 'p' to BASE TABLE.
	var relkind string
	c.Assert(target.QueryRowContext(ctx, `
		SELECT relkind::text FROM pg_class WHERE relname = 'events'
	`).Scan(&relkind), qt.IsNil)
	c.Assert(relkind, qt.Equals, "p")

	dir := t.TempDir()
	entitiesDir := writePartitionedIndexEntities(c, dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)

	files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesDir: entitiesDir,
		DatabaseURL:   targetURL,
		MigrationName: "add_events_tenant_index",
		OutputDir:     migrationsDir,
		DiffPolicy:    generator.DiffPolicy{ConcurrentIndex: true},
	})

	c.Assert(files, qt.IsNil)
	c.Assert(err, qt.ErrorMatches,
		`CREATE INDEX CONCURRENTLY requested by diff\.concurrent_index\.create cannot be generated `+
			`for partitioned table\(s\): "idx_events_tenant" on "events"; .*`)
	c.Assert(err.Error(), qt.Contains, "SQLSTATE 0A000")
	c.Assert(err.Error(), qt.Contains, "ALTER INDEX ... ATTACH PARTITION")

	// Nothing was published: no migration file, no checksum, nothing.
	entries, err := os.ReadDir(migrationsDir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}
