package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestGenerateMigration_PartitionedParentShadowRoundTripWithRealPostgres runs
// the same generate/apply/generate loop through the surface a user reaches with
// --shadow-db, on a partitioned parent whose history the migration directory
// carries.
//
// The shadow database is dropped and the directory replayed onto it before
// anything is written, so the parent is created there by the plan itself. That
// is the only arrangement in which the shadow can say anything about a
// partitioned parent at all: the shadow starts empty, and a migration that only
// adds an index to a table the directory never created fails there with
// `relation "events" does not exist` whether or not the table is partitioned.
//
// The partition is attached between the cycles rather than created with the
// parent, because attaching an existing table is what makes PostgreSQL build
// the copy of the parent's index -- the object the second cycle used to plan a
// DROP for. See #997.
func TestGenerateMigration_PartitionedParentShadowRoundTripWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	adminURL := requireGeneratorPostgresURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	c.Assert(platform.NormalizeDialect(admin.Info().Dialect), qt.Equals, platform.Postgres)

	targetURL, targetDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_generator_partition_shadow_target")
	defer dropGeneratorTestPostgres(c, admin, targetDatabase)
	shadowURL, shadowDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_generator_partition_shadow_db")
	defer dropGeneratorTestPostgres(c, admin, shadowDatabase)

	target, err := dbschema.ConnectToDatabase(ctx, targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)

	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	desired := partitionedParentDesiredSchema()

	// Cycle 1: the parent, the table that becomes its partition, and the index
	// on the parent -- verified on the shadow before a byte is written.
	files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		Generated:         desired,
		DatabaseURL:       targetURL,
		ShadowDatabaseURL: shadowURL,
		MigrationName:     "create_partitioned_events",
		OutputDir:         migrationsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)
	c.Assert(files.Files, qt.HasLen, 1)
	upSQL, err := os.ReadFile(files.Files[0].UpFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upSQL), qt.Contains, `PARTITION BY RANGE ("created_at")`)

	provider, err := migrator.NewFSMigrationProvider(os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	c.Assert(migrator.NewMigrator(target, provider).MigrateUp(ctx), qt.IsNil)

	_, err = target.ExecContext(ctx, `
		ALTER TABLE events ATTACH PARTITION events_2026
			FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
		INSERT INTO events
			SELECT g, 'tenant-' || (g % 7), DATE '2026-01-01' + (g % 300)
			FROM generate_series(1, 5000) AS g;
		ANALYZE;
	`)
	c.Assert(err, qt.IsNil)
	c.Assert(
		readPartitionedIndexCatalog(c, target),
		qt.DeepEquals,
		[]partitionedIndexRow{
			{Name: "events_2026_tenant_idx", RelKind: "i", Valid: true, Ready: true, Attached: true},
			{Name: "idx_events_tenant", RelKind: "I", Valid: true, Ready: true, Attached: false},
		},
	)

	// Cycle 2 over the same surface: nothing left to plan, and the shadow is
	// never asked to judge a statement that would have failed on the target.
	second, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		Generated:         desired,
		DatabaseURL:       targetURL,
		ShadowDatabaseURL: shadowURL,
		MigrationName:     "second_cycle",
		OutputDir:         migrationsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(second, qt.IsNil)
	c.Assert(readGeneratedMigrationFilenames(c, migrationsDir), qt.HasLen, 2)

	replay, err := migrator.NewFSMigrationProvider(os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	c.Assert(migrator.NewMigrator(target, replay).MigrateUp(ctx), qt.IsNil)
	c.Assert(
		readPartitionedIndexCatalog(c, target),
		qt.DeepEquals,
		[]partitionedIndexRow{
			{Name: "events_2026_tenant_idx", RelKind: "i", Valid: true, Ready: true, Attached: true},
			{Name: "idx_events_tenant", RelKind: "I", Valid: true, Ready: true, Attached: false},
		},
	)
}

// partitionedParentDesiredSchema is the desired state both shadow cycles
// compare against: a partitioned parent, a plain table an operator attaches to
// it as a partition, and one index declared on the parent.
func partitionedParentDesiredSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{
				Name:       "events",
				StructName: "Event",
				Partition: &goschema.PartitionSpec{
					Type:  "RANGE",
					Parts: []goschema.PartitionPart{{Name: "created_at"}},
				},
			},
			{Name: "events_2026", StructName: "Event2026"},
		},
		Fields: []goschema.Field{
			{StructName: "Event", Name: "id", Type: "BIGINT"},
			{StructName: "Event", Name: "tenant", Type: "TEXT"},
			{StructName: "Event", Name: "created_at", Type: "DATE"},
			{StructName: "Event2026", Name: "id", Type: "BIGINT"},
			{StructName: "Event2026", Name: "tenant", Type: "TEXT"},
			{StructName: "Event2026", Name: "created_at", Type: "DATE"},
		},
		Indexes: []goschema.Index{
			{Name: "idx_events_tenant", StructName: "Event", Fields: []string{"tenant"}},
		},
	}
}
