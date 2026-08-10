//go:build ptah_live_generator

package generator_test

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestGenerateMigration_PartitionedParentSurvivesASecondCycleWithRealPostgres
// runs the loop a single round trip cannot see: generate, apply, and generate
// again against the state the first cycle produced.
//
// Creating an index on a partitioned parent makes PostgreSQL create one copy of
// it per partition, named by the server (events_2026_tenant_idx). Those copies
// do not exist when the first migration is generated, so the first cycle is
// clean; they exist by the time the next generate introspects, and a desired
// state written against the parent never names them. Read as ordinary indexes
// they are removals, and PostgreSQL 17.10 answers the DROP with
// `cannot drop index events_2026_tenant_idx because index idx_events_tenant
// requires it (SQLSTATE 2BP01)` -- at execution time, after the file, its
// checksum and its commit exist. See #997.
//
// Every cycle asserts pg_index rather than the generated text: the parent index
// is relkind 'I' and the copy is relkind 'i', and both are indisvalid and
// indisready here, which is what separates them from the failed concurrent
// build #1101 is about.
func TestGenerateMigration_PartitionedParentSurvivesASecondCycleWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	adminURL := requireGeneratorPostgresURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	c.Assert(platform.NormalizeDialect(admin.Info().Dialect), qt.Equals, platform.Postgres)
	targetURL, targetDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_generator_partition_cycle")
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

	dir := t.TempDir()
	entitiesDir := writePartitionedIndexEntities(c, dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)

	// Cycle 1: the parent index is planned and applied.
	files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesDir: entitiesDir,
		DatabaseURL:   targetURL,
		MigrationName: "add_events_tenant_index",
		OutputDir:     migrationsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)

	provider, err := migrator.NewFSMigrationProvider(os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	migrations := migrator.NewMigrator(target, provider)
	c.Assert(migrations.MigrateUp(ctx), qt.IsNil)

	// The state the second cycle has to survive: the parent index and the copy
	// the server created for the partition, both usable.
	c.Assert(
		readPartitionedIndexCatalog(c, target),
		qt.DeepEquals,
		[]partitionedIndexRow{
			{Name: "events_2026_tenant_idx", RelKind: "i", Valid: true, Ready: true, Attached: true},
			{Name: "idx_events_tenant", RelKind: "I", Valid: true, Ready: true, Attached: false},
		},
	)

	// Cycle 2: nothing is left to plan. Before #997 this published
	// DROP INDEX IF EXISTS "events_2026_tenant_idx", which the server refuses.
	second, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesDir: entitiesDir,
		DatabaseURL:   targetURL,
		MigrationName: "second_cycle",
		OutputDir:     migrationsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(second, qt.IsNil)
	c.Assert(readGeneratedMigrationFilenames(c, migrationsDir), qt.HasLen, 2)

	// Whatever the second cycle decided, the directory still applies: a no-op
	// run is only worth something if the run itself succeeds.
	replayProvider, err := migrator.NewFSMigrationProvider(os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	c.Assert(migrator.NewMigrator(target, replayProvider).MigrateUp(ctx), qt.IsNil)
	c.Assert(
		readPartitionedIndexCatalog(c, target),
		qt.DeepEquals,
		[]partitionedIndexRow{
			{Name: "events_2026_tenant_idx", RelKind: "i", Valid: true, Ready: true, Attached: true},
			{Name: "idx_events_tenant", RelKind: "I", Valid: true, Ready: true, Attached: false},
		},
	)

	// The copy is skipped because the catalog says it is attached, not because
	// indexes on partitions stopped being managed. An index created on the
	// partition directly is still planned for removal, and the server accepts
	// that drop -- these two indexes are the pair that separates the fact from
	// every cheaper guess about it.
	_, err = target.ExecContext(ctx, `CREATE INDEX events_2026_id_idx ON events_2026 (id)`)
	c.Assert(err, qt.IsNil)
	third, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesDir: entitiesDir,
		DatabaseURL:   targetURL,
		MigrationName: "third_cycle",
		OutputDir:     migrationsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(third, qt.IsNotNil)
	c.Assert(third.Files, qt.HasLen, 1)
	thirdUp, err := os.ReadFile(third.Files[0].UpFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(thirdUp), qt.Contains, `DROP INDEX IF EXISTS "events_2026_id_idx";`)
	c.Assert(string(thirdUp), qt.Not(qt.Contains), "events_2026_tenant_idx")

	thirdProvider, err := migrator.NewFSMigrationProvider(os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	c.Assert(migrator.NewMigrator(target, thirdProvider).MigrateUp(ctx), qt.IsNil)
	c.Assert(
		readPartitionedIndexCatalog(c, target),
		qt.DeepEquals,
		[]partitionedIndexRow{
			{Name: "events_2026_tenant_idx", RelKind: "i", Valid: true, Ready: true, Attached: true},
			{Name: "idx_events_tenant", RelKind: "I", Valid: true, Ready: true, Attached: false},
		},
	)
}

// TestReadSchema_PartitionAttachedIndexIsMarkedWithRealPostgres pins what the
// reader reports, on the one fixture pair where a cheaper rule and the catalog
// fact disagree in both directions.
//
// A copy attached under a name of the operator's own choosing
// (my_local_created) is not droppable; a standalone index carrying the name
// PostgreSQL would have generated for a copy (events_2026_id_idx) is. Neither
// the name nor the table separates them -- pg_inherits over the INDEX relation
// does. Three things this fixture is built to refuse:
//
//   - The naming convention. events_2026_id_idx carries the name a copy would
//     have and is droppable; my_local_created is a copy and is not.
//   - relkind. It marks the parent ('I') and leaves the copy ('i'), and two of
//     the three droppable indexes here are 'I' while the third is 'i'.
//   - The partition's own attachment. pg_inherits over the TABLE relation is
//     true for every index on events_2026, which is three of these five.
//
// The fixture is read TWICE, before and after the ATTACH, because the two
// halves of it are the same object in different states and a single reading
// cannot tell a rule that answers the state from one that answers the name. It
// also records what the catalog cannot hold at once: attaching the only
// partition's index to idx_events_created is exactly what makes that parent
// indisvalid, so "my_local_created is attached" and "idx_events_created is not
// yet valid" are states of two different moments, never of one.
func TestReadSchema_PartitionAttachedIndexIsMarkedWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	adminURL := requireGeneratorPostgresURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	c.Assert(platform.NormalizeDialect(admin.Info().Dialect), qt.Equals, platform.Postgres)
	targetURL, targetDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_reader_partition_index")
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
		CREATE INDEX idx_events_tenant ON events (tenant);
		CREATE INDEX events_2026_id_idx ON events_2026 (id);
		CREATE INDEX my_local_created ON events_2026 (created_at);
		CREATE INDEX idx_events_created ON ONLY events (created_at);
	`)
	c.Assert(err, qt.IsNil)

	// Before the ATTACH. my_local_created is an ordinary index that happens to
	// live on a partition, and idx_events_created is a parent with no child
	// yet, so it is not valid. A rule keyed on the partition's own attachment
	// marks my_local_created here, and this is where it is wrong.
	c.Assert(
		readPartitionedIndexCatalog(c, target),
		qt.DeepEquals,
		[]partitionedIndexRow{
			{Name: "events_2026_id_idx", RelKind: "i", Valid: true, Ready: true, Attached: false},
			{Name: "events_2026_tenant_idx", RelKind: "i", Valid: true, Ready: true, Attached: true},
			{Name: "idx_events_created", RelKind: "I", Valid: false, Ready: true, Attached: false},
			{Name: "idx_events_tenant", RelKind: "I", Valid: true, Ready: true, Attached: false},
			{Name: "my_local_created", RelKind: "i", Valid: true, Ready: true, Attached: false},
		},
	)
	c.Assert(readPartitionAttachedMarks(c, target), qt.DeepEquals, map[string]bool{
		"idx_events_tenant":      false,
		"idx_events_created":     false,
		"events_2026_tenant_idx": true,
		"events_2026_id_idx":     false,
		"my_local_created":       false,
	})

	_, err = target.ExecContext(ctx, `ALTER INDEX idx_events_created ATTACH PARTITION my_local_created`)
	c.Assert(err, qt.IsNil)

	// After the ATTACH. my_local_created became a copy without being touched,
	// and idx_events_created became valid without being touched: the single
	// partition now has an attached index, which is the condition PostgreSQL
	// validates the parent on.
	c.Assert(
		readPartitionedIndexCatalog(c, target),
		qt.DeepEquals,
		[]partitionedIndexRow{
			{Name: "events_2026_id_idx", RelKind: "i", Valid: true, Ready: true, Attached: false},
			{Name: "events_2026_tenant_idx", RelKind: "i", Valid: true, Ready: true, Attached: true},
			{Name: "idx_events_created", RelKind: "I", Valid: true, Ready: true, Attached: false},
			{Name: "idx_events_tenant", RelKind: "I", Valid: true, Ready: true, Attached: false},
			{Name: "my_local_created", RelKind: "i", Valid: true, Ready: true, Attached: true},
		},
	)
	attached := readPartitionAttachedMarks(c, target)
	c.Assert(attached, qt.DeepEquals, map[string]bool{
		"idx_events_tenant":      false,
		"idx_events_created":     false,
		"events_2026_tenant_idx": true,
		"events_2026_id_idx":     false,
		"my_local_created":       true,
	})

	// The reader's mark and the server's refusal name the same objects, over
	// the WHOLE fixture rather than over a chosen pair. Three of these five
	// indexes accept a DROP INDEX and two refuse it, and the two that refuse
	// are exactly the two the reader marks.
	c.Assert(refusedDropIndex(c, target, indexNames(attached)), qt.DeepEquals, attached)

	// The refusal is the one #997 is about, quoted rather than counted.
	_, err = target.ExecContext(ctx, `DROP INDEX "events_2026_id_idx"`)
	c.Assert(err, qt.IsNil)
	_, err = target.ExecContext(ctx, `DROP INDEX "my_local_created"`)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "cannot drop index my_local_created")
}

// readPartitionAttachedMarks reports PartitionAttached per index name, as the
// reader under test describes the live database.
func readPartitionAttachedMarks(c *qt.C, conn *dbschema.DatabaseConnection) map[string]bool {
	c.Helper()
	schema, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	marks := make(map[string]bool, len(schema.Indexes))
	for _, index := range schema.Indexes {
		marks[index.Name] = index.PartitionAttached
	}
	return marks
}

// indexNames lists the keys of a per-index map in sorted order, so the set the
// server is asked about is the set the reader described rather than a list
// written out by hand beside it.
func indexNames(marks map[string]bool) []string {
	names := make([]string, 0, len(marks))
	for name := range marks {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// refusedDropIndex reports, per index name, whether PostgreSQL refuses a
// DROP INDEX that names it.
//
// Each attempt runs in a transaction of its own and is rolled back, so every
// name is measured against the same catalog the reader was asked about --
// dropping one index for real would change what the next attempt means, and
// dropping a parent takes its copies with it.
func refusedDropIndex(c *qt.C, conn *dbschema.DatabaseConnection, names []string) map[string]bool {
	c.Helper()
	session, err := conn.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(session.Close(), qt.IsNil) }()

	refused := make(map[string]bool, len(names))
	for _, name := range names {
		transaction, beginErr := session.BeginTx(c.Context(), nil)
		c.Assert(beginErr, qt.IsNil)
		_, dropErr := transaction.ExecContext(c.Context(), `DROP INDEX "`+name+`"`)
		refused[name] = dropErr != nil
		c.Assert(transaction.Rollback(), qt.IsNil)
	}
	return refused
}

// partitionedIndexRow is one pg_index observation, named so a failure reads as
// the catalog state it is rather than as a tuple of booleans.
type partitionedIndexRow struct {
	Name     string
	RelKind  string
	Valid    bool
	Ready    bool
	Attached bool
}

// readPartitionedIndexCatalog reports every index on the partitioned parent and
// its partition, in name order.
//
// indisvalid and indisready are read alongside relkind because they are what
// separates the shapes a probe must not confuse: a partition's copy is 'i', a
// parent index is 'I', a parent built with CREATE INDEX ... ON ONLY is 'I' with
// indisvalid false and indisready true, and a failed concurrent build is 'i'
// with both false.
func readPartitionedIndexCatalog(c *qt.C, conn *dbschema.DatabaseConnection) []partitionedIndexRow {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), `
		SELECT class.relname,
		       class.relkind::text,
		       index.indisvalid,
		       index.indisready,
		       EXISTS (SELECT 1 FROM pg_inherits inh WHERE inh.inhrelid = class.oid)
		FROM pg_index AS index
		JOIN pg_class AS class ON class.oid = index.indexrelid
		JOIN pg_class AS parent ON parent.oid = index.indrelid
		JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = current_schema()
		  AND parent.relname IN ('events', 'events_2026')
		ORDER BY class.relname
	`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()

	var observed []partitionedIndexRow
	for rows.Next() {
		var row partitionedIndexRow
		c.Assert(rows.Scan(&row.Name, &row.RelKind, &row.Valid, &row.Ready, &row.Attached), qt.IsNil)
		observed = append(observed, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return observed
}

// readGeneratedMigrationFilenames lists the migration directory so a "nothing
// was generated" claim is measured against the directory rather than against
// the return value alone.
func readGeneratedMigrationFilenames(c *qt.C, dir string) []string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}
