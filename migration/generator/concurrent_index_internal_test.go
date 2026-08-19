package generator

// White-box testing required: these tests exercise the internal split between
// transactional and concurrent-index plans before publication. The exported
// generation and rollback path is covered in shadow_postgres_live_test.go.

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestPlanGeneratedMigrationSpecs_ConcurrentIndexForPopulatedPostgresTable(t *testing.T) {
	c := qt.New(t)

	specs, assessments, err := planGeneratedMigrationSpecs(
		indexOnlyDiff(),
		indexOnlyGeneratedSchema(),
		&dbschematypes.DBSchema{Tables: []dbschematypes.DBTable{{Name: "users", Type: "BASE TABLE", EstimatedRows: 10}}},
		postgresInfo(capability.Postgres16()),
		100,
		"add_user_email_index",
		DiffPolicy{},
		atlasmigrate.Qualifier{},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 1)
	c.Assert(len(assessments) > 0, qt.IsTrue)
	c.Assert(specs[0].NoTransaction, qt.IsTrue)
	c.Assert(specs[0].UpSQL, qt.Contains, "-- +ptah no_transaction")
	c.Assert(specs[0].UpSQL, qt.Contains, `CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_users_email" ON "users" ("email");`)
	c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "+ptah lock_timeout")
	c.Assert(specs[0].DownSQL, qt.Contains, "-- +ptah no_transaction")
	// Reverting the concurrent-drop wiring prints
	// `DROP INDEX IF EXISTS "idx_users_email";` here: the rollback of a
	// non-blocking build would take the very write lock the build avoided.
	c.Assert(specs[0].DownSQL, qt.Contains, `DROP INDEX CONCURRENTLY IF EXISTS "idx_users_email";`)
}

func TestPlanGeneratedMigrationSpecs_ReverseOnlyNoTransactionMarksPair(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{EnumsModified: []types.EnumDiff{{
		EnumName: "status", ValuesRemoved: []string{"retired"},
	}}}
	desired := &goschema.Database{Enums: []goschema.Enum{{
		Name: "status", Values: []string{"active"},
	}}}
	current := &dbschematypes.DBSchema{Enums: []dbschematypes.DBEnum{{
		Name: "status", Values: []string{"active", "retired"},
	}}}

	specs, _, err := planGeneratedMigrationSpecs(
		diff,
		desired,
		current,
		postgresInfo(capability.Postgres17()),
		100,
		"remove_retired_status",
		DiffPolicy{},
		atlasmigrate.Qualifier{},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 1)
	c.Assert(specs[0].NoTransaction, qt.IsTrue)
	c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "-- +ptah no_transaction")
	c.Assert(specs[0].DownSQL, qt.Contains, "-- +ptah no_transaction")
	c.Assert(specs[0].DownSQL, qt.Contains, `ALTER TYPE "status" ADD VALUE 'retired'`)
}

func TestPlanGeneratedMigrationSpecs_YugabyteConcurrentCreateUsesBlockingRollback(t *testing.T) {
	c := qt.New(t)

	specs, assessments, err := planGeneratedMigrationSpecs(
		indexOnlyDiff(),
		indexOnlyGeneratedSchema(),
		&dbschematypes.DBSchema{Tables: []dbschematypes.DBTable{{Name: "users", Type: "BASE TABLE", EstimatedRows: 10}}},
		dbschematypes.DBInfo{
			Dialect:      platform.YugabyteDB,
			Capabilities: capability.YugabyteDB25(),
		},
		100,
		"add_user_email_index",
		DiffPolicy{},
		atlasmigrate.Qualifier{},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 1)
	c.Assert(len(assessments) > 0, qt.IsTrue)
	c.Assert(specs[0].NoTransaction, qt.IsTrue)
	c.Assert(specs[0].UpSQL, qt.Contains, "-- +ptah no_transaction")
	c.Assert(specs[0].UpSQL, qt.Contains,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_users_email" ON "users" ("email");`)
	c.Assert(specs[0].DownSQL, qt.Not(qt.Contains), "-- +ptah no_transaction")
	c.Assert(specs[0].DownSQL, qt.Contains, `DROP INDEX IF EXISTS "idx_users_email";`)
	c.Assert(specs[0].DownSQL, qt.Not(qt.Contains), "CONCURRENTLY")
}

func TestPlanGeneratedMigrationSpecs_ConcurrentIndexRequiresPopulatedCapablePostgres(t *testing.T) {
	tests := []struct {
		name     string
		dbSchema *dbschematypes.DBSchema
		info     dbschematypes.DBInfo
	}{
		{
			name:     "empty table stays transactional",
			dbSchema: &dbschematypes.DBSchema{Tables: []dbschematypes.DBTable{{Name: "users", Type: "BASE TABLE"}}},
			info:     postgresInfo(capability.Postgres16()),
		},
		{
			name:     "missing table stats stays transactional",
			dbSchema: &dbschematypes.DBSchema{},
			info:     postgresInfo(capability.Postgres16()),
		},
		{
			name:     "capability-disabled postgres family stays transactional",
			dbSchema: &dbschematypes.DBSchema{Tables: []dbschematypes.DBTable{{Name: "users", Type: "BASE TABLE", EstimatedRows: 10}}},
			info:     postgresInfo(capability.Postgres16().With(capability.CreateIndexConcurrently, false)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			specs, _, err := planGeneratedMigrationSpecs(indexOnlyDiff(), indexOnlyGeneratedSchema(), tt.dbSchema, tt.info, 100, "add_index", DiffPolicy{}, atlasmigrate.Qualifier{})

			c.Assert(err, qt.IsNil)
			c.Assert(specs, qt.HasLen, 1)
			c.Assert(specs[0].NoTransaction, qt.IsFalse)
			c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "CONCURRENTLY")
			c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "-- +ptah no_transaction")
			c.Assert(specs[0].UpSQL, qt.Contains, `CREATE INDEX IF NOT EXISTS "idx_users_email" ON "users" ("email");`)
		})
	}
}

func TestPlanGeneratedMigrationSpecs_SplitsTransactionalAndConcurrentIndex(t *testing.T) {
	c := qt.New(t)

	diff := indexOnlyDiff()
	diff.TablesAdded = []string{"posts"}
	generated := indexOnlyGeneratedSchema()
	generated.Tables = append(generated.Tables, goschema.Table{StructName: "Post", Name: "posts"})
	generated.Fields = append(generated.Fields, goschema.Field{
		StructName: "Post",
		Name:       "id",
		Type:       "SERIAL",
		Primary:    true,
		AutoInc:    true,
	})

	specs, _, err := planGeneratedMigrationSpecs(
		diff,
		generated,
		&dbschematypes.DBSchema{Tables: []dbschematypes.DBTable{{Name: "users", Type: "BASE TABLE", EstimatedRows: 10}}},
		postgresInfo(capability.Postgres16()),
		100,
		"add_posts_and_user_index",
		DiffPolicy{},
		atlasmigrate.Qualifier{},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 2)
	c.Assert(specs[0].Version, qt.Equals, int64(100))
	c.Assert(specs[0].Name, qt.Equals, "add_posts_and_user_index_transactional")
	c.Assert(specs[0].NoTransaction, qt.IsFalse)
	c.Assert(specs[0].UpSQL, qt.Contains, `CREATE TABLE "posts"`)
	c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "idx_users_email")

	c.Assert(specs[1].Version, qt.Equals, int64(101))
	c.Assert(specs[1].Name, qt.Equals, "add_posts_and_user_index_concurrent_indexes")
	c.Assert(specs[1].NoTransaction, qt.IsTrue)
	c.Assert(specs[1].UpSQL, qt.Contains, "-- +ptah no_transaction")
	c.Assert(specs[1].UpSQL, qt.Contains, `CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_users_email" ON "users" ("email");`)
	// Reverting the concurrent-drop wiring prints
	// `DROP INDEX IF EXISTS "idx_users_email";` here.
	c.Assert(specs[1].DownSQL, qt.Contains, `DROP INDEX CONCURRENTLY IF EXISTS "idx_users_email";`)
}

func TestPlanGeneratedMigrationSpecs_SplitsPopulatedAndEmptyTableIndexes(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{IndexesAdded: []types.IndexRef{
		{Name: "idx_users_email", TableName: "users"},
		{Name: "idx_posts_title", TableName: "posts"},
	}}
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Post", Name: "posts"},
		},
		Indexes: []goschema.Index{
			{Name: "idx_users_email", StructName: "User", Fields: []string{"email"}},
			{Name: "idx_posts_title", StructName: "Post", Fields: []string{"title"}},
		},
	}
	dbSchema := &dbschematypes.DBSchema{Tables: []dbschematypes.DBTable{
		{Name: "users", Type: "BASE TABLE", EstimatedRows: 10},
		{Name: "posts", Type: "BASE TABLE", EstimatedRows: 0},
	}}

	specs, _, err := planGeneratedMigrationSpecs(diff, generated, dbSchema, postgresInfo(capability.Postgres16()), 100, "add_indexes", DiffPolicy{}, atlasmigrate.Qualifier{})

	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 2)
	c.Assert(specs[0].NoTransaction, qt.IsFalse)
	c.Assert(specs[0].UpSQL, qt.Contains, `CREATE INDEX IF NOT EXISTS "idx_posts_title" ON "posts" ("title");`)
	c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "idx_users_email")
	c.Assert(specs[1].NoTransaction, qt.IsTrue)
	c.Assert(specs[1].UpSQL, qt.Contains, `CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_users_email" ON "users" ("email");`)
	c.Assert(specs[1].UpSQL, qt.Not(qt.Contains), "idx_posts_title")
}

// TestPlanGeneratedMigrationSpecs_LeadsWithTheEnumValueAddition covers
// stokaro/ptah#1714.
//
// This exact diff used to produce NO migration at all: a PostgreSQL enum value
// addition beside a table change was answered "mixes transactional statements
// with non-transactional statements that cannot be split automatically", for a
// reason about transactionality the user did not choose and cannot see in their
// schema.
//
// The order is the assertion, not just the count. `ALTER TYPE ... ADD VALUE`
// has to be committed BEFORE any statement that uses the value -- PostgreSQL
// answers 55P04 otherwise -- so the enum file LEADS, which is the opposite of
// where the concurrent-index file goes. Emitting the two in the other order
// would still produce two files and still pass a count-only test.
func TestPlanGeneratedMigrationSpecs_LeadsWithTheEnumValueAddition(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesAdded: []string{"users"},
		EnumsModified: []types.EnumDiff{{
			EnumName:    "status",
			ValuesAdded: []string{"archived"},
		}},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{{
			StructName: "User",
			Name:       "id",
			Type:       "SERIAL",
			Primary:    true,
			AutoInc:    true,
		}},
		Enums: []goschema.Enum{{Name: "status", Values: []string{"active", "archived"}}},
	}

	specs, _, err := planGeneratedMigrationSpecs(diff, generated, &dbschematypes.DBSchema{}, postgresInfo(capability.Postgres16()), 100, "mixed", DiffPolicy{}, atlasmigrate.Qualifier{})

	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 2)

	c.Assert(specs[0].NoTransaction, qt.IsTrue)
	c.Assert(specs[0].Version, qt.Equals, int64(100))
	c.Assert(specs[0].UpSQL, qt.Contains, `ALTER TYPE "status" ADD VALUE 'archived';`)
	c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "CREATE TABLE")

	c.Assert(specs[1].NoTransaction, qt.IsFalse)
	c.Assert(specs[1].Version, qt.Equals, int64(101))
	c.Assert(specs[1].UpSQL, qt.Contains, `CREATE TABLE "users"`)
	c.Assert(specs[1].UpSQL, qt.Not(qt.Contains), "ADD VALUE")
}

func TestPublishPlannedMigration_WritesAllPairs(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	var files *MigrationFiles
	var publishErr error
	err := atlasmigrate.WithMigrationDirectoryLock(t.Context(), dir, 0, func(context.Context) error {
		writer, bindErr := bindMigrationOutputDir(nil, dir)
		c.Assert(bindErr, qt.IsNil)
		defer func() { _ = writer.Close() }()
		files, publishErr = publishPlannedMigration(t.Context(), writer, "", []generatedMigrationSpec{
			{Version: 100, Name: "transactional", UpSQL: "SELECT 1;\n", DownSQL: "SELECT 2;\n"},
			{Version: 101, Name: "concurrent_indexes", UpSQL: "-- +ptah no_transaction\nSELECT 3;\n", DownSQL: "-- +ptah no_transaction\nSELECT 4;\n", NoTransaction: true},
		})
		return publishErr
	})

	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 2)
	c.Assert(files.Files[0].NoTransaction, qt.IsFalse)
	c.Assert(files.Files[1].NoTransaction, qt.IsTrue)
	c.Assert(files.Files[0].Version < files.Files[1].Version, qt.IsTrue)
	c.Assert(strings.HasSuffix(files.Files[0].UpFile, "100_transactional.up.sql"), qt.IsTrue)
	c.Assert(strings.HasSuffix(files.Files[1].UpFile, "101_concurrent_indexes.up.sql"), qt.IsTrue)
}

// TestPlanGeneratedMigrationSpecs_ConcurrentIndexDropPolicy pins the UP
// direction of the drop policy: a standalone index removal becomes
// non-blocking only when the project asks for it, and the resulting statement
// is routed into its own no_transaction migration because PostgreSQL rejects
// DROP INDEX CONCURRENTLY inside a transaction block.
//
// With the change reverted the "policy on" row prints
// `DROP INDEX IF EXISTS "idx_users_email";` with NoTransaction false, so it
// becomes indistinguishable from the "policy off" row.
func TestPlanGeneratedMigrationSpecs_ConcurrentIndexDropPolicy(t *testing.T) {
	tests := []struct {
		name              string
		policy            DiffPolicy
		info              dbschematypes.DBInfo
		wantNoTransaction bool
		wantUpSQL         string
		wantDownSQL       string
	}{
		{
			name:              "policy off keeps the blocking drop",
			policy:            DiffPolicy{},
			info:              postgresInfo(capability.Postgres16()),
			wantNoTransaction: false,
			wantUpSQL:         `DROP INDEX IF EXISTS "idx_users_email";`,
			wantDownSQL:       `CREATE INDEX IF NOT EXISTS "idx_users_email" ON "users" ("email");`,
		},
		{
			name:              "policy on drops concurrently and rebuilds concurrently",
			policy:            DiffPolicy{ConcurrentIndexDrop: true},
			info:              postgresInfo(capability.Postgres16()),
			wantNoTransaction: true,
			wantUpSQL:         `DROP INDEX CONCURRENTLY IF EXISTS "idx_users_email";`,
			wantDownSQL:       `CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_users_email" ON "users" ("email");`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			specs, _, err := planGeneratedMigrationSpecs(
				indexRemovalOnlyDiff(),
				indexOnlyGeneratedSchema(),
				indexRemovalDBSchema(),
				tt.info,
				100,
				"drop_user_email_index",
				tt.policy,
				atlasmigrate.Qualifier{},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(specs, qt.HasLen, 1)
			c.Assert(specs[0].NoTransaction, qt.Equals, tt.wantNoTransaction)
			c.Assert(specs[0].UpSQL, qt.Contains, tt.wantUpSQL)
			c.Assert(specs[0].DownSQL, qt.Contains, tt.wantDownSQL)
		})
	}
}

func TestPlanGeneratedMigrationSpecs_ConcurrentIndexDropRequiresCapability(t *testing.T) {
	c := qt.New(t)

	specs, _, err := planGeneratedMigrationSpecs(
		indexRemovalOnlyDiff(),
		indexOnlyGeneratedSchema(),
		indexRemovalDBSchema(),
		postgresInfo(capability.Postgres16().With(capability.DropIndexConcurrently, false)),
		100,
		"drop_user_email_index",
		DiffPolicy{ConcurrentIndexDrop: true},
		atlasmigrate.Qualifier{},
	)

	c.Assert(err, qt.ErrorMatches, `DROP INDEX CONCURRENTLY requested by diff\.concurrent_index\.drop cannot be generated for dialect "postgres": target capability drop_index_concurrently is unavailable`)
	c.Assert(specs, qt.HasLen, 0)
}

// TestPlanGeneratedMigrationSpecs_ConcurrentIndexDropSplitsFromTransactional
// pins the file split: a concurrent drop cannot share a file with a statement
// that needs the transaction, so it gets its own migration.
//
// With the change reverted this test fails at the first assertion with
// "specs has len 1, want 2" — the drop stays blocking and everything fits in a
// single transactional migration.
func TestPlanGeneratedMigrationSpecs_ConcurrentIndexDropSplitsFromTransactional(t *testing.T) {
	c := qt.New(t)

	diff := indexRemovalOnlyDiff()
	diff.TablesAdded = []string{"posts"}
	generated := indexOnlyGeneratedSchema()
	generated.Tables = append(generated.Tables, goschema.Table{StructName: "Post", Name: "posts"})
	generated.Fields = append(generated.Fields, goschema.Field{
		StructName: "Post",
		Name:       "id",
		Type:       "SERIAL",
		Primary:    true,
		AutoInc:    true,
	})

	specs, _, err := planGeneratedMigrationSpecs(
		diff,
		generated,
		indexRemovalDBSchema(),
		postgresInfo(capability.Postgres16()),
		100,
		"add_posts_drop_index",
		DiffPolicy{ConcurrentIndexDrop: true},
		atlasmigrate.Qualifier{},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 2)
	c.Assert(specs[0].Name, qt.Equals, "add_posts_drop_index_transactional")
	c.Assert(specs[0].NoTransaction, qt.IsFalse)
	c.Assert(specs[0].UpSQL, qt.Contains, `CREATE TABLE "posts"`)
	c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "idx_users_email")

	c.Assert(specs[1].Name, qt.Equals, "add_posts_drop_index_concurrent_indexes")
	c.Assert(specs[1].NoTransaction, qt.IsTrue)
	c.Assert(specs[1].UpSQL, qt.Contains, "-- +ptah no_transaction")
	c.Assert(specs[1].UpSQL, qt.Contains, `DROP INDEX CONCURRENTLY IF EXISTS "idx_users_email";`)
	c.Assert(specs[1].UpSQL, qt.Not(qt.Contains), `CREATE TABLE "posts"`)
}

// indexRemovalDBSchema is the pre-change database state a removal is planned
// against: the index the migration drops still exists, so the down direction
// can rebuild it.
func indexRemovalDBSchema() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{{
			Name:          "users",
			Type:          "BASE TABLE",
			EstimatedRows: 10,
			Columns: []dbschematypes.DBColumn{
				{Name: "email", DataType: "text", UDTName: "text", IsNullable: "YES", OrdinalPosition: 1},
			},
		}},
		Indexes: []dbschematypes.DBIndex{{
			Name:      "idx_users_email",
			TableName: "users",
			Columns:   []string{"email"},
		}},
	}
}

func indexRemovalOnlyDiff() *types.SchemaDiff {
	return &types.SchemaDiff{IndexesRemoved: []types.IndexRef{
		{Name: "idx_users_email", TableName: "users"},
	}}
}

func indexOnlyDiff() *types.SchemaDiff {
	return &types.SchemaDiff{IndexesAdded: []types.IndexRef{
		{Name: "idx_users_email", TableName: "users"},
	}}
}

func indexOnlyGeneratedSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Indexes: []goschema.Index{
			{Name: "idx_users_email", StructName: "User", Fields: []string{"email"}},
		},
	}
}

func postgresInfo(caps capability.Capabilities) dbschematypes.DBInfo {
	return dbschematypes.DBInfo{
		Dialect:      platform.Postgres,
		Capabilities: caps,
	}
}
