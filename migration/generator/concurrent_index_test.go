package generator

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/types"
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
	)

	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 1)
	c.Assert(len(assessments) > 0, qt.IsTrue)
	c.Assert(specs[0].NoTransaction, qt.IsTrue)
	c.Assert(specs[0].UpSQL, qt.Contains, "-- +ptah no_transaction")
	c.Assert(specs[0].UpSQL, qt.Contains, `CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_users_email" ON "users" ("email");`)
	c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "+ptah lock_timeout")
	c.Assert(specs[0].DownSQL, qt.Contains, "-- +ptah no_transaction")
	c.Assert(specs[0].DownSQL, qt.Contains, `DROP INDEX IF EXISTS "idx_users_email";`)
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
		{
			name:     "yugabyte stays transactional",
			dbSchema: &dbschematypes.DBSchema{Tables: []dbschematypes.DBTable{{Name: "users", Type: "BASE TABLE", EstimatedRows: 10}}},
			info: dbschematypes.DBInfo{
				Dialect:      platform.YugabyteDB,
				Capabilities: capability.YugabyteDB25(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			specs, _, err := planGeneratedMigrationSpecs(indexOnlyDiff(), indexOnlyGeneratedSchema(), tt.dbSchema, tt.info, 100, "add_index")

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
	c.Assert(specs[1].DownSQL, qt.Contains, `DROP INDEX IF EXISTS "idx_users_email";`)
}

func TestPlanGeneratedMigrationSpecs_SplitsPopulatedAndEmptyTableIndexes(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{IndexesAdded: []string{"idx_users_email", "idx_posts_title"}}
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

	specs, _, err := planGeneratedMigrationSpecs(diff, generated, dbSchema, postgresInfo(capability.Postgres16()), 100, "add_indexes")

	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 2)
	c.Assert(specs[0].NoTransaction, qt.IsFalse)
	c.Assert(specs[0].UpSQL, qt.Contains, `CREATE INDEX IF NOT EXISTS "idx_posts_title" ON "posts" ("title");`)
	c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "idx_users_email")
	c.Assert(specs[1].NoTransaction, qt.IsTrue)
	c.Assert(specs[1].UpSQL, qt.Contains, `CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_users_email" ON "users" ("email");`)
	c.Assert(specs[1].UpSQL, qt.Not(qt.Contains), "idx_posts_title")
}

func TestPlanGeneratedMigrationSpecs_RefusesUnsplitNonTransactionalMix(t *testing.T) {
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

	specs, _, err := planGeneratedMigrationSpecs(diff, generated, &dbschematypes.DBSchema{}, postgresInfo(capability.Postgres16()), 100, "mixed")

	c.Assert(specs, qt.IsNil)
	c.Assert(err, qt.ErrorMatches, "generated migration mixes transactional statements with non-transactional statements that cannot be split automatically")
}

func TestCreateMigrationFilesFromSpecs_WritesAllPairs(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	files, err := createMigrationFilesFromSpecs(dir, "", []generatedMigrationSpec{
		{Version: 100, Name: "transactional", UpSQL: "SELECT 1;\n", DownSQL: "SELECT 2;\n"},
		{Version: 101, Name: "concurrent_indexes", UpSQL: "-- +ptah no_transaction\nSELECT 3;\n", DownSQL: "-- +ptah no_transaction\nSELECT 4;\n", NoTransaction: true},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 2)
	c.Assert(files.UpFile, qt.Equals, files.Files[0].UpFile)
	c.Assert(files.Files[0].NoTransaction, qt.IsFalse)
	c.Assert(files.Files[1].NoTransaction, qt.IsTrue)
	c.Assert(files.Files[0].Version < files.Files[1].Version, qt.IsTrue)
	c.Assert(strings.HasSuffix(files.Files[0].UpFile, "100_transactional.up.sql"), qt.IsTrue)
	c.Assert(strings.HasSuffix(files.Files[1].UpFile, "101_concurrent_indexes.up.sql"), qt.IsTrue)
}

func indexOnlyDiff() *types.SchemaDiff {
	return &types.SchemaDiff{IndexesAdded: []string{"idx_users_email"}}
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
