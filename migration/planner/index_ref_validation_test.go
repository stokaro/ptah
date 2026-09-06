package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestGenerateSchemaDiffAST_NilDiffRejected(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgresql", dialect: platform.Postgres},
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "cockroachdb", dialect: platform.CockroachDB},
		{name: "yugabytedb", dialect: platform.YugabyteDB},
		{name: "spanner", dialect: platform.Spanner},
		{name: "sql server", dialect: platform.SQLServer},
		{name: "clickhouse", dialect: platform.ClickHouse},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffAST(nil, test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestGenerateSchemaDiffAST_RemovalDoesNotRequireTargetSchema(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgresql", dialect: platform.Postgres},
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "cockroachdb", dialect: platform.CockroachDB},
		{name: "yugabytedb", dialect: platform.YugabyteDB},
		{name: "spanner", dialect: platform.Spanner},
		{name: "sql server", dialect: platform.SQLServer},
		{name: "clickhouse", dialect: platform.ClickHouse},
	}
	diff := &difftypes.SchemaDiff{
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx_users_email", TableName: "users"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffAST(diff, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 1)
		})
	}
}

func TestGenerateSchemaDiffAST_IndexRefMissingOwnerRejected(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		IndexesAdded: difftypes.IndexChanges{{Index: schemamodel.Index{Name: "idx_users_email", Fields: []string{"email"}}}},
	}
	nodes, err := planner.GenerateSchemaDiffAST(diff, platform.Postgres)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(nodes, qt.IsNil)
}

func TestGenerateSchemaDiffAST_SchemaScopedDuplicateAdditionsRejected(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgresql", dialect: platform.Postgres},
		{name: "spanner", dialect: platform.Spanner},
		{name: "sqlite", dialect: platform.SQLite},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{
				IndexesAdded: difftypes.IndexChanges{
					{Index: schemamodel.Index{Name: "idx_shared", StructName: "Shared", Fields: []string{"email"}}, TableName: "users"},
					{Index: schemamodel.Index{Name: "idx_shared", StructName: "Shared", Fields: []string{"email"}}, TableName: "orders"},
				},
			}
			nodes, err := planner.GenerateSchemaDiffAST(diff, test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestGenerateSchemaDiffAST_TableScopedDuplicateAdditionsAccepted(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "cockroachdb", dialect: platform.CockroachDB},
		{name: "clickhouse", dialect: platform.ClickHouse},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{
				IndexesAdded: difftypes.IndexChanges{
					{Index: schemamodel.Index{Name: "idx_shared", StructName: "Shared", Fields: []string{"email"}}, TableName: "users"},
					{Index: schemamodel.Index{Name: "idx_shared", StructName: "Shared", Fields: []string{"email"}}, TableName: "orders"},
				},
			}
			nodes, err := planner.GenerateSchemaDiffAST(diff, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 2)
		})
	}
}

func TestGenerateSchemaDiffAST_MariaDBUnicodeCaseReplacementDropsFirst(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		IndexesAdded: difftypes.IndexChanges{{Index: schemamodel.Index{Name: "ä_idx", TableName: "users", Fields: []string{"email"}}, TableName: "users"}},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "Ä_idx", TableName: "users"},
		},
	}
	nodes, err := planner.GenerateSchemaDiffAST(diff, platform.MariaDB)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	drop, ok := nodes[0].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Name, qt.Equals, "Ä_idx")
	create, ok := nodes[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(create.Name, qt.Equals, "ä_idx")
}

func TestGenerateSchemaDiffSQL_PostgreSQLRawDottedIndexNameIsOneIdentifier(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		IndexesAdded: difftypes.IndexChanges{{Index: schemamodel.Index{Name: "idx.users.email", TableName: "public.users", Fields: []string{"email"}}, TableName: "public.users"}},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx.users.email", TableName: "public.users"},
		},
	}
	nodes, err := planner.GenerateSchemaDiffAST(diff, platform.Postgres)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL(platform.Postgres, nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `DROP INDEX IF EXISTS "public"."idx.users.email";`)
	c.Assert(sql, qt.Contains, `CREATE INDEX IF NOT EXISTS "idx.users.email" ON "public"."users" ("email");`)
}

func TestGenerateSchemaDiffSQL_SQLiteRawDottedIndexNameIsOneIdentifier(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		IndexesAdded: difftypes.IndexChanges{{Index: schemamodel.Index{Name: "idx.users.email", TableName: "main.users", Fields: []string{"email"}}, TableName: "main.users"}},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx.users.email", TableName: "main.users"},
		},
	}
	nodes, err := planner.GenerateSchemaDiffAST(diff, platform.SQLite)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL(platform.SQLite, nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `DROP INDEX IF EXISTS "main"."idx.users.email";`)
	c.Assert(sql, qt.Contains, `CREATE INDEX IF NOT EXISTS "main"."idx.users.email" ON "users" ("email");`)
}

func TestGenerateSchemaDiffSQL_ClickHouseIndexIdentifiersAreInjectionSafe(t *testing.T) {
	c := qt.New(t)
	tableName := "analytics.events`; DROP TABLE audit; --"
	indexName := "idx`; DROP TABLE users; --"
	diff := &difftypes.SchemaDiff{
		IndexesAdded: difftypes.IndexChanges{
			{Index: schemamodel.Index{Name: indexName, TableName: tableName, Fields: []string{"payload"}, Type: "minmax"}, TableName: tableName},
		},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: indexName, TableName: tableName},
		},
	}
	nodes, err := planner.GenerateSchemaDiffAST(diff, platform.ClickHouse)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL(platform.ClickHouse, nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains,
		"ALTER TABLE `analytics`.`events``; DROP TABLE audit; --` DROP INDEX `idx``; DROP TABLE users; --`;")
	c.Assert(sql, qt.Contains,
		"ALTER TABLE `analytics`.`events``; DROP TABLE audit; --` ADD INDEX `idx``; DROP TABLE users; --` payload TYPE minmax GRANULARITY 8192;")
}
