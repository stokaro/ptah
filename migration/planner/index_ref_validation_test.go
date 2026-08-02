package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestGenerateSchemaDiffAST_NilDiffRejected(t *testing.T) {
	c := qt.New(t)
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
		c.Run(test.name, func(c *qt.C) {
			nodes, err := planner.GenerateSchemaDiffAST(nil, &goschema.Database{}, test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestGenerateSchemaDiffAST_RemovalDoesNotRequireTargetSchema(t *testing.T) {
	c := qt.New(t)
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
	diff := &types.SchemaDiff{
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_users_email", TableName: "users"},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			nodes, err := planner.GenerateSchemaDiffAST(diff, nil, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 1)
		})
	}
}

func TestGenerateSchemaDiffAST_IndexRefMissingOwnerRejected(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{{Name: "idx_users_email"}},
	}
	generated := &goschema.Database{
		Indexes: []goschema.Index{
			{Name: "idx_users_email", TableName: "users", Fields: []string{"email"}},
		},
	}

	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.Postgres)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(nodes, qt.IsNil)
}

func TestGenerateSchemaDiffAST_AmbiguousStructOwnerRejected(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_shared", TableName: "app.users"},
		},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Shared", Schema: "app", Name: "users"},
			{StructName: "Shared", Schema: "archive", Name: "users"},
		},
		Indexes: []goschema.Index{
			{Name: "idx_shared", StructName: "Shared", Fields: []string{"email"}},
		},
	}

	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.Postgres)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(nodes, qt.IsNil)
}

func TestGenerateSchemaDiffAST_SchemaScopedDuplicateAdditionsRejected(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgresql", dialect: platform.Postgres},
		{name: "spanner", dialect: platform.Spanner},
		{name: "sqlite", dialect: platform.SQLite},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			diff := &types.SchemaDiff{
				IndexesAdded: []types.IndexRef{
					{Name: "idx_shared", TableName: "users"},
					{Name: "idx_shared", TableName: "orders"},
				},
			}
			generated := duplicateIndexTarget("users", "orders")

			nodes, err := planner.GenerateSchemaDiffAST(diff, generated, test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestGenerateSchemaDiffAST_UnchangedTargetIndexConflictRejected(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name          string
		dialect       string
		addedTable    string
		existingTable string
	}{
		{
			name:          "postgresql",
			dialect:       platform.Postgres,
			addedTable:    "app.orders",
			existingTable: "app.users",
		},
		{
			name:          "spanner",
			dialect:       platform.Spanner,
			addedTable:    "app.orders",
			existingTable: "app.users",
		},
		{
			name:          "sqlite",
			dialect:       platform.SQLite,
			addedTable:    "main.orders",
			existingTable: "main.users",
		},
		{
			name:          "postgresql mixed qualification",
			dialect:       platform.Postgres,
			addedTable:    "orders",
			existingTable: "public.users",
		},
		{
			name:          "yugabytedb mixed qualification",
			dialect:       platform.YugabyteDB,
			addedTable:    "orders",
			existingTable: "public.users",
		},
		{
			name:          "spanner mixed qualification",
			dialect:       platform.Spanner,
			addedTable:    "orders",
			existingTable: "public.users",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			diff := &types.SchemaDiff{
				IndexesAdded: []types.IndexRef{
					{Name: "idx_shared", TableName: test.addedTable},
				},
			}
			generated := duplicateIndexTarget(test.addedTable, test.existingTable)

			nodes, err := planner.GenerateSchemaDiffAST(diff, generated, test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestGenerateSchemaDiffAST_TargetConflictRejectedWithoutIndexChanges(t *testing.T) {
	c := qt.New(t)
	generated := duplicateIndexTarget("app.users", "app.orders")

	nodes, err := planner.GenerateSchemaDiffAST(
		&types.SchemaDiff{},
		generated,
		platform.Postgres,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(nodes, qt.IsNil)
}

func TestGenerateSchemaDiffAST_UnknownQualifiedTargetOwnerRejected(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_users_email", TableName: "publci.users"},
		},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "public", Name: "users"},
		},
		Indexes: []goschema.Index{
			{
				Name:      "idx_users_email",
				TableName: "publci.users",
				Fields:    []string{"email"},
			},
		},
	}

	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.Postgres)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(nodes, qt.IsNil)
}

func TestGenerateSchemaDiffAST_TableScopedDuplicateAdditionsAccepted(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "cockroachdb", dialect: platform.CockroachDB},
		{name: "clickhouse", dialect: platform.ClickHouse},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			diff := &types.SchemaDiff{
				IndexesAdded: []types.IndexRef{
					{Name: "idx_shared", TableName: "users"},
					{Name: "idx_shared", TableName: "orders"},
				},
			}
			generated := duplicateIndexTarget("users", "orders")

			nodes, err := planner.GenerateSchemaDiffAST(diff, generated, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 2)
		})
	}
}

func TestGenerateSchemaDiffAST_MariaDBUnicodeCaseReplacementDropsFirst(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "ä_idx", TableName: "users"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "Ä_idx", TableName: "users"},
		},
	}
	generated := &goschema.Database{
		Indexes: []goschema.Index{
			{Name: "ä_idx", TableName: "users", Fields: []string{"email"}},
		},
	}

	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.MariaDB)

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
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx.users.email", TableName: "public.users"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx.users.email", TableName: "public.users"},
		},
	}
	generated := &goschema.Database{
		Indexes: []goschema.Index{
			{Name: "idx.users.email", TableName: "public.users", Fields: []string{"email"}},
		},
	}

	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.Postgres)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL(platform.Postgres, nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `DROP INDEX IF EXISTS "public"."idx.users.email";`)
	c.Assert(sql, qt.Contains, `CREATE INDEX IF NOT EXISTS "idx.users.email" ON "public"."users" ("email");`)
}

func TestGenerateSchemaDiffSQL_SQLiteRawDottedIndexNameIsOneIdentifier(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx.users.email", TableName: "main.users"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx.users.email", TableName: "main.users"},
		},
	}
	generated := &goschema.Database{
		Indexes: []goschema.Index{
			{Name: "idx.users.email", TableName: "main.users", Fields: []string{"email"}},
		},
	}

	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.SQLite)
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
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: indexName, TableName: tableName},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: indexName, TableName: tableName},
		},
	}
	generated := &goschema.Database{
		Indexes: []goschema.Index{
			{Name: indexName, TableName: tableName, Fields: []string{"payload"}, Type: "minmax"},
		},
	}

	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.ClickHouse)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL(platform.ClickHouse, nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains,
		"ALTER TABLE `analytics`.`events``; DROP TABLE audit; --` DROP INDEX `idx``; DROP TABLE users; --`;")
	c.Assert(sql, qt.Contains,
		"ALTER TABLE `analytics`.`events``; DROP TABLE audit; --` ADD INDEX `idx``; DROP TABLE users; --` payload TYPE minmax GRANULARITY 8192;")
}

func duplicateIndexTarget(tableNames ...string) *goschema.Database {
	return &goschema.Database{
		Indexes: []goschema.Index{
			{Name: "idx_shared", TableName: tableNames[0], Fields: []string{"tenant_id"}, Type: "minmax"},
			{Name: "idx_shared", TableName: tableNames[1], Fields: []string{"tenant_id"}, Type: "minmax"},
		},
	}
}
