//go:build integration

package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

const migrationSchema = "ptah_ydb_migration"

var migrationSchemas = []string{migrationSchema}

// TestYDBMigration_PlannedAgainstTheLiveDatabase plans a migration against a
// YDB database that holds rows, applies it, and reads the result back to
// nothing left to plan. The migration adds a column, drops a column an index
// covers, adds an index, drops another, and drops a table: YDB refuses to drop
// an indexed column, so the plan has to drop the index first, and it runs one
// statement per query because a query of several DDL statements is compiled
// against the schema as it stood before the query.
func TestYDBMigration_PlannedAgainstTheLiveDatabase(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, migrationSchemas)
			c.Cleanup(func() { dropTables(c, conn, migrationSchemas) })

			apply(c, conn, planAgainst(c, conn, migrationBefore(), migrationSchemas))
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"UPSERT INTO `ptah_ydb_migration/items` (`id`, `name`, `sku`, `legacy`) "+
					"VALUES (1l, 'one'u, 'A-1'u, 'x'u), (2l, 'two'u, 'B-2'u, 'y'u)"), qt.IsNil)

			after := migrationAfter()
			planned := planAgainst(c, conn, after, migrationSchemas)
			c.Assert(planned, qt.Not(qt.HasLen), 0)
			apply(c, conn, planned)

			c.Assert(planAgainst(c, conn, after, migrationSchemas), qt.HasLen, 0)
			live := readScoped(c, conn, migrationSchemas)
			c.Assert(tableNames(live), qt.DeepEquals, []string{"ptah_ydb_migration|items"})
			c.Assert(columnNamesOf(tableNamed(c, live, migrationSchema, "items")), qt.DeepEquals,
				[]string{"id", "name", "sku", "stock"})
			c.Assert(indexNamesOf(live), qt.DeepEquals, []string{"idx_items_name"})

			var rows int64
			c.Assert(conn.QueryRowContext(c.Context(),
				"SELECT COUNT(*) FROM `ptah_ydb_migration/items` WHERE `stock` IS NULL").Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, int64(2))
		})
	}
}

func migrationBefore() *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Item", Name: "items", Schema: migrationSchema},
			{StructName: "Obsolete", Name: "obsolete", Schema: migrationSchema},
		},
		Fields: []schemamodel.Field{
			{StructName: "Item", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Item", Name: "name", Type: "TEXT", Nullable: true},
			{StructName: "Item", Name: "sku", Type: "VARCHAR(32)", Nullable: true},
			{StructName: "Item", Name: "legacy", Type: "TEXT", Nullable: true},
			{StructName: "Obsolete", Name: "id", Type: "INTEGER", Primary: true},
		},
		Indexes: []schemamodel.Index{
			{StructName: "Item", Name: "idx_items_sku", Fields: []string{"sku"}},
			{StructName: "Item", Name: "idx_items_legacy", Fields: []string{"legacy"}},
		},
	}
	schemamodel.Finalize(db)
	return db
}

func migrationAfter() *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", Schema: migrationSchema}},
		Fields: []schemamodel.Field{
			{StructName: "Item", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Item", Name: "name", Type: "TEXT", Nullable: true},
			{StructName: "Item", Name: "sku", Type: "VARCHAR(32)", Nullable: true},
			{StructName: "Item", Name: "stock", Type: "INTEGER", Nullable: true},
		},
		Indexes: []schemamodel.Index{
			{StructName: "Item", Name: "idx_items_name", Fields: []string{"name"}},
		},
	}
	schemamodel.Finalize(db)
	return db
}
