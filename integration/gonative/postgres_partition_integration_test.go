//go:build integration

package gonative_test

import (
	"database/sql"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestPostgreSQLPartitionedTableExecuteIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	cleanupPostgreSQLPartitionedTable(t, db)
	defer cleanupPostgreSQLPartitionedTable(t, db)

	table := ast.NewCreateTable("ptah_partition_metrics").
		AddColumn(ast.NewColumn("x", "integer").SetNotNull()).
		AddColumn(ast.NewColumn("y", "integer").SetNotNull())
	table.Partition = &ast.PartitionSpec{
		Type: "RANGE",
		Parts: []ast.PartitionPart{
			{Name: "x"},
			{Expr: "y * 2"},
		},
	}

	sqlText, err := renderer.RenderSQL("postgres", table)
	c.Assert(err, qt.IsNil)
	c.Assert(legacyRenderedSQL(sqlText), qt.Contains, "PARTITION BY RANGE (x, (y * 2))")

	for _, stmt := range migrator.SplitSQLStatements(sqlText) {
		_, err = db.Exec(stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("statement failed: %s", stmt))
	}

	var relkind, partitionKey string
	err = db.QueryRow(`
SELECT c.relkind, pg_get_partkeydef(c.oid)
FROM pg_class c
WHERE c.relname = 'ptah_partition_metrics'
`).Scan(&relkind, &partitionKey)
	c.Assert(err, qt.IsNil)
	c.Assert(relkind, qt.Equals, "p")
	c.Assert(partitionKey, qt.Contains, "RANGE")
	c.Assert(partitionKey, qt.Contains, "x")
	c.Assert(strings.ReplaceAll(partitionKey, " ", ""), qt.Contains, "y*2")
}

func cleanupPostgreSQLPartitionedTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, _ = db.Exec("DROP TABLE IF EXISTS ptah_partition_metrics CASCADE")
}
