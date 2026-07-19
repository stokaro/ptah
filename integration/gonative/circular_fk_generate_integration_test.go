//go:build integration

package gonative_test

import (
	"database/sql"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestPostgreSQLGenerateMutualForeignKeysApplyIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	cleanupMutualForeignKeyTables(t, db)
	defer cleanupMutualForeignKeyTables(t, db)

	_, currentFile, _, ok := runtime.Caller(0)
	c.Assert(ok, qt.IsTrue)
	fixtureDir := filepath.Join(
		filepath.Dir(currentFile),
		"..",
		"fixtures",
		"entities",
		"029-roundtrip-mutual-cycle",
	)

	database, err := goschema.ParseDir(fixtureDir)
	c.Assert(err, qt.IsNil)

	sqlText := strings.Join(renderer.GetOrderedCreateStatements(database, "postgres"), "\n")
	c.Assert(sqlText, qt.Contains, `ALTER TABLE "left_nodes" ADD CONSTRAINT "fk_left_nodes_right_id"`)
	c.Assert(sqlText, qt.Contains, `ALTER TABLE "right_nodes" ADD CONSTRAINT "fk_right_nodes_left_id"`)

	for _, stmt := range migrator.SplitSQLStatements(sqlText) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		_, err = db.Exec(stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("statement failed:\n%s", stmt))
	}

	var foreignKeyCount int
	err = db.QueryRow(`
		SELECT count(*)
		FROM pg_constraint
		WHERE contype = 'f'
			AND conname IN ('fk_left_nodes_right_id', 'fk_right_nodes_left_id')
	`).Scan(&foreignKeyCount)
	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeyCount, qt.Equals, 2)
}

func cleanupMutualForeignKeyTables(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`DROP TABLE IF EXISTS "left_nodes", "right_nodes" CASCADE`)
	qt.Assert(t, err, qt.IsNil)
}
