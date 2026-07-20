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

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")
	c.Assert(err, qt.IsNil)
	sqlText := strings.Join(statements, "\n")
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

func TestPostgreSQLForeignKeyReferencingUniqueIndexApplyIntegration(t *testing.T) {
	c := qt.New(t)

	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "ptah_fk_unique_parents"},
			{StructName: "Child", Name: "ptah_fk_unique_children"},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Parent", Name: "code", Type: "TEXT", Nullable: false},
			{StructName: "Child", Name: "id", Type: "SERIAL", Primary: true},
			{
				StructName:     "Child",
				Name:           "parent_code",
				Type:           "TEXT",
				Nullable:       false,
				Foreign:        "ptah_fk_unique_parents(code)",
				ForeignKeyName: "fk_ptah_fk_unique_children_parent_code",
			},
		},
		Indexes: []goschema.Index{
			{
				StructName: "Child",
				TableName:  "ptah_fk_unique_children",
				Name:       "idx_ptah_fk_unique_children_parent_code",
				Fields:     []string{"parent_code"},
			},
			{
				StructName: "Parent",
				TableName:  "ptah_fk_unique_parents",
				Name:       "uq_ptah_fk_unique_parents_code",
				Fields:     []string{"code"},
				Unique:     true,
			},
		},
	}

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")
	c.Assert(err, qt.IsNil)
	uniqueIndexPos := statementIndexContaining(statements, "CREATE UNIQUE INDEX", "uq_ptah_fk_unique_parents_code")
	foreignKeyPos := statementIndexContaining(statements, "ALTER TABLE", "fk_ptah_fk_unique_children_parent_code")
	nonUniqueIndexPos := statementIndexContaining(statements, "CREATE INDEX", "idx_ptah_fk_unique_children_parent_code")
	c.Assert(uniqueIndexPos, qt.Not(qt.Equals), -1)
	c.Assert(foreignKeyPos, qt.Not(qt.Equals), -1)
	c.Assert(nonUniqueIndexPos, qt.Not(qt.Equals), -1)
	c.Assert(uniqueIndexPos < foreignKeyPos, qt.IsTrue)
	c.Assert(foreignKeyPos < nonUniqueIndexPos, qt.IsTrue)

	dsn := skipIfNoPostgreSQL(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	cleanupUniqueIndexForeignKeyTables(t, db)
	defer cleanupUniqueIndexForeignKeyTables(t, db)

	sqlText := strings.Join(statements, "\n")
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
			AND conname = 'fk_ptah_fk_unique_children_parent_code'
	`).Scan(&foreignKeyCount)
	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeyCount, qt.Equals, 1)
}

func statementIndexContaining(statements []string, fragments ...string) int {
	for i, stmt := range statements {
		matches := true
		for _, fragment := range fragments {
			if !strings.Contains(stmt, fragment) {
				matches = false
				break
			}
		}
		if matches {
			return i
		}
	}
	return -1
}

func cleanupMutualForeignKeyTables(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`DROP TABLE IF EXISTS "left_nodes", "right_nodes" CASCADE`)
	qt.Assert(t, err, qt.IsNil)
}

func cleanupUniqueIndexForeignKeyTables(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`DROP TABLE IF EXISTS "ptah_fk_unique_children", "ptah_fk_unique_parents" CASCADE`)
	qt.Assert(t, err, qt.IsNil)
}
