//go:build integration

package gonative_test

import (
	"database/sql"
	"strings"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/migration/migrator"
)

func renderConformanceSQL(c *qt.C, target *goschema.Database, dialect string) string {
	createAST := fromschema.FromDatabase(*target, dialect)
	createSQL, err := renderer.RenderSQL(dialect, createAST.Statements...)
	c.Assert(err, qt.IsNil)
	return strings.TrimSpace(createSQL)
}

func execConformanceSQL(c *qt.C, db *sql.DB, sqlText, fixture string) {
	for _, stmt := range migrator.SplitSQLStatements(sqlText) {
		_, err := db.Exec(stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("%s schema statement must apply: %s", fixture, stmt))
	}
}

func filterConformanceSchema(in *dbschematypes.DBSchema, keepTables map[string]struct{}) *dbschematypes.DBSchema {
	out := *in
	out.Tables = filterTables(in.Tables, keepTables)
	out.Indexes = filterIndexes(in.Indexes, keepTables)
	out.Constraints = filterConstraints(in.Constraints, keepTables)
	return &out
}
