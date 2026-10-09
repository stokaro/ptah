//go:build integration

package gonative_test

import (
	"context"
	"database/sql"
	"strings"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/engine/builtin"
	"ptah.run/internal/modelast"
)

func renderConformanceSQL(c *qt.C, target *schemamodel.Database, dialect string) string {
	createAST := must.Must(modelast.CollectDatabase(*target, dialect, modelast.Lowering{Context: context.Background()}))
	createSQL, err := builtin.RenderSQL(dialect, createAST.Statements...)
	c.Assert(err, qt.IsNil)
	return strings.TrimSpace(createSQL)
}

func execConformanceSQL(c *qt.C, db *sql.DB, sqlText, fixture string) {
	for _, stmt := range sqlutil.SplitStatements(sqlText) {
		_, err := db.Exec(stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("%s schema statement must apply: %s", fixture, stmt))
	}
}

func filterConformanceSchema(in *catalog.Database, keepTables map[string]struct{}) *catalog.Database {
	out := *in
	out.Tables = filterTables(in.Tables, keepTables)
	out.Indexes = filterIndexes(in.Indexes, keepTables)
	out.Constraints = filterConstraints(in.Constraints, keepTables)
	return &out
}
