package planner_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgtype"
)

// archiveWithWrap declares the table archive and the composite type wrap,
// whose attribute r is archive's row type, beside the tables every style row
// starts from.
func archiveWithWrap() *schemamodel.Database {
	return styleDeclaredWith(func(d *schemamodel.Database) {
		d.Tables = append(d.Tables, schemamodel.Table{StructName: "Archive", Name: "archive"})
		d.Fields = append(d.Fields, schemamodel.Field{StructName: "Archive", Name: "id", Type: "integer", Primary: true})
		d.FeatureObjects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: pgtype.Ref(pgtype.CompositeKind, "", "wrap"),
			Value: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "r", Type: "archive"}, {Name: "n", Type: "integer"}}}}))
		d.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Desired))
	})
}

// heldWithoutTypes is the database every style row starts from, read with a
// complete type coverage that found no type.
func heldWithoutTypes() *catalog.Database {
	return styleHeldWith(func(h *catalog.Database) {
		h.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Observed))
	})
}

// TestGenerateSchemaDiffSQLStatementsWithOptions_TypeOverACreatedRowType_HappyPath
// pins the creation of a composite type whose attribute is the row type of a
// table the same plan creates, in both statement styles: the table first, the
// type after it. The type phase runs before the tables, and the server
// answered `type "archive" does not exist` when the type went there.
func TestGenerateSchemaDiffSQLStatementsWithOptions_TypeOverACreatedRowType_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		style planext.StatementStyle
	}{
		{name: "annotated", style: planext.StatementStyleAnnotated},
		{name: "exact", style: planext.StatementStyleExact},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := stylePlan(c, archiveWithWrap(), heldWithoutTypes(), planext.Options{Statements: test.style})

			table := slices.IndexFunc(got, func(statement string) bool { return strings.Contains(statement, `CREATE TABLE "archive"`) })
			typ := slices.IndexFunc(got, func(statement string) bool {
				return strings.Contains(statement, `CREATE TYPE "wrap" AS ("r" archive, "n" integer)`)
			})
			c.Assert(table, qt.Not(qt.Equals), -1, qt.Commentf("%q", got))
			c.Assert(typ > table, qt.IsTrue, qt.Commentf("%q", got))
		})
	}
}
