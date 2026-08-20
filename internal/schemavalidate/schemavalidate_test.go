package schemavalidate_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemavalidate"
)

// The renderer refuses a schema it cannot render, but it refuses the first
// problem it meets and says nothing at all about indexes: an index on a column,
// or a table, that no declaration mentions rendered happily (stokaro/ptah#1711).

// ordersSchema is a one-table schema every case below indexes.
func ordersSchema(indexes ...goschema.Index) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Order", Name: "orders"}},
		Fields: []goschema.Field{
			{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Order", Name: "total", Type: "INTEGER"},
		},
		Indexes: indexes,
	}
}

// messages reduces a problem list to its rendered lines.
func messages(problems []schemavalidate.Problem) []string {
	lines := make([]string, 0, len(problems))
	for _, problem := range problems {
		lines = append(lines, problem.String())
	}
	return lines
}

func TestCollect_IndexNamingAnUndeclaredColumnIsAProblem(t *testing.T) {
	c := qt.New(t)

	problems := schemavalidate.Collect(ordersSchema(goschema.Index{
		StructName: "Order",
		Name:       "idx_missing",
		Fields:     []string{"nosuchcolumn"},
	}), "postgres")

	c.Assert(problems, qt.HasLen, 1)
	c.Assert(problems[0].Kind, qt.Equals, "index")
	c.Assert(problems[0].Object, qt.Equals, "idx_missing")
	c.Assert(problems[0].Message, qt.Contains, `names column "nosuchcolumn"`)
}

// TestCollect_IndexPartSpellingIsCheckedToo holds the other spelling. A
// declaration can name its columns through Fields or through Parts, and a
// fixture using only one accepts a checker that reads only that one.
func TestCollect_IndexPartSpellingIsCheckedToo(t *testing.T) {
	c := qt.New(t)

	problems := schemavalidate.Collect(ordersSchema(goschema.Index{
		StructName: "Order",
		Name:       "idx_parts",
		Parts:      []goschema.IndexPart{{Name: "total"}, {Name: "nosuchcolumn"}},
	}), "postgres")

	c.Assert(problems, qt.HasLen, 1)
	c.Assert(problems[0].Message, qt.Contains, `names column "nosuchcolumn"`)
}

// TestCollect_ExpressionPartsNameNoColumn is the negative control for the
// spelling above: an expression part carries no column name, so checking it as
// one would refuse every functional index.
//
// Both shapes are held. Every constructor today writes an expression key with
// Name empty, so the empty-name skip alone would pass the first case; the
// second states the precedence rule directly, so a part that ever carries both
// is read as the expression it is rather than as a column named after one.
func TestCollect_ExpressionPartsNameNoColumn(t *testing.T) {
	c := qt.New(t)

	exprOnly := schemavalidate.Collect(ordersSchema(goschema.Index{
		StructName: "Order",
		Name:       "idx_expr",
		Parts:      []goschema.IndexPart{{Expr: "lower(total)"}},
	}), "postgres")
	exprAndName := schemavalidate.Collect(ordersSchema(goschema.Index{
		StructName: "Order",
		Name:       "idx_expr_named",
		Parts:      []goschema.IndexPart{{Name: "nosuchcolumn", Expr: "lower(total)"}},
	}), "postgres")

	c.Assert(exprOnly, qt.HasLen, 0)
	c.Assert(exprAndName, qt.HasLen, 0)
}

func TestCollect_IncludeColumnsAreChecked(t *testing.T) {
	c := qt.New(t)

	problems := schemavalidate.Collect(ordersSchema(goschema.Index{
		StructName:     "Order",
		Name:           "idx_include",
		Fields:         []string{"total"},
		IncludeColumns: []string{"nosuchcolumn"},
	}), "postgres")

	c.Assert(problems, qt.HasLen, 1)
	c.Assert(problems[0].Message, qt.Contains, `names column "nosuchcolumn"`)
}

func TestCollect_IndexNamingAnUndeclaredTableIsAProblem(t *testing.T) {
	c := qt.New(t)

	problems := schemavalidate.Collect(ordersSchema(goschema.Index{
		StructName: "NoSuchStruct",
		TableName:  "nosuchtable",
		Name:       "idx_orphan",
		Fields:     []string{"id"},
	}), "postgres")

	c.Assert(problems, qt.HasLen, 1)
	c.Assert(problems[0].Message, qt.Contains, `names table "nosuchtable"`)
}

// TestCollect_ReportsEveryProblemNotTheFirst is what separates this verb from
// reading the renderer's error: three faults produce three lines.
func TestCollect_ReportsEveryProblemNotTheFirst(t *testing.T) {
	c := qt.New(t)

	problems := schemavalidate.Collect(ordersSchema(
		goschema.Index{StructName: "Order", Name: "a", Fields: []string{"missing_one"}},
		goschema.Index{StructName: "Order", Name: "b", Fields: []string{"missing_two"}},
		goschema.Index{StructName: "Order", Name: "c", Fields: []string{"missing_three"}},
	), "postgres")

	c.Assert(problems, qt.HasLen, 3)
	c.Assert(messages(problems), qt.Contains, `postgres: index "b": names column "missing_two", which table "orders" does not declare`)
}

// TestCollect_AValidSchemaHasNoProblems is the control that keeps the checks
// from being satisfied by refusing everything.
func TestCollect_AValidSchemaHasNoProblems(t *testing.T) {
	c := qt.New(t)

	problems := schemavalidate.Collect(ordersSchema(goschema.Index{
		StructName: "Order",
		Name:       "idx_total",
		Fields:     []string{"total"},
	}), "postgres")

	c.Assert(problems, qt.HasLen, 0)
}

// TestCollect_AnIndexOnAScopedAwayMaterializedViewIsAnOrphan is the case that
// looked like a false positive and is not.
//
// Materialized views are scoped per dialect and indexes are not, so an index
// on a view the target was not given keeps pointing at nothing. The renderer
// does not refuse it: for MySQL it emits CREATE INDEX `idx_summary` ON
// `Summary`, falling back to the Go struct name, which the server answers with
// "relation does not exist" at apply time. That is the shape stokaro/ptah#1725
// found on the render side, and it is exactly what this verb exists to catch
// before a database is involved.
func TestCollect_AnIndexOnAScopedAwayMaterializedViewIsAnOrphan(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Order", Name: "orders"}},
		Fields: []goschema.Field{{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true}},
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "Summary",
			Name:       "summary",
			Body:       "SELECT id FROM orders",
			Dialects:   []string{"postgres"},
		}},
		Indexes: []goschema.Index{{StructName: "Summary", Name: "idx_summary", Fields: []string{"id"}}},
	}

	postgresProblems := schemavalidate.Collect(database, "postgres")
	mysqlProblems := schemavalidate.Collect(database, "mysql")

	// PostgreSQL was given the view, so the index has an owner and the columns
	// of a view are not checkable here.
	c.Assert(postgresProblems, qt.HasLen, 0)
	c.Assert(mysqlProblems, qt.HasLen, 1)
	c.Assert(mysqlProblems[0].Message, qt.Contains, "which no declaration defines")
}

func TestCollect_ANilSchemaIsReportedRatherThanPanicking(t *testing.T) {
	c := qt.New(t)

	problems := schemavalidate.Collect(nil, "postgres")

	c.Assert(problems, qt.HasLen, 1)
	c.Assert(problems[0].Kind, qt.Equals, "schema")
}

func TestDialects_NormalizesAndDeduplicatesPreservingOrder(t *testing.T) {
	c := qt.New(t)

	got := schemavalidate.Dialects([]string{"postgres", " mysql ", "postgres", "", "mysql"})

	c.Assert(got, qt.DeepEquals, []string{"postgres", "mysql"})
}
