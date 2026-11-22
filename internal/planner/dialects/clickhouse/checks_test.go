package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// checkTable is a declared table t over an empty sorting key, with the fields
// and constraints a test gives it.
func checkTable(fields []schemamodel.Field, constraints ...schemamodel.Constraint) *schemamodel.Database {
	storage := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()"}
	return &schemamodel.Database{
		Tables:      []schemamodel.Table{{Name: "t", StructName: "T", Facets: must.Must(schemaext.NewFacets(storage.Desired()))}},
		Fields:      fields,
		Constraints: constraints,
	}
}

// observedCheckTable is table t as a read reports it, with its Int32 columns,
// a String column s where columns name it, and the CHECK constraints the
// server prints.
func observedCheckTable(columns []string, checks ...catalog.Constraint) *catalog.Database {
	storage := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()"}
	table := catalog.Table{Name: "t", Facets: must.Must(schemaext.NewFacets(storage))}
	for _, column := range columns {
		columnType := map[bool]string{true: "String", false: "Int32"}[column == "s"]
		table.Columns = append(table.Columns, catalog.Column{Name: column, DataType: columnType, ColumnType: columnType, IsNullable: "NO"})
	}
	for i := range checks {
		checks[i].TableName, checks[i].Type = "t", "CHECK"
	}
	return &catalog.Database{FeatureCoverage: observedStorageCoverage(), Tables: []catalog.Table{table}, Constraints: checks}
}

func int32Field(name, check string) schemamodel.Field {
	return schemamodel.Field{StructName: "T", Name: name, Type: "Int32", Check: check}
}

func tableCheck(name, expression string) schemamodel.Constraint {
	return schemamodel.Constraint{StructName: "T", Table: "t", Type: "CHECK", Name: name, CheckExpression: expression}
}

func reportedCheck(name, clause string) catalog.Constraint {
	return catalog.Constraint{Name: name, CheckClause: new(clause)}
}

func checkStatements(c *qt.C, desired *schemamodel.Database, current *catalog.Database) []string {
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDialect(c.Context(), desired, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), runtime, diff, "clickhouse")
	c.Assert(err, qt.IsNil)
	return statements
}

// ClickHouse refuses a DROP COLUMN of a column a CHECK reads and an ADD
// CONSTRAINT over a column the table lacks, so the CHECK drops go first and
// the additions last. An unnamed CHECK takes the name ClickHouse needs, and an
// added column's CHECK is a constraint of its own, since ADD COLUMN has none.
func TestGenerateMigrationAST_PlansCheckChangesAroundTheColumnChanges(t *testing.T) {
	c := qt.New(t)
	desired := checkTable(
		[]schemamodel.Field{int32Field("id", ""), int32Field("v", ""), int32Field("w", "w > 0")},
		tableCheck("", "v > 0"), tableCheck("t_named", "v < 100"),
	)
	current := observedCheckTable([]string{"id", "v", "u"}, reportedCheck("t_u_check", "u > 0"), reportedCheck("t_named", "v < 50"))

	got := checkStatements(c, desired, current)

	c.Assert(got, qt.DeepEquals, []string{
		"ALTER TABLE t DROP CONSTRAINT t_named",
		"ALTER TABLE t DROP CONSTRAINT t_u_check",
		"ALTER TABLE t ADD COLUMN w Int32",
		"ALTER TABLE t DROP COLUMN u",
		heldRows + "ALTER TABLE t ADD CONSTRAINT t_w_check CHECK (w > 0)",
		heldRows + "ALTER TABLE t ADD CONSTRAINT t_check CHECK (v > 0)",
		heldRows + "ALTER TABLE t ADD CONSTRAINT t_named CHECK (v < 100)",
	})
}

// heldRows is the note before each ADD CONSTRAINT of table t.
const heldRows = "-- CLICKHOUSE: ADD CONSTRAINT checks the rows written after it, not the rows t holds\n"

// A table whose only change is a CHECK has no column change for the request
// to capture it by, and its steps still plan.
func TestGenerateMigrationAST_PlansACheckOnlyChange(t *testing.T) {
	c := qt.New(t)
	desired := checkTable([]schemamodel.Field{int32Field("id", ""), int32Field("v", "")}, tableCheck("", "v > 1"))
	current := observedCheckTable([]string{"id", "v"}, reportedCheck("t_check", "v > 0"))

	got := checkStatements(c, desired, current)

	c.Assert(got, qt.DeepEquals, []string{
		"ALTER TABLE t DROP CONSTRAINT t_check",
		heldRows + "ALTER TABLE t ADD CONSTRAINT t_check CHECK (v > 1)",
	})
}

// The same holds for a table whose only change is its comment.
func TestGenerateMigrationAST_PlansATableCommentOnlyChange(t *testing.T) {
	c := qt.New(t)
	desired := checkTable([]schemamodel.Field{int32Field("id", "")})
	desired.Tables[0].Comment = "hello"
	current := observedCheckTable([]string{"id"})

	got := checkStatements(c, desired, current)

	c.Assert(got, qt.DeepEquals, []string{"ALTER TABLE t MODIFY COMMENT 'hello'"})
}

// The server prints a CHECK with its own parentheses and operators, and the
// declaration it came from plans nothing.
func TestGenerateMigrationAST_ACheckTheServerReformatsPlansNothing(t *testing.T) {
	c := qt.New(t)
	desired := checkTable(
		[]schemamodel.Field{int32Field("id", ""), int32Field("v", "v != 7"), {StructName: "T", Name: "s", Type: "String"}},
		tableCheck("", "v > 0"), tableCheck("", "v<>5 AND v BETWEEN 1 AND 100"), tableCheck("t_named", "length(s) < 10 OR s LIKE 'x%'"),
	)
	current := observedCheckTable([]string{"id", "v", "s"},
		reportedCheck("t_v_check", "v != 7"), reportedCheck("t_check", "v > 0"),
		reportedCheck("t_check1", "(v != 5) AND ((v >= 1) AND (v <= 100))"), reportedCheck("t_named", "(length(s) < 10) OR (s LIKE 'x%')"),
	)

	got := checkStatements(c, desired, current)

	c.Assert(got, qt.HasLen, 0)
}

// A table the plan creates takes its CHECKs in CREATE TABLE, and a table it
// drops takes them away with it.
func TestGenerateMigrationAST_CreatesAndDropsATableWithItsChecks(t *testing.T) {
	c := qt.New(t)
	desired := checkTable([]schemamodel.Field{int32Field("id", ""), int32Field("v", "v != 7")}, tableCheck("", "v > 0"))
	current := observedCheckTable(nil)
	current.Tables[0].Name = "gone"
	current.Tables[0].Columns = []catalog.Column{{Name: "id", DataType: "Int32", ColumnType: "Int32", IsNullable: "NO"}}
	current.Constraints = []catalog.Constraint{{Name: "gone_check", TableName: "gone", Type: "CHECK", CheckClause: new("id > 0")}}

	got := checkStatements(c, desired, current)

	c.Assert(got, qt.DeepEquals, []string{
		"CREATE TABLE t (\n  id Int32,\n  v Int32,\n  CONSTRAINT t_v_check CHECK (v != 7),\n  CONSTRAINT t_check CHECK (v > 0)\n) ENGINE = MergeTree ORDER BY (tuple()) PRIMARY KEY (tuple())",
		"-- WARNING: dropping table will delete all data\nDROP TABLE IF EXISTS gone",
	})
}
