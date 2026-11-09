package schemadiff_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff"
)

func TestMariaDBColumnCheckChangesWithItsColumn(t *testing.T) {
	for _, test := range []struct {
		name        string
		oldCheck    string
		newCheck    string
		columnLevel bool
		wantSQL     string
		wantDrop    bool
	}{
		{name: "remove column check", oldCheck: "`c` > 0", wantSQL: "MODIFY COLUMN `c` int", columnLevel: true},
		{name: "change column check", oldCheck: "`c` > 0", newCheck: "c > 1", wantSQL: "CHECK (c > 1)", columnLevel: true},
		{name: "remove table check", oldCheck: "`c` > 0", wantDrop: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
				Fields: []schemamodel.Field{{StructName: "T", Name: "c", Type: "int", Nullable: true, Check: test.newCheck}},
			}
			current := &catalog.Database{
				Tables:      []catalog.Table{{Name: "t", Columns: []catalog.Column{{Name: "c", DataType: "int", IsNullable: "YES"}}}},
				Constraints: []catalog.Constraint{{Name: "c", TableName: "t", Type: "CHECK", ColumnName: "c", ColumnLevelCheck: test.columnLevel, CheckClause: &test.oldCheck}},
			}
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, platform.MariaDB, runtime)
			c.Assert(err, qt.IsNil)
			plan, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, diff, platform.MariaDB)
			c.Assert(err, qt.IsNil)
			sql := strings.Join(plan, "\n")
			c.Assert(sql, qt.Contains, test.wantSQL)
			c.Assert(strings.Contains(sql, "DROP CONSTRAINT"), qt.Equals, test.wantDrop)
			c.Assert(strings.Contains(sql, "MODIFY COLUMN"), qt.Equals, !test.wantDrop)
			c.Assert(sql, qt.Not(qt.Contains), "ADD CONSTRAINT")
			nodes, err := planner.GenerateSchemaDiffAST(t.Context(), runtime, diff, platform.MariaDB)
			c.Assert(err, qt.IsNil)
			assessments, err := safety.AssessRendered(t.Context(), runtime, nodes, platform.MariaDB)
			c.Assert(err, qt.IsNil)
			c.Assert(safety.HasDestructiveAssessment(assessments), qt.IsTrue, qt.Commentf("%+v", assessments))
		})
	}
}
