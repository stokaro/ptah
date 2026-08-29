package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestGenerateSchemaDiffSQL_AnEnumRemovalConvertsTheColumnsTheComparisonFound
// drives the comparison rather than a hand-built diff, because the thing under
// test is what the comparison PUTS on the change.
//
// Removing a value from a PostgreSQL enum is not an ALTER: the type is renamed
// aside, recreated without the value, and every column naming it is converted
// across. Which columns those are is a question about the whole declaration, and
// the planner used to answer it by scanning every declared field. The comparison
// answers it now, at the moment it decides the value is going.
//
// Every other test of this path builds the diff by hand and supplies the answer,
// so emptying the comparison's list reddens none of them -- measured. This is
// the one that fails.
func TestGenerateSchemaDiffSQL_AnEnumRemovalConvertsTheColumnsTheComparisonFound(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Ticket", Name: "wf2315_tickets"}},
		Fields: []schemamodel.Field{
			{StructName: "Ticket", Name: "id", Type: "BIGINT", Primary: true},
			{
				StructName: "Ticket", Name: "state", Type: "wf2315_state",
				Default: "open", DefaultSet: true,
			},
		},
		Enums: []schemamodel.Enum{{
			Name:   "wf2315_state",
			Values: []string{"open", "closed"},
		}},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "wf2315_tickets", Type: "TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
				{Name: "state", DataType: "wf2315_state", IsNullable: "YES", OrdinalPosition: 2},
			},
		}},
		Enums: []catalog.Enum{{
			Name:   "wf2315_state",
			Values: []string{"open", "closed", "archived"},
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, platform.Postgres)
	c.Assert(diff.EnumsModified, qt.HasLen, 1)
	c.Assert(diff.EnumsModified[0].ValuesRemoved, qt.DeepEquals, []string{"archived"})

	sql, err := planner.GenerateSchemaDiffSQL(diff, desired, platform.Postgres)
	c.Assert(err, qt.IsNil)

	// The conversion, and the default put back around it. Without the carried
	// column the plan renames and recreates the type and leaves the column
	// pointing at the renamed one, which no longer exists under that name.
	c.Assert(sql, qt.Contains, `ALTER TABLE "wf2315_tickets" ALTER COLUMN "state" TYPE "wf2315_state"`,
		qt.Commentf("plan:\n%s", sql))
	c.Assert(sql, qt.Contains, `ALTER COLUMN "state" DROP DEFAULT`, qt.Commentf("plan:\n%s", sql))
	c.Assert(sql, qt.Contains, `ALTER COLUMN "state" SET DEFAULT 'open'`, qt.Commentf("plan:\n%s", sql))

	// The drop of the renamed type comes last: it cannot run while a column
	// still names it.
	convert := strings.Index(sql, `ALTER COLUMN "state" TYPE`)
	drop := strings.Index(sql, `DROP TYPE "wf2315_state__ptah_old"`)
	c.Assert(drop > convert, qt.IsTrue, qt.Commentf("plan:\n%s", sql))
}
