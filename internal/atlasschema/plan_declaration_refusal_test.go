package atlasschema_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/planrefusal"
	"ptah.run/migration/schemadiff"
)

// coveringUnique declares a UNIQUE constraint with an INCLUDE column, which the
// selected validator refuses on SQLite as it does on the MySQL family.
func coveringUnique() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "ID", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "T", FieldName: "A", Name: "a", Type: "INTEGER"},
			{StructName: "T", FieldName: "B", Name: "b", Type: "INTEGER"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "T", Name: "t_a_key", Type: "UNIQUE", Table: "t",
			Columns: []string{"a"}, IncludeColumns: []string{"b"},
		}},
	}
}

// foldedColumnNames declares two columns whose names SQLite folds into one, which
// the comparison's own check of the declaration refuses.
func foldedColumnNames() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "ID", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "T", FieldName: "Upper", Name: "VALUE", Type: "INTEGER"},
			{StructName: "T", FieldName: "Lower", Name: "value", Type: "INTEGER"},
		},
	}
}

// TestPreparePlanFile_MarksWhatTheComparisonRefused_FailurePath drives a real
// plan to the two refusals a comparison completes. The refusal of a covering
// UNIQUE constraint is the selected validator's, and it is not
// [schemadiff.RefusalError]. Each one reaches the report with the same code.
func TestPreparePlanFile_MarksWhatTheComparisonRefused_FailurePath(t *testing.T) {
	t.Run("the selected validator refuses a covering constraint", func(t *testing.T) {
		c := qt.New(t)
		conn := managedDataConnection(c, "covering.db")

		plan, err := atlasschema.PreparePlanFile(c.Context(), conn, atlasschema.PlanFileOptions{
			Desired: coveringUnique(),
			Runtime: must.Must(builtin.New()),
		})

		c.Assert(plan, qt.DeepEquals, atlasschema.PlanFile{})
		c.Assert(err, qt.ErrorMatches, `compare database schema: sqlite does not support INCLUDE columns on UNIQUE constraint "t_a_key"; target .*`)
		c.Assert(err, qt.ErrorAs, new(*schemavalidation.RefusalError))
		c.Assert(err, qt.Not(qt.ErrorAs), new(*schemadiff.RefusalError))
		c.Assert(err, qt.ErrorAs, new(*planrefusal.DeclarationError))
		report := atlasschema.NewPlanReport(atlasschema.PlanEvidence{Err: err})
		c.Assert(report.Outcome, qt.Equals, atlasschema.PlanOutcomeRefused)
		c.Assert(report.Refusal, qt.DeepEquals, &atlasschema.Refusal{Code: atlasschema.RefusalDeclarationRefused})
	})
	t.Run("the comparison refuses two column names that fold into one", func(t *testing.T) {
		c := qt.New(t)
		conn := managedDataConnection(c, "folded-columns.db")

		plan, err := atlasschema.PreparePlanFile(c.Context(), conn, atlasschema.PlanFileOptions{
			Desired: foldedColumnNames(),
			Runtime: must.Must(builtin.New()),
		})

		c.Assert(plan, qt.DeepEquals, atlasschema.PlanFile{})
		c.Assert(err, qt.ErrorAs, new(*schemadiff.RefusalError))
		c.Assert(err, qt.Not(qt.ErrorAs), new(*schemavalidation.RefusalError))
		c.Assert(err, qt.ErrorAs, new(*planrefusal.DeclarationError))
		report := atlasschema.NewPlanReport(atlasschema.PlanEvidence{Err: err})
		c.Assert(report.Outcome, qt.Equals, atlasschema.PlanOutcomeRefused)
		c.Assert(report.Refusal, qt.DeepEquals, &atlasschema.Refusal{Code: atlasschema.RefusalDeclarationRefused})
	})
}

// TestPreparePlanFile_ReadsTheTargetBeforeItRefusesADeclaration_FailurePath holds
// the promise the code makes: a target that cannot be read answers with the read,
// not with the refusal the same declaration draws from a target that can.
func TestPreparePlanFile_ReadsTheTargetBeforeItRefusesADeclaration_FailurePath(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "unreadable.db")
	unreadable(c, conn)

	_, err := atlasschema.PreparePlanFile(c.Context(), conn, atlasschema.PlanFileOptions{
		Desired: coveringUnique(),
		Runtime: must.Must(builtin.New()),
	})

	c.Assert(err, qt.ErrorMatches, `read database schema: .*`)
	c.Assert(err, qt.Not(qt.ErrorAs), new(*planrefusal.DeclarationError))
	c.Assert(atlasschema.NewPlanReport(atlasschema.PlanEvidence{Err: err}).Outcome, qt.Equals, atlasschema.PlanOutcomeFailed)
}

// unreadable leaves conn on a database the schema reader refuses: an index on a
// virtual table's shadow storage.
func unreadable(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	for _, statement := range []string{
		`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
		`CREATE INDEX my_idx ON docs_content(c0)`,
	} {
		_, err := conn.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}
