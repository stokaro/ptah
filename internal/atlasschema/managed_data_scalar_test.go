package atlasschema_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
)

// eventsSchema declares one table whose managed column holds a moment, with the
// declared value carried as the YAML scalar that declared it.
func eventsSchema(seenAt schemamodel.ManagedValue) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Event", Name: "events"}},
		Fields: []schemamodel.Field{
			{StructName: "Event", FieldName: "ID", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "Event", FieldName: "SeenAt", Name: "seen_at", Type: "TEXT", Nullable: true},
		},
		ManagedData: []schemamodel.ManagedData{{
			StructName: "Event",
			Table:      "events",
			Keys:       []string{"id"},
			File:       "events.yaml",
			Rows: []schemamodel.ManagedRow{{
				"id":      {Tag: "str", Text: "A"},
				"seen_at": seenAt,
			}},
		}},
	}
	schemamodel.Finalize(db)
	return db
}

// liveMoment applies the schema, then writes the moment the database holds, so
// the comparison meets a value a driver returned rather than one this plan just
// rendered.
func liveMoment(c *qt.C, conn *dbschema.DatabaseConnection, text string) {
	c.Helper()
	applyPlan(c, conn, eventsSchema(schemamodel.ManagedValue{Tag: "str", Text: "seeded"}))
	_, err := conn.ExecContext(context.Background(), `UPDATE events SET seen_at = ? WHERE id = 'A'`, text)
	c.Assert(err, qt.IsNil)
}

// TestPreparePlanFile_ResolvesADeclaredMomentTheWayTheRowReportDoes holds the
// plan and the row report to one answer about one declaration.
//
// A declared scalar resolves the way the YAML resolver resolves it, so a
// timestamp is a moment and not its source text. Resolved as text instead, the
// plan compares `2024-03-01` against the `2024-03-01T00:00:00Z` the database
// returned, finds them different, and carries an UPDATE for a converged row on
// every run -- while `ptah schema drift`, reading the same declaration against
// the same database, reports no drift at all (stokaro/ptah#3276).
func TestPreparePlanFile_ResolvesADeclaredMomentTheWayTheRowReportDoes(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "moment.db")
	liveMoment(c, conn, "2024-03-01T00:00:00Z")

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired: eventsSchema(schemamodel.ManagedValue{Tag: "timestamp", Text: "2024-03-01"}),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements, qt.HasLen, 0)
}

// TestPreparePlanFile_StillPlansAMomentTheDatabaseDoesNotHold is the control.
// Reading the declared moment as an instant must not fold every moment into
// every other one: a row whose stored value names a different day is still a
// difference the plan carries.
func TestPreparePlanFile_StillPlansAMomentTheDatabaseDoesNotHold(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "other-moment.db")
	liveMoment(c, conn, "2024-03-02T00:00:00Z")

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired: eventsSchema(schemamodel.ManagedValue{Tag: "timestamp", Text: "2024-03-01"}),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(planSQL(plan), qt.Contains, `UPDATE "events"`)
}
