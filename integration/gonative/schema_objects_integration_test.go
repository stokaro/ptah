//go:build integration

package gonative_test

import (
	"database/sql"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/stokaro/ptah/core/goschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/dbschema/postgres"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestSchemaObjects_RoundTripAndBodyChange_PostgreSQL_Integration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	cleanupSchemaObjects(t, db)
	defer cleanupSchemaObjects(t, db)

	target := schemaObjectsTarget()
	diff := schemadiff.Compare(target, &dbschematypes.DBSchema{})
	c.Assert(diff.HasChanges(), qt.IsTrue)

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, target, "postgres")
	c.Assert(err, qt.IsNil)
	sqlText := strings.Join(statements, ";\n") + ";"
	_, err = db.Exec(sqlText)
	c.Assert(err, qt.IsNil, qt.Commentf("generated migration SQL must execute: %s", sqlText))

	reader := postgres.NewPostgreSQLReader(db, "public")
	read, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	filtered := filterSchemaObjects(read)

	roundTrip := schemadiff.Compare(target, filtered)
	c.Assert(roundTrip.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", roundTrip))

	modified := schemaObjectsTarget()
	modified.Views[0].Body = "SELECT id, updated_at FROM ptah_schema_objects_users WHERE deleted_at IS NULL AND updated_at IS NOT NULL"
	modified.MaterializedViews[0].Body = "SELECT id, COUNT(updated_at) FROM ptah_schema_objects_users GROUP BY id"
	modified.Triggers[0].Body = "NEW.updated_at = clock_timestamp(); RETURN NEW;"

	bodyDiff := schemadiff.Compare(modified, filtered)
	c.Assert(bodyDiff.ViewsModified, qt.HasLen, 1)
	c.Assert(bodyDiff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(bodyDiff.TriggersModified, qt.HasLen, 1)

	modifiedStatements, err := planner.GenerateSchemaDiffSQLStatements(bodyDiff, modified, "postgres")
	c.Assert(err, qt.IsNil)
	modifiedSQL := strings.Join(modifiedStatements, ";\n") + ";"
	_, err = db.Exec(modifiedSQL)
	c.Assert(err, qt.IsNil, qt.Commentf("modified migration SQL must execute: %s", modifiedSQL))

	read, err = reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	modifiedFiltered := filterSchemaObjects(read)
	modifiedRoundTrip := schemadiff.Compare(modified, modifiedFiltered)
	c.Assert(modifiedRoundTrip.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", modifiedRoundTrip))

	tableOnly := schemaObjectsTableOnly()
	removalDiff := schemadiff.Compare(tableOnly, modifiedFiltered)
	c.Assert(removalDiff.ViewsRemoved, qt.DeepEquals, []string{"ptah_schema_objects_active_users"})
	c.Assert(removalDiff.MaterializedViewsRemoved, qt.DeepEquals, []string{"ptah_schema_objects_user_stats"})
	c.Assert(removalDiff.TriggersRemoved, qt.DeepEquals, []difftypes.TriggerRef{{
		TriggerName: "ptah_schema_objects_set_updated_at",
		TableName:   "ptah_schema_objects_users",
	}})

	removalStatements, err := planner.GenerateSchemaDiffSQLStatements(removalDiff, tableOnly, "postgres")
	c.Assert(err, qt.IsNil)
	removalSQL := strings.Join(removalStatements, ";\n") + ";"
	_, err = db.Exec(removalSQL)
	c.Assert(err, qt.IsNil, qt.Commentf("removal migration SQL must execute: %s", removalSQL))

	read, err = reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	removedFiltered := filterSchemaObjects(read)
	removedRoundTrip := schemadiff.Compare(tableOnly, removedFiltered)
	c.Assert(removedRoundTrip.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", removedRoundTrip))
}

func schemaObjectsTarget() *goschema.Database {
	db := &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "SchemaObjectUser",
			Name:       "ptah_schema_objects_users",
		}},
		Fields: []goschema.Field{
			{StructName: "SchemaObjectUser", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "SchemaObjectUser", Name: "deleted_at", Type: "TIMESTAMP"},
			{StructName: "SchemaObjectUser", Name: "updated_at", Type: "TIMESTAMP"},
		},
		Views: []goschema.View{{
			Name: "ptah_schema_objects_active_users",
			Body: "SELECT id, updated_at FROM ptah_schema_objects_users WHERE deleted_at IS NULL",
		}},
		MaterializedViews: []goschema.MaterializedView{{
			Name:            "ptah_schema_objects_user_stats",
			Body:            "SELECT id, COUNT(*) FROM ptah_schema_objects_users GROUP BY id",
			RefreshStrategy: "manual",
		}},
		Triggers: []goschema.Trigger{{
			Name:   "ptah_schema_objects_set_updated_at",
			Table:  "ptah_schema_objects_users",
			Timing: "BEFORE",
			Event:  "UPDATE",
			Body:   "NEW.updated_at = NOW(); RETURN NEW;",
		}},
	}
	goschema.Finalize(db)
	return db
}

func schemaObjectsTableOnly() *goschema.Database {
	db := schemaObjectsTarget()
	db.Views = nil
	db.MaterializedViews = nil
	db.Triggers = nil
	return db
}

func cleanupSchemaObjects(t *testing.T, db *sql.DB) {
	t.Helper()
	_, _ = db.Exec("DROP MATERIALIZED VIEW IF EXISTS ptah_schema_objects_user_stats CASCADE")
	_, _ = db.Exec("DROP VIEW IF EXISTS ptah_schema_objects_active_users CASCADE")
	_, _ = db.Exec("DROP TABLE IF EXISTS ptah_schema_objects_users CASCADE")
	_, _ = db.Exec("DROP FUNCTION IF EXISTS ptah_trigger_ptah_schema_objects_users_ptah_schema_objects_set_updated_at()")
}

func filterSchemaObjects(in *dbschematypes.DBSchema) *dbschematypes.DBSchema {
	out := &dbschematypes.DBSchema{}
	for _, table := range in.Tables {
		if table.Name == "ptah_schema_objects_users" {
			out.Tables = append(out.Tables, table)
		}
	}
	for _, view := range in.Views {
		if view.Name == "ptah_schema_objects_active_users" {
			out.Views = append(out.Views, view)
		}
	}
	for _, view := range in.MatViews {
		if view.Name == "ptah_schema_objects_user_stats" {
			out.MatViews = append(out.MatViews, view)
		}
	}
	for _, trigger := range in.Triggers {
		if trigger.Name == "ptah_schema_objects_set_updated_at" {
			out.Triggers = append(out.Triggers, trigger)
		}
	}
	return out
}
