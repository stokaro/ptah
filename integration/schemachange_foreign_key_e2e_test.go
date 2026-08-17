//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/schemachange"
	"go.5x5.cz/ptah/internal/schemastate"
)

// TestSchemaChangeForeignKeyPipelinePostgresE2E applies the prototype's plan to
// a real PostgreSQL and reads the result back out of the catalog.
//
// stokaro/ptah#1350 requires live execution for the slice's representative
// target behaviors, and the reason is specific: every offline test in
// internal/schemachange asserts against a fixture somebody wrote, so a plan
// that is internally consistent and wrong about PostgreSQL passes all of them.
// This is the test that cannot.
//
// It runs in its own database. Applying a desired schema to a shared one drops
// whatever the description does not declare, and the shared server is where
// other suites keep their fixtures.
func TestSchemaChangeForeignKeyPipelinePostgresE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_fk_slice_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, adminURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, err = db.ExecContext(ctx, `CREATE TABLE parent (id integer PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, `CREATE TABLE child (id integer PRIMARY KEY, parent_id integer)`)
	c.Assert(err, qt.IsNil)

	profile := livePostgresProfile()

	// The addition. Its statement is applied, and the catalog is asked what it
	// produced rather than the plan being trusted.
	addition := planFor(c, liveDescription("CASCADE"), liveCatalog(nil), profile)
	c.Assert(addition, qt.HasLen, 1)
	_, err = db.ExecContext(ctx, addition[0].SQL)
	c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", addition[0].SQL))
	c.Assert(liveDeleteRule(c, ctx, db), qt.Equals, "CASCADE")

	// The modification. One change, two statements, and the engine has to
	// accept them in the order the plan emitted them.
	modification := planFor(c, liveDescription("SET NULL"), liveCatalog(stringPointer("CASCADE")), profile)
	c.Assert(modification, qt.HasLen, 2)
	c.Assert(modification[0].Change.Operation, qt.Equals, schemachange.Modify)
	for _, operation := range modification {
		_, execErr := db.ExecContext(ctx, operation.SQL)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement: %s", operation.SQL))
	}
	c.Assert(liveDeleteRule(c, ctx, db), qt.Equals, "SET NULL")

	// The removal.
	removal := planFor(c, liveDescriptionWithoutForeignKey(), liveCatalog(stringPointer("SET NULL")), profile)
	c.Assert(removal, qt.HasLen, 1)
	_, err = db.ExecContext(ctx, removal[0].SQL)
	c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", removal[0].SQL))
	c.Assert(liveConstraintCount(c, ctx, db), qt.Equals, 0)
}

// TestSchemaChangeForeignKeyRefusalIsTheEnginesPostgresE2E pins that the
// prototype's blocked answer matches what the engine does, rather than being a
// rule this repository invented.
//
// A foreign key whose referenced column carries no unique constraint is blocked
// offline; here the same statement is handed to PostgreSQL, which must refuse
// it. If the engine accepted it, the block would be Ptah refusing something the
// target supports.
func TestSchemaChangeForeignKeyRefusalIsTheEnginesPostgresE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_fk_slice_refusal_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, adminURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	// No PRIMARY KEY on the parent, which is the fact the block reads.
	_, err = db.ExecContext(ctx, `CREATE TABLE parent (id integer)`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, `CREATE TABLE child (id integer PRIMARY KEY, parent_id integer)`)
	c.Assert(err, qt.IsNil)

	description := liveDescription("")
	description.Fields[0].Primary = false
	changes := changesFor(c, description, liveCatalog(nil), livePostgresProfile())
	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Blocked)

	// The engine's own answer to the statement the block prevented.
	_, engineErr := db.ExecContext(ctx,
		`ALTER TABLE "child" ADD CONSTRAINT "fk_child_parent" FOREIGN KEY ("parent_id") REFERENCES "parent"("id")`)

	c.Assert(engineErr, qt.IsNotNil)
	c.Assert(engineErr.Error(), qt.Contains, "unique constraint")
}

func livePostgresProfile() schemastate.Profile {
	return schemastate.Profile{
		Dialect:      "postgres",
		Semantics:    identifier.ForDialect("postgres"),
		Capabilities: capability.Postgres17(),
	}
}

// planFor runs the whole prototype and returns its rendered operations.
func planFor(
	c *qt.C,
	description *goschema.Database,
	catalog *dbschematypes.DBSchema,
	profile schemastate.Profile,
) []schemachange.PlannedOperation {
	c.Helper()
	operations, err := schemachange.Plan(changesFor(c, description, catalog, profile), profile)
	c.Assert(err, qt.IsNil)
	return operations
}

// changesFor runs the prototype up to the ordered change list, for the row that
// asserts on a blocked change rather than on statements.
func changesFor(
	c *qt.C,
	description *goschema.Database,
	catalog *dbschematypes.DBSchema,
	profile schemastate.Profile,
) []schemachange.Change {
	c.Helper()
	rawDesired, err := schemastate.FromDescription(description, profile.Dialect, profile.Semantics)
	c.Assert(err, qt.IsNil)
	rawCurrent, err := schemastate.FromCatalog(catalog, profile.Dialect, profile.Semantics)
	c.Assert(err, qt.IsNil)
	desired, err := schemastate.Normalize(rawDesired, profile)
	c.Assert(err, qt.IsNil)
	current, err := schemastate.Normalize(rawCurrent, profile)
	c.Assert(err, qt.IsNil)
	changes, err := schemachange.Compare(current, desired, profile)
	c.Assert(err, qt.IsNil)
	graph, err := schemachange.BuildGraph(changes, current, desired)
	c.Assert(err, qt.IsNil)
	ordered, err := graph.Forward()
	c.Assert(err, qt.IsNil)
	return ordered
}

func liveDescription(onDelete string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parent"},
			{StructName: "Child", Name: "child"},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "id", Type: "integer", Primary: true},
			{StructName: "Child", Name: "id", Type: "integer", Primary: true},
			{
				StructName:     "Child",
				Name:           "parent_id",
				Type:           "integer",
				Foreign:        "parent(id)",
				ForeignKeyName: "fk_child_parent",
				OnDelete:       onDelete,
			},
		},
	}
}

func liveDescriptionWithoutForeignKey() *goschema.Database {
	description := liveDescription("")
	description.Fields[2].Foreign = ""
	description.Fields[2].ForeignKeyName = ""
	return description
}

// liveCatalog is the database as it stands before each step. A nil delete rule
// means the foreign key is not there yet.
func liveCatalog(deleteRule *string) *dbschematypes.DBSchema {
	schema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "parent", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "child", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "parent_id", DataType: "integer", IsNullable: "YES"},
			}},
		},
	}
	if deleteRule == nil {
		return schema
	}
	parent := "parent"
	column := "id"
	schema.Constraints = []dbschematypes.DBConstraint{{
		Name:          "fk_child_parent",
		TableName:     "child",
		Schema:        "public",
		Type:          "FOREIGN KEY",
		ColumnName:    "parent_id",
		ColumnNames:   []string{"parent_id"},
		ForeignTable:  &parent,
		ForeignSchema: "public",
		ForeignColumn: &column,
		DeleteRule:    deleteRule,
	}}
	return schema
}

// liveDeleteRule reads back what the engine recorded, which is the assertion
// that a fixture cannot fake.
func liveDeleteRule(c *qt.C, ctx context.Context, db *sql.DB) string {
	c.Helper()
	var rule string
	err := db.QueryRowContext(ctx, `
		SELECT rc.delete_rule
		FROM information_schema.referential_constraints rc
		WHERE rc.constraint_name = 'fk_child_parent'`).Scan(&rule)
	c.Assert(err, qt.IsNil)
	return rule
}

func liveConstraintCount(c *qt.C, ctx context.Context, db *sql.DB) int {
	c.Helper()
	var count int
	err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM information_schema.table_constraints
		WHERE constraint_name = 'fk_child_parent'`).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func stringPointer(value string) *string {
	return &value
}
