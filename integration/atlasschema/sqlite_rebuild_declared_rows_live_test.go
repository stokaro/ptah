//go:build integration

package atlasschema_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
)

// A SQLite table rebuild drops the original table, and that DROP violates any
// foreign key that references it. The plan disables enforcement around the
// rebuild. The apply honors that only when the plan starts with the disabling
// pragma and ends with the enabling one: PRAGMA foreign_keys is silently
// ignored inside a transaction, so the apply suspends enforcement on the
// connection instead of executing it.
//
// A declared row placed after the enabling pragma ends the plan in its place.
// The apply then opens an ordinary transaction and the rebuild runs with
// enforcement on. Measured on the compiled-in SQLite against a file database
// (stokaro/ptah#3282):
//
//	failed to execute SQL statement: sqlite: SQL execution failed: constraint
//	failed: FOREIGN KEY constraint failed (787)
//	SQL: DROP TABLE "regions"
//
// and, where the reference cascades, a commit that removed every referencing
// row the declaration still held.
//
// Every test below applies twice. The first apply creates both tables and their
// rows. The second drops a column from the referenced table, which SQLite can
// only do by rebuilding it. The engine answers each one; a file database is the
// target because a rebuild on an in-memory one shares nothing with a real
// deployment's connection handling.

// TestSQLiteRebuildAppliesWithDeclaredRowsLive is the reported failure: the
// rebuild and a new declared row in one plan, applied the way `ptah schema
// apply` applies it.
func TestSQLiteRebuildAppliesWithDeclaredRowsLive(t *testing.T) {
	c := qt.New(t)
	ctx, conn := rebuildRowsDatabase(c)
	rebuildRowsApply(c, ctx, conn, rebuildRowsSchema(rebuildRowsFixture{
		legacyNote: true,
		countries:  []string{"CZ", "SK"},
	}))

	err := rebuildRowsExecute(c, ctx, conn, rebuildRowsSchema(rebuildRowsFixture{
		countries: []string{"AT", "CZ", "SK"},
	}))

	c.Assert(err, qt.IsNil)
	c.Assert(rebuildRowsCountries(c, ctx, conn), qt.DeepEquals, []string{"AT", "CZ", "SK"})
	c.Assert(rebuildRowsHasLegacyNote(c, ctx, conn), qt.IsFalse)
}

// TestSQLiteRebuildAppliesPlanStatementsWithDeclaredRowsLive applies the same
// plan as its statement list, which is what `ptah-compat schema apply` and the
// dev-database rehearsal execute. Those statements keep the comment the planner
// writes above each pragma, so the bracket has to be recognized through it.
func TestSQLiteRebuildAppliesPlanStatementsWithDeclaredRowsLive(t *testing.T) {
	c := qt.New(t)
	ctx, conn := rebuildRowsDatabase(c)
	rebuildRowsApply(c, ctx, conn, rebuildRowsSchema(rebuildRowsFixture{
		legacyNote: true,
		countries:  []string{"CZ", "SK"},
	}))

	err := rebuildRowsApplyStatements(c, ctx, conn, rebuildRowsSchema(rebuildRowsFixture{
		countries: []string{"AT", "CZ", "SK"},
	}))

	c.Assert(err, qt.IsNil)
	c.Assert(rebuildRowsCountries(c, ctx, conn), qt.DeepEquals, []string{"AT", "CZ", "SK"})
	c.Assert(rebuildRowsHasLegacyNote(c, ctx, conn), qt.IsFalse)
}

// TestSQLiteRebuildAppliesASavedPlanWithDeclaredRowsLive applies the statements
// a saved plan records, which carry the declared rows beside the DDL in an
// order of their own.
func TestSQLiteRebuildAppliesASavedPlanWithDeclaredRowsLive(t *testing.T) {
	c := qt.New(t)
	ctx, conn := rebuildRowsDatabase(c)
	rebuildRowsApply(c, ctx, conn, rebuildRowsSchema(rebuildRowsFixture{
		legacyNote: true,
		countries:  []string{"CZ", "SK"},
	}))

	err := rebuildRowsApplySavedPlan(c, ctx, conn, rebuildRowsSchema(rebuildRowsFixture{
		countries: []string{"AT", "CZ", "SK"},
	}))

	c.Assert(err, qt.IsNil)
	c.Assert(rebuildRowsCountries(c, ctx, conn), qt.DeepEquals, []string{"AT", "CZ", "SK"})
	c.Assert(rebuildRowsHasLegacyNote(c, ctx, conn), qt.IsFalse)
}

// TestSQLiteRebuildKeepsCascadedRowsWithDeclaredRowsLive is the silent form of
// the same defect. With ON DELETE CASCADE the DROP inside the rebuild does not
// fail: with enforcement on it deletes every referencing row first, and the
// apply commits.
func TestSQLiteRebuildKeepsCascadedRowsWithDeclaredRowsLive(t *testing.T) {
	c := qt.New(t)
	ctx, conn := rebuildRowsDatabase(c)
	rebuildRowsApply(c, ctx, conn, rebuildRowsSchema(rebuildRowsFixture{
		legacyNote: true,
		onDelete:   "CASCADE",
		countries:  []string{"CZ", "SK"},
	}))

	err := rebuildRowsExecute(c, ctx, conn, rebuildRowsSchema(rebuildRowsFixture{
		onDelete:  "CASCADE",
		countries: []string{"AT", "CZ", "SK"},
	}))

	c.Assert(err, qt.IsNil)
	c.Assert(rebuildRowsCountries(c, ctx, conn), qt.DeepEquals, []string{"AT", "CZ", "SK"})
}

// TestSQLiteRebuildAppliesWithoutDeclaredRowsLive is the control: the same
// rebuild over the same referencing rows, with no data block in the second
// declaration. It isolates the defect to the declared rows rather than to
// rebuilds with inbound references in general.
func TestSQLiteRebuildAppliesWithoutDeclaredRowsLive(t *testing.T) {
	c := qt.New(t)
	ctx, conn := rebuildRowsDatabase(c)
	rebuildRowsApply(c, ctx, conn, rebuildRowsSchema(rebuildRowsFixture{
		legacyNote: true,
		countries:  []string{"CZ", "SK"},
	}))

	err := rebuildRowsExecute(c, ctx, conn, rebuildRowsSchema(rebuildRowsFixture{
		withoutData: true,
	}))

	c.Assert(err, qt.IsNil)
	c.Assert(rebuildRowsCountries(c, ctx, conn), qt.DeepEquals, []string{"CZ", "SK"})
	c.Assert(rebuildRowsHasLegacyNote(c, ctx, conn), qt.IsFalse)
}

// TestSQLiteRebuildAppliesPlanStatementsWithoutDeclaredRowsLive is the control
// through the statement list. No row is involved, so what it measures is the
// comment in front of each pragma alone.
func TestSQLiteRebuildAppliesPlanStatementsWithoutDeclaredRowsLive(t *testing.T) {
	c := qt.New(t)
	ctx, conn := rebuildRowsDatabase(c)
	rebuildRowsApply(c, ctx, conn, rebuildRowsSchema(rebuildRowsFixture{
		legacyNote: true,
		countries:  []string{"CZ", "SK"},
	}))

	err := rebuildRowsApplyStatements(c, ctx, conn, rebuildRowsSchema(rebuildRowsFixture{
		withoutData: true,
	}))

	c.Assert(err, qt.IsNil)
	c.Assert(rebuildRowsCountries(c, ctx, conn), qt.DeepEquals, []string{"CZ", "SK"})
	c.Assert(rebuildRowsHasLegacyNote(c, ctx, conn), qt.IsFalse)
}

// TestSQLiteRebuildRefusesADeclaredRowWithoutItsParentLive is what running the
// declared rows inside the bracket costs, and why the cost is bounded. The rows
// run with enforcement suspended, so an INSERT naming a region nobody declared
// is not refused where it stands. The foreign-key check the apply runs before
// it commits refuses it instead, and the whole plan rolls back.
func TestSQLiteRebuildRefusesADeclaredRowWithoutItsParentLive(t *testing.T) {
	c := qt.New(t)
	ctx, conn := rebuildRowsDatabase(c)
	rebuildRowsApply(c, ctx, conn, rebuildRowsSchema(rebuildRowsFixture{
		legacyNote: true,
		countries:  []string{"CZ", "SK"},
	}))

	err := rebuildRowsExecute(c, ctx, conn, rebuildRowsSchema(rebuildRowsFixture{
		countries:    []string{"CZ", "SK"},
		orphanRegion: "apac",
	}))

	c.Assert(err, qt.ErrorMatches,
		`commit schema apply transaction: sqlite: rebuild left 1 unresolved foreign-key reference\(s\): countries references missing regions`)
	c.Assert(rebuildRowsCountries(c, ctx, conn), qt.DeepEquals, []string{"CZ", "SK"})
	c.Assert(rebuildRowsHasLegacyNote(c, ctx, conn), qt.IsTrue)
}

// rebuildRowsFixture varies the declaration between the two applies.
type rebuildRowsFixture struct {
	// legacyNote keeps the column whose removal forces the rebuild.
	legacyNote bool
	// onDelete is the referencing key's ON DELETE action.
	onDelete string
	// countries are the declared country codes, each in region emea.
	countries []string
	// orphanRegion, when set, declares one more country, JP, in a region no
	// row declares.
	orphanRegion string
	// withoutData declares no rows at all.
	withoutData bool
}

func rebuildRowsDatabase(c *qt.C) (context.Context, *dbschema.DatabaseConnection) {
	c.Helper()
	ctx, cancel := context.WithTimeout(c.Context(), time.Minute)
	c.Cleanup(cancel)

	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(c.TempDir(), "rebuild-rows-3282.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return ctx, conn
}

// rebuildRowsSchema is regions, and countries referencing them. "countries"
// sorts first, so a plan ordered by table name rather than by the foreign key
// would fail for a reason unrelated to the rebuild.
func rebuildRowsSchema(fixture rebuildRowsFixture) *schemamodel.Database {
	fields := []schemamodel.Field{
		{StructName: "Region", FieldName: "Code", Name: "code", Type: "TEXT", Primary: true},
		{StructName: "Region", FieldName: "Name", Name: "name", Type: "TEXT"},
	}
	if fixture.legacyNote {
		fields = append(fields, schemamodel.Field{
			StructName: "Region", FieldName: "LegacyNote", Name: "legacy_note", Type: "TEXT", Nullable: true,
		})
	}
	fields = append(fields,
		schemamodel.Field{StructName: "Country", FieldName: "Code", Name: "code", Type: "TEXT", Primary: true},
		schemamodel.Field{StructName: "Country", FieldName: "Name", Name: "name", Type: "TEXT"},
		schemamodel.Field{
			StructName:     "Country",
			FieldName:      "RegionCode",
			Name:           "region_code",
			Type:           "TEXT",
			Foreign:        "regions(code)",
			ForeignKeyName: "fk_countries_region",
			OnDelete:       fixture.onDelete,
		},
	)

	countries := make([]schemamodel.ManagedRow, 0, len(fixture.countries)+1)
	for _, code := range fixture.countries {
		countries = append(countries, rebuildRowsCountry(code, "emea"))
	}
	if fixture.orphanRegion != "" {
		countries = append(countries, rebuildRowsCountry("JP", fixture.orphanRegion))
	}

	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Country", Name: "countries"},
			{StructName: "Region", Name: "regions"},
		},
		Fields: fields,
	}
	if !fixture.withoutData {
		db.ManagedData = []schemamodel.ManagedData{
			{
				StructName: "Country",
				Table:      "countries",
				Keys:       []string{"code"},
				File:       "countries.yaml",
				Rows:       countries,
			},
			{
				StructName: "Region",
				Table:      "regions",
				Keys:       []string{"code"},
				File:       "regions.yaml",
				Rows: []schemamodel.ManagedRow{{
					"code": {Tag: "str", Text: "emea"},
					"name": {Tag: "str", Text: "Europe, Middle East and Africa"},
				}},
			},
		}
	}
	schemamodel.Finalize(db)
	return db
}

func rebuildRowsCountry(code, region string) schemamodel.ManagedRow {
	return schemamodel.ManagedRow{
		"code":        {Tag: "str", Text: code},
		"name":        {Tag: "str", Text: "Country " + code},
		"region_code": {Tag: "str", Text: region},
	}
}

func rebuildRowsApply(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, desired *schemamodel.Database) {
	c.Helper()
	c.Assert(rebuildRowsExecute(c, ctx, conn, desired), qt.IsNil)
}

// rebuildRowsExecute runs the apply the way `ptah schema apply` does: the
// prepared plan, executed in one transaction for the whole plan.
func rebuildRowsExecute(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
) error {
	c.Helper()
	return rebuildRowsPrepare(c, ctx, conn, desired).Execute(ctx)
}

// rebuildRowsApplyStatements runs the prepared plan's statement list, comments
// and all.
func rebuildRowsApplyStatements(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
) error {
	c.Helper()
	plan := rebuildRowsPrepare(c, ctx, conn, desired)
	conn.SchemaWriter().SetDryRun(false)
	return atlasschema.ApplyStatements(ctx, conn, migrator.MigrationTxModeFile, plan.Statements())
}

// rebuildRowsApplySavedPlan runs the statements a saved plan records.
func rebuildRowsApplySavedPlan(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
) error {
	c.Helper()
	plan, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{Desired: desired})
	c.Assert(err, qt.IsNil)
	statements := make([]string, 0, len(plan.Statements))
	for _, statement := range plan.Statements {
		statements = append(statements, statement.SQL)
	}
	conn.SchemaWriter().SetDryRun(false)
	return atlasschema.ApplyStatements(ctx, conn, migrator.MigrationTxModeFile, statements)
}

func rebuildRowsPrepare(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
) atlasschema.ApplyRuntimePlan {
	c.Helper()
	plan, err := atlasschema.PrepareApply(ctx, conn, atlasschema.ApplyRuntimeOptions{
		Desired: desired,
		TxMode:  migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	c.Logf("plan:\n%s", plan.SQL())
	return plan
}

func rebuildRowsCountries(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, `SELECT "code" FROM "countries" ORDER BY "code"`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	codes := []string{}
	for rows.Next() {
		code := ""
		c.Assert(rows.Scan(&code), qt.IsNil)
		codes = append(codes, code)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return codes
}

func rebuildRowsHasLegacyNote(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) bool {
	c.Helper()
	var count int
	err := conn.QueryRowContext(ctx,
		`SELECT count(*) FROM pragma_table_info('regions') WHERE name = 'legacy_note'`,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}
