package atlasschema_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/safety"
)

// TestPreparePlanFile_PlansDeclaredRowsOnAFreshDatabase covers the case the
// operator meets first: the tables do not exist yet, so every declared row is
// an insert and nothing is read back from a table that is not there.
func TestPreparePlanFile_PlansDeclaredRowsOnAFreshDatabase(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "fresh.db")

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired: regionsSchema(regionRow("CZ", "Czechia", 2), regionRow("NO", "Norway", 1)),
	})

	c.Assert(err, qt.IsNil)
	sql := planSQL(plan)
	c.Assert(sql, qt.Contains, "CREATE TABLE")
	c.Assert(sql, qt.Contains, `INSERT INTO "regions"`)
	c.Assert(sql, qt.Contains, "'Norway'")
	c.Assert(plan.Destructive, qt.IsFalse)
	// The rows follow the table they go into.
	c.Assert(strings.Index(sql, "CREATE TABLE") < strings.Index(sql, "INSERT INTO"), qt.IsTrue)
}

// TestPreparePlanFile_PlansADataOnlyChange is the property the epic asks for by
// name: a release that edits a reference row changes no DDL, and a planner that
// stopped at the schema diff would report nothing to do.
func TestPreparePlanFile_PlansADataOnlyChange(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "data-only.db")
	applyPlan(c, conn, regionsSchema(regionRow("NO", "Norway", 1)))

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired: regionsSchema(regionRow("NO", "Norge", 1)),
	})

	c.Assert(err, qt.IsNil)
	sql := planSQL(plan)
	c.Assert(sql, qt.Not(qt.Contains), "CREATE TABLE")
	c.Assert(sql, qt.Contains, `UPDATE "regions"`)
	c.Assert(sql, qt.Contains, "'Norge'")
	c.Assert(planSeverity(plan, "UPDATE"), qt.Equals, safety.Warning)
	c.Assert(plan.Destructive, qt.IsFalse)
}

// TestPreparePlanFile_ADeletedRowIsDestructive is where the SQL analyzer and
// the truth disagree. It reads a DELETE of a reference row as safe, because it
// removes no table and tightens no constraint.
func TestPreparePlanFile_ADeletedRowIsDestructive(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "deleted-row.db")
	applyPlan(c, conn, regionsSchema(regionRow("CZ", "Czechia", 2), regionRow("NO", "Norway", 1)))

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired: regionsSchema(regionRow("NO", "Norway", 1)),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(planSQL(plan), qt.Contains, `DELETE FROM "regions"`)
	c.Assert(planSeverity(plan, "DELETE"), qt.Equals, safety.Destructive)
	c.Assert(plan.Destructive, qt.IsTrue)
}

// TestPreparePlanFile_ConvergedRowsPlanNothing keeps a repeated reconciliation
// from issuing DML that changes nothing.
func TestPreparePlanFile_ConvergedRowsPlanNothing(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "converged.db")
	desired := regionsSchema(regionRow("NO", "Norway", 1))
	applyPlan(c, conn, desired)

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{Desired: desired})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements, qt.HasLen, 0)
}

// TestPreparePlanFile_UnmanagedColumnsAreNotRewritten pins the ownership rule:
// reconciling a reference table is not permission to rewrite the columns beside
// the declared ones.
func TestPreparePlanFile_UnmanagedColumnsAreNotRewritten(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "unmanaged.db")
	desired := regionsSchema(regionRow("NO", "Norway", 1))
	applyPlan(c, conn, desired)
	_, err := conn.ExecContext(context.Background(), `UPDATE regions SET note = 'kept' WHERE code = 'NO'`)
	c.Assert(err, qt.IsNil)

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{Desired: desired})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements, qt.HasLen, 0, qt.Commentf("a column no declaration names is not a difference"))
	var note string
	row := conn.QueryRowContext(context.Background(), `SELECT note FROM regions WHERE code = 'NO'`)
	c.Assert(row.Scan(&note), qt.IsNil)
	c.Assert(note, qt.Equals, "kept")
}

// TestPreparePlanFile_RefusesRowsNobodyRead covers the nil-versus-empty
// distinction the whole publication path depends on. A declaration that names
// no file has nothing left to resolve: the layer that carried its rows was
// dropped between the artifact and here, and planning an empty set would delete
// every row the table holds.
func TestPreparePlanFile_RefusesRowsNobodyRead(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "unread.db")
	desired := regionsSchema()
	desired.ManagedData[0].Rows = nil
	desired.ManagedData[0].File = ""

	_, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{Desired: desired})

	c.Assert(err, qt.ErrorMatches, `managed data for table regions was never read.*`)
}

// TestPreparePlanFile_ReadsRowsADeclarationStillNames is the other half. A
// working copy carries a file name rather than the rows, and only publication
// used to resolve it, so planning refused a row set sitting next to the schema
// it was planning (stokaro/ptah#3269).
func TestPreparePlanFile_ReadsRowsADeclarationStillNames(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "declared.db")
	sourceDir := c.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(sourceDir, "regions.yaml"),
		[]byte("- code: CZ\n  name: Czechia\n  rank: 1\n"),
		0o600,
	), qt.IsNil)
	desired := regionsSchema()
	desired.ManagedData[0].Rows = nil
	desired.ManagedData[0].SourceDir = sourceDir

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{Desired: desired})

	c.Assert(err, qt.IsNil)
	declared := make([]string, 0, len(plan.Statements))
	for _, statement := range plan.Statements {
		declared = append(declared, statement.SQL)
	}
	c.Assert(strings.Join(declared, "\n"), qt.Contains, "'CZ'")
}

// TestPreparePlanFile_RefusesARowFileOutsideTheProject keeps the read from
// widening what a desired state can reach. The path is data, and a schema is not
// always one the reader wrote, so it is bounded by the project the caller named
// -- the boundary atlas.hcl's file() already has.
//
// The boundary is the project rather than the declaration's own directory
// because `schema export` writes a path back out of the directory it exports
// into, and a rule that refuses the tool's own output is a rule nobody can keep.
func TestPreparePlanFile_RefusesARowFileOutsideTheProject(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "escape.db")
	project := c.TempDir()
	sourceDir := filepath.Join(project, "models")
	c.Assert(os.MkdirAll(sourceDir, 0o750), qt.IsNil)
	desired := regionsSchema()
	desired.ManagedData[0].Rows = nil
	desired.ManagedData[0].SourceDir = sourceDir
	desired.ManagedData[0].File = filepath.Join("..", "..", "secret.yaml")

	_, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired: desired, ProjectRoot: project,
	})

	c.Assert(err, qt.ErrorMatches, `.*outside.*`)
}

// TestPreparePlanFile_ResolvesAnAbsoluteRowFileUnderItsSource records why the
// refusal above names only one shape. An absolute file is not a second escape:
// the loader joins it to the source directory, and filepath.Join drops the
// leading separator, so `/etc/passwd` reaches for `<source>/etc/passwd` and
// leaves the project no more than a relative name does. Asserting a refusal for
// it would be asserting a branch nothing can reach.
func TestPreparePlanFile_ResolvesAnAbsoluteRowFileUnderItsSource(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "absolute.db")
	project := c.TempDir()
	sourceDir := filepath.Join(project, "models")
	c.Assert(os.MkdirAll(filepath.Join(sourceDir, "etc"), 0o750), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(sourceDir, "etc", "passwd"),
		[]byte("- code: CZ\n  name: Czechia\n  rank: 1\n"),
		0o600,
	), qt.IsNil)
	desired := regionsSchema()
	desired.ManagedData[0].Rows = nil
	desired.ManagedData[0].SourceDir = sourceDir
	desired.ManagedData[0].File = filepath.Join(string(filepath.Separator), "etc", "passwd")

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired: desired, ProjectRoot: project,
	})

	c.Assert(err, qt.IsNil, qt.Commentf("the absolute name was read under the source directory"))
	declared := make([]string, 0, len(plan.Statements))
	for _, statement := range plan.Statements {
		declared = append(declared, statement.SQL)
	}
	c.Assert(strings.Join(declared, "\n"), qt.Contains, "'CZ'")
}

// TestPreparePlanFile_ReadsARowFileTheProjectStillContains is the control that
// keeps the boundary from being a ban on relative paths. `schema export` writes
// exactly this shape: the HCL in one directory of the project, the rows in
// another, and the declaration pointing across.
func TestPreparePlanFile_ReadsARowFileTheProjectStillContains(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "sibling.db")
	project := c.TempDir()
	models := filepath.Join(project, "models")
	exported := filepath.Join(project, "schema")
	c.Assert(os.MkdirAll(models, 0o750), qt.IsNil)
	c.Assert(os.MkdirAll(exported, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(models, "regions.yaml"),
		[]byte("- code: CZ\n  name: Czechia\n  rank: 1\n"),
		0o600,
	), qt.IsNil)
	desired := regionsSchema()
	desired.ManagedData[0].Rows = nil
	desired.ManagedData[0].SourceDir = exported
	desired.ManagedData[0].File = filepath.Join("..", "models", "regions.yaml")

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired: desired, ProjectRoot: project,
	})

	c.Assert(err, qt.IsNil)
	declared := make([]string, 0, len(plan.Statements))
	for _, statement := range plan.Statements {
		declared = append(declared, statement.SQL)
	}
	c.Assert(strings.Join(declared, "\n"), qt.Contains, "'CZ'")
}

func managedDataConnection(c *qt.C, name string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+filepath.Join(c.TempDir(), name))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

func regionsSchema(rows ...schemamodel.ManagedRow) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Region", Name: "regions"}},
		Fields: []schemamodel.Field{
			{StructName: "Region", FieldName: "Code", Name: "code", Type: "TEXT", Primary: true},
			{StructName: "Region", FieldName: "Name", Name: "name", Type: "TEXT"},
			{StructName: "Region", FieldName: "Rank", Name: "rank", Type: "INTEGER", Nullable: true},
			{StructName: "Region", FieldName: "Note", Name: "note", Type: "TEXT", Nullable: true},
		},
		ManagedData: []schemamodel.ManagedData{{
			StructName: "Region",
			Table:      "regions",
			Keys:       []string{"code"},
			File:       "regions.yaml",
			Rows:       append(make([]schemamodel.ManagedRow, 0, len(rows)), rows...),
		}},
	}
	schemamodel.Finalize(db)
	return db
}

func regionRow(code, name string, rank int) schemamodel.ManagedRow {
	return schemamodel.ManagedRow{
		"code": {Tag: "str", Text: code},
		"name": {Tag: "str", Text: name},
		"rank": {Tag: "int", Text: itoa(rank)},
	}
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := ""
	for value > 0 {
		digits = string(rune('0'+value%10)) + digits
		value /= 10
	}
	return digits
}

func applyPlan(c *qt.C, conn *dbschema.DatabaseConnection, desired *schemamodel.Database) {
	c.Helper()
	plan, err := atlasschema.PlanApply(context.Background(), conn, atlasschema.ApplyOptions{Desired: desired})
	c.Assert(err, qt.IsNil)
	for _, statement := range plan.Statements() {
		_, err := conn.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

func planSQL(plan atlasschema.PlanFile) string {
	parts := make([]string, 0, len(plan.Statements))
	for _, statement := range plan.Statements {
		parts = append(parts, statement.SQL)
	}
	return strings.Join(parts, "\n")
}

func planSeverity(plan atlasschema.PlanFile, prefix string) safety.Severity {
	for _, statement := range plan.Statements {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(statement.SQL)), prefix) {
			return statement.Severity
		}
	}
	return safety.Safe
}
