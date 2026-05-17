//go:build integration

package gonative_test

import (
	"database/sql"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/stokaro/ptah/core/convert/fromschema"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/dbschema/postgres"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// TestFieldLevelCheckConstraint_RoundTrip_Integration is the e2e backfill for
// PR #123 (issue #112). PR #123 introduced field-level `check=` / `check_name=`
// annotations with drift detection in the diff layer, but landed without a
// real-database round-trip — the schemadiff "trust the name, don't compare the
// expression" decision was unit-tested but never exercised against the live
// PostgreSQL clause normalization that motivated it (parens, type casts, the
// `IN (...)` → `= ANY (ARRAY[...])` rewrite).
//
// This test installs a table with a field-level CHECK via the renderer, reads
// the live schema back, and runs the diff against the same Go definition to
// confirm idempotency holds against Postgres' own normalized clause text.
func TestFieldLevelCheckConstraint_RoundTrip_Integration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, _ = db.Exec("DROP TABLE IF EXISTS ptah_test_files CASCADE")
	defer func() { _, _ = db.Exec("DROP TABLE IF EXISTS ptah_test_files CASCADE") }()

	target := &goschema.Database{
		Tables: []goschema.Table{{Name: "ptah_test_files", StructName: "File"}},
		Fields: []goschema.Field{
			{StructName: "File", Name: "id", Type: "SERIAL", Primary: true},
			{
				StructName: "File",
				Name:       "category",
				Type:       "TEXT",
				Nullable:   false,
				Default:    "other",
				Check:      "category IN ('photos','invoices','documents','other')",
			},
			{
				StructName: "File",
				Name:       "kind",
				Type:       "TEXT",
				Nullable:   false,
				Check:      "kind IN ('image','document','video','audio','archive','other')",
				CheckName:  "ptah_test_files_kind_valid",
			},
		},
	}

	// Render the CREATE TABLE statement and apply it.
	stmts := fromschema.FromDatabase(*target, "postgres")
	sqlText, err := renderer.RenderSQL("postgres", stmts.Statements...)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Contains(sqlText, "CHECK (category IN"), qt.IsTrue)
	c.Assert(strings.Contains(sqlText, "CONSTRAINT ptah_test_files_kind_valid CHECK"), qt.IsTrue)

	_, err = db.Exec(sqlText)
	c.Assert(err, qt.IsNil, qt.Commentf("CREATE TABLE must execute: %s", sqlText))

	// Read the live schema and confirm both CHECK constraints landed.
	reader := postgres.NewPostgreSQLReader(db, "public")
	dbSchema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)

	checks := map[string]bool{}
	for _, cs := range dbSchema.Constraints {
		if cs.Type == "CHECK" && cs.TableName == "ptah_test_files" {
			checks[cs.Name] = true
		}
	}
	c.Assert(checks["ptah_test_files_category_check"], qt.IsTrue,
		qt.Commentf("auto-named CHECK should land under the <table>_<column>_check convention"))
	c.Assert(checks["ptah_test_files_kind_valid"], qt.IsTrue,
		qt.Commentf("explicit check_name= must be honored"))

	// Idempotency: re-diffing the live schema against the same target must
	// produce no changes. This is where the "trust the name" decision pays
	// off — Postgres rewrites the stored CHECK clause (parens, type casts,
	// `IN (...)` → `= ANY (ARRAY[...])`) and a naive text compare would
	// regen a migration on every run.
	diff := schemadiff.Compare(target, dbSchema)
	c.Assert(diff.HasChanges(), qt.IsFalse,
		qt.Commentf("round-trip diff must be clean; got added=%v removed=%v",
			diff.ConstraintsAdded, diff.ConstraintsRemoved))
}

// TestFieldLevelCheckConstraint_Removal_Integration exercises the drop path:
// the field's `check=` annotation goes away → the diff must report the
// synthesized constraint as removed → the migration must drop it.
func TestFieldLevelCheckConstraint_Removal_Integration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, _ = db.Exec("DROP TABLE IF EXISTS ptah_test_check_drop CASCADE")
	defer func() { _, _ = db.Exec("DROP TABLE IF EXISTS ptah_test_check_drop CASCADE") }()

	// Phase 1 — install table with the CHECK.
	withCheck := &goschema.Database{
		Tables: []goschema.Table{{Name: "ptah_test_check_drop", StructName: "Doc"}},
		Fields: []goschema.Field{
			{StructName: "Doc", Name: "id", Type: "SERIAL", Primary: true},
			{
				StructName: "Doc",
				Name:       "score",
				Type:       "INTEGER",
				Nullable:   false,
				Check:      "score >= 0",
			},
		},
	}
	stmts := fromschema.FromDatabase(*withCheck, "postgres")
	createSQL, err := renderer.RenderSQL("postgres", stmts.Statements...)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(createSQL)
	c.Assert(err, qt.IsNil)

	// Phase 2 — drop the CHECK from the Go definition and regenerate.
	withoutCheck := &goschema.Database{
		Tables: []goschema.Table{{Name: "ptah_test_check_drop", StructName: "Doc"}},
		Fields: []goschema.Field{
			{StructName: "Doc", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Doc", Name: "score", Type: "INTEGER", Nullable: false},
		},
	}

	reader := postgres.NewPostgreSQLReader(db, "public")
	dbSchema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)

	diff := schemadiff.Compare(withoutCheck, dbSchema)
	c.Assert(diff.HasChanges(), qt.IsTrue)
	c.Assert(diff.ConstraintsRemoved, qt.Contains, "ptah_test_check_drop_score_check")

	migrationSQL := strings.Join(
		planner.GenerateSchemaDiffSQLStatements(diff, withoutCheck, "postgres"),
		";\n",
	) + ";"
	c.Assert(strings.Contains(migrationSQL, "ptah_test_check_drop_score_check"), qt.IsTrue,
		qt.Commentf("drop migration must reference the synthesized constraint by its auto-name; got: %s", migrationSQL))

	_, err = db.Exec(migrationSQL)
	c.Assert(err, qt.IsNil, qt.Commentf("drop migration must execute: %s", migrationSQL))

	// Phase 3 — confirm the score CHECK is gone and a fresh diff is clean.
	// PostgreSQL surfaces synthesized NOT NULL checks under the same
	// information_schema CHECK type with `_not_null` suffix; those are not
	// part of this assertion.
	afterSchema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	for _, cs := range afterSchema.Constraints {
		if cs.TableName != "ptah_test_check_drop" || cs.Type != "CHECK" {
			continue
		}
		if strings.HasSuffix(cs.Name, "_not_null") {
			continue
		}
		c.Errorf("CHECK constraint should have been dropped, still found: %s", cs.Name)
	}

	idempDiff := schemadiff.Compare(withoutCheck, afterSchema)
	c.Assert(idempDiff.HasChanges(), qt.IsFalse,
		qt.Commentf("post-drop diff must be clean"))
}
