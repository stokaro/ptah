package atlasreport_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
)

// The PostgreSQL planner ends a plan that drops a table's last RLS policy on a
// note, a statement that is only a comment. The `sql` template function writes
// each executable statement with a semicolon and the note without one: a
// semicolon after a comment ends nothing and read as "--;" in the migration
// file a reviewer approves (stokaro/ptah#3903).
//
// migrate diff, schema diff, schema apply and schema plan each have their own
// `sql`, so each is asserted.
func TestSQLTemplate_ANoteWithNothingAfterItEndsWithoutASemicolon(t *testing.T) {
	c := qt.New(t)
	drop := "-- Drop RLS policy tenant_only from table site_media_settings\n" +
		`DROP POLICY IF EXISTS "tenant_only" ON "site_media_settings"`
	note := "-- NOTE: RLS policies were removed from table site_media_settings - verify if RLS should be disabled"
	statements := []string{drop, note}
	want := "  -- Drop RLS policy tenant_only from table site_media_settings\n" +
		"  DROP POLICY IF EXISTS \"tenant_only\" ON \"site_media_settings\";\n" +
		"  -- NOTE: RLS policies were removed from table site_media_settings - verify if RLS should be disabled\n"

	var migrateDiff bytes.Buffer
	c.Assert(atlasreport.WriteMigrateDiff(&migrateDiff, atlasreport.NormalizeMigrateDiffFormat(""),
		atlasreport.NewSchemaDiff(nil, nil, statements)), qt.IsNil)
	c.Assert(migrateDiff.String(), qt.Equals, want)

	var schemaDiff bytes.Buffer
	c.Assert(atlasreport.WriteSchemaDiff(&schemaDiff, `{{ sql . "  " }}`, atlasreport.NewSchemaDiff(nil, nil, statements)), qt.IsNil)
	c.Assert(schemaDiff.String(), qt.Equals, want)

	apply, err := atlasreport.NewSchemaApply(atlasreport.SchemaApplyOptions{Statements: statements}).MarshalSQL("  ")
	c.Assert(err, qt.IsNil)
	c.Assert(apply, qt.Equals, want)

	plan, err := atlasreport.NewSchemaPlan(atlasreport.SchemaPlanOptions{
		Statements: []atlasreport.SchemaPlanChange{{Cmd: drop}, {Cmd: note}},
	}).MarshalSQL("  ")
	c.Assert(err, qt.IsNil)
	c.Assert(plan, qt.Equals, want)
}
