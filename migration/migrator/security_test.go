package migrator

import (
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// fmtVerbRe matches any Go fmt verb shape (`%s`, `%d`, `%v`, `%+v`, `%5.3f`,
// `%-10q`, …). The character class at the end is the union of Go's printf
// verbs so a sloppy revert using `%+v` or `%5s` is caught alongside the
// obvious `%s`/`%d` form.
var fmtVerbRe = regexp.MustCompile(`%[#+\- 0]*\d*(?:\.\d+)?[bcdeEfFgGoOpqstTUvxX]`)

// TestRecordMigrationSQL_UsesPlaceholders is a regression guard for #130.
//
// The recorded migration SQL must use bind placeholders rather than fmt verbs,
// so a description containing `'` or `;` cannot be interpolated directly into
// the SQL text by the migrator. If anyone reverts the generated SQL back to
// `VALUES (%d, '%s', '%s')`, this test fails before the regression reaches
// production. We assert both that exactly three `?` placeholders are present
// (so changing `VALUES (?, %s, NOW())` would also fail even though `?` is
// technically still there) and that no Go fmt verb of any flavor is left in
// the SQL that the migrator actually executes.
func TestRecordMigrationSQL_UsesPlaceholders(t *testing.T) {
	c := qt.New(t)

	sql := (&Migrator{}).recordMigrationSQL()

	c.Assert(strings.Count(sql, "?"), qt.Equals, 3,
		qt.Commentf("record migration SQL must contain exactly 3 ? placeholders (version, description, applied_at)"))

	c.Assert(fmtVerbRe.FindString(sql), qt.Equals, "",
		qt.Commentf("record migration SQL must not contain any Go fmt verb - values must be bound as driver parameters"))
}

// TestDeleteMigrationSQL_UsesPlaceholders is the same regression guard for
// the migration-deletion path.
func TestDeleteMigrationSQL_UsesPlaceholders(t *testing.T) {
	c := qt.New(t)

	sql := (&Migrator{}).deleteMigrationSQL()

	c.Assert(strings.Count(sql, "?"), qt.Equals, 1,
		qt.Commentf("delete migration SQL must contain exactly 1 ? placeholder (version)"))

	c.Assert(fmtVerbRe.FindString(sql), qt.Equals, "",
		qt.Commentf("delete migration SQL must not contain any Go fmt verb - values must be bound as driver parameters"))
}

func TestMigrator_CustomMigrationsTableSQL(t *testing.T) {
	c := qt.New(t)

	m := (&Migrator{}).WithMigrationsTable("infra", "ptah_migrations")

	c.Assert(m.migrationsSchemaStatement(), qt.Equals, `CREATE SCHEMA IF NOT EXISTS "infra"`)
	c.Assert(m.createMigrationsTableSQL(), qt.Contains, `CREATE TABLE IF NOT EXISTS "infra"."ptah_migrations"`)
	c.Assert(m.getVersionSQL(), qt.Equals, `SELECT COALESCE(MAX(version), 0) FROM "infra"."ptah_migrations"`)
	c.Assert(m.recordMigrationSQL(), qt.Equals, `INSERT INTO "infra"."ptah_migrations" (version, description, applied_at) VALUES (?, ?, ?)`)
	c.Assert(m.deleteMigrationSQL(), qt.Equals, `DELETE FROM "infra"."ptah_migrations" WHERE version = ?`)
}
