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
// the SQL text by the migrator. If anyone reverts the template back to
// `VALUES (%d, '%s', '%s')`, this test fails before the regression reaches
// production. We assert both that exactly three `?` placeholders are present
// (so changing `VALUES (?, %s, NOW())` would also fail even though `?` is
// technically still there) and that no Go fmt verb of any flavour is left in
// the template.
func TestRecordMigrationSQL_UsesPlaceholders(t *testing.T) {
	c := qt.New(t)

	c.Assert(strings.Count(recordMigrationSQL, "?"), qt.Equals, 3,
		qt.Commentf("record_migration.sql must contain exactly 3 ? placeholders (version, description, applied_at)"))

	c.Assert(fmtVerbRe.FindString(recordMigrationSQL), qt.Equals, "",
		qt.Commentf("record_migration.sql must not contain any Go fmt verb — values must be bound as driver parameters"))
}

// TestDeleteMigrationSQL_UsesPlaceholders is the same regression guard for
// the migration-deletion path.
func TestDeleteMigrationSQL_UsesPlaceholders(t *testing.T) {
	c := qt.New(t)

	c.Assert(strings.Count(deleteMigrationSQL, "?"), qt.Equals, 1,
		qt.Commentf("delete_migration.sql must contain exactly 1 ? placeholder (version)"))

	c.Assert(fmtVerbRe.FindString(deleteMigrationSQL), qt.Equals, "",
		qt.Commentf("delete_migration.sql must not contain any Go fmt verb — values must be bound as driver parameters"))
}
