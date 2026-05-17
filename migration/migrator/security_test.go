package migrator

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestRecordMigrationSQL_UsesPlaceholders is a regression guard for #130.
//
// The recorded migration SQL must use bind placeholders rather than fmt verbs,
// so a description containing `'` or `;` cannot be interpolated directly into
// the SQL text by the migrator. If anyone reverts the template back to
// `VALUES (%d, '%s', '%s')`, this test fails before the regression reaches
// production.
func TestRecordMigrationSQL_UsesPlaceholders(t *testing.T) {
	c := qt.New(t)

	c.Assert(recordMigrationSQL, qt.Contains, "?", qt.Commentf("record_migration.sql must use ? placeholders"))

	for _, verb := range []string{"%s", "%d", "%v", "%q"} {
		c.Assert(strings.Contains(recordMigrationSQL, verb), qt.IsFalse,
			qt.Commentf("record_migration.sql must not contain fmt verb %q — values must be bound as driver parameters", verb))
	}
}

// TestDeleteMigrationSQL_UsesPlaceholders is the same regression guard for
// the migration-deletion path.
func TestDeleteMigrationSQL_UsesPlaceholders(t *testing.T) {
	c := qt.New(t)

	c.Assert(deleteMigrationSQL, qt.Contains, "?", qt.Commentf("delete_migration.sql must use ? placeholders"))

	for _, verb := range []string{"%s", "%d", "%v", "%q"} {
		c.Assert(strings.Contains(deleteMigrationSQL, verb), qt.IsFalse,
			qt.Commentf("delete_migration.sql must not contain fmt verb %q — values must be bound as driver parameters", verb))
	}
}
