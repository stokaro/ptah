package migrator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

func TestResolveAtlasDirectiveTxMode(t *testing.T) {
	c := qt.New(t)

	// An unspecified directive keeps the global mode.
	mode, err := migrator.ResolveAtlasDirectiveTxMode(
		migrator.MigrationTxModeNone, migrationfile.FileTxModeUnspecified, "plan.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(mode, qt.Equals, migrator.MigrationTxModeNone)

	// A directive overrides the global mode.
	mode, err = migrator.ResolveAtlasDirectiveTxMode(
		migrator.MigrationTxModeNone, migrationfile.FileTxModeFile, "plan.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(mode, qt.Equals, migrator.MigrationTxModeFile)

	// Under `all` the combination is refused rather than resolved, and the
	// refusal names the artifact that carried the directive.
	_, err = migrator.ResolveAtlasDirectiveTxMode(
		migrator.MigrationTxModeAll, migrationfile.FileTxModeNone, "plan.sql")
	c.Assert(err, qt.ErrorMatches,
		`cannot set txmode directive to "none" in "plan.sql" when txmode "all" is set globally`)
}
