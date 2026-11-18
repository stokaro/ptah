package atlas

import (
	"errors"

	"ptah.run/internal/atlasmigrateimport"
	"ptah.run/internal/envbool"
)

// atlasRepeatableMigrationsEnvVar reads a top-level R__<identity>.sql of a
// directory in the Atlas layout as a native repeatable migration on `migrate
// apply` and `migrate status`: it runs after the versioned files whenever its
// content changed, and its record is a table beside the revision table, named
// after it with _repeatable. Unset, the file is the one-time version R, as
// the community binary reads it.
//
// It is a variable rather than a flag because the compatibility surface
// registers no flag the community binary does not have.
const atlasRepeatableMigrationsEnvVar = "PTAH_ATLAS_REPEATABLE_MIGRATIONS"

// atlasRepeatableMigrations is the declaration of the variable. See
// [ptah.run/internal/envbool].
//
// It is [ptah.run/internal/envbool.Gated]: the pinned community binary has no
// re-run-on-change semantics. It runs R__<name>.sql once, as the version R,
// and never again after its content changes, so strict compatibility refuses
// an enabled value rather than run a file the oracle would leave alone.
var atlasRepeatableMigrations = envbool.New(atlasRepeatableMigrationsEnvVar, false, envbool.Gated)

// resolveAtlasRepeatableMigrations reads the run's choice. A malformed value
// is refused before the command reads its directory or opens the database,
// whatever layout the directory turns out to have.
func resolveAtlasRepeatableMigrations() (bool, error) {
	return atlasRepeatableMigrations.Resolve()
}

// atlasReadsRepeatables reports whether a run that resolved enabled reads the
// repeatable migrations of a directory in format. The reading applies to the
// Atlas layout alone: every other layout is converted, and gives R__ its own
// meaning or none.
func atlasReadsRepeatables(enabled bool, format atlasmigrateimport.Format) bool {
	return enabled && format == atlasmigrateimport.FormatAtlas
}

// errAtlasDownRepeatables refuses `migrate down` under the variable. A
// rollback is refused natively while the record holds a row, because a
// repeatable migration has no rollback and was written against the latest
// schema; this verb does not read the record, so it refuses rather than roll
// back underneath a repeatable migration it cannot see.
var errAtlasDownRepeatables = errors.New(atlasRepeatableMigrationsEnvVar + " is set, and migrate down does not " +
	"read repeatable migrations: a rollback under a repeatable migration is refused, and this verb cannot see " +
	"the record; unset the variable to roll back a directory with no repeatable migrations, or roll forward " +
	"with a new migration instead")

// refuseAtlasDownRepeatables refuses `migrate down` when the variable is
// enabled, and a malformed value in any case.
func refuseAtlasDownRepeatables() error {
	enabled, err := resolveAtlasRepeatableMigrations()
	if err != nil {
		return err
	}
	if enabled {
		return errAtlasDownRepeatables
	}
	return nil
}
