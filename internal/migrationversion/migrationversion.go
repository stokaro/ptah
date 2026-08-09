// Package migrationversion answers the one question every writer that picks a
// migration version has to answer before it writes: can the reader read this
// version back.
//
// A version is not free-form. The ptah layout renders it with %010d and parses
// it with a ten-digit regex, so 9999999999 is the largest value that survives a
// round trip. The Atlas layout renders it with %d and its parser refuses a
// version that is not strictly positive, so the bound there is math.MaxInt64.
//
// Both bounds matter because discovery drops a name it cannot parse SILENTLY:
// [migrator.DiscoverMigrationFiles] only reports "no migration files matched"
// when the matched set is empty, so an unreadable eleventh file among ten
// readable ones is written, hashed into the integrity file, reported as
// created, and then never executed. Any rule of the form "one above the newest
// migration" therefore has to ask [Next] instead of computing latest+1 itself
// (stokaro/ptah#938).
package migrationversion

import (
	"fmt"
	"math"

	"go.5x5.cz/ptah/migration/migrator"
)

const (
	// PtahMax is the largest version a ptah NNNNNNNNNN_description.up.sql name
	// can carry. GenerateMigrationFileName renders the version with %010d and
	// ParseMigrationFileName matches exactly ten digits, so 10000000000 writes
	// an eleven-digit name no reader accepts.
	PtahMax int64 = 9999999999

	// AtlasMax is the largest version an Atlas <version>_description.sql name
	// can carry. The name renders the version with %d and the parser requires a
	// digit-leading stem parsed as a positive int64, so the ceiling is the int64
	// ceiling itself -- one more wraps to a negative version whose leading `-`
	// is not a digit.
	AtlasMax int64 = math.MaxInt64
)

// Max returns the largest version format's file names can carry.
//
// Only the Atlas layout gets the int64 ceiling. Every other value -- ptah,
// auto, or the empty format -- is bounded by the ptah width, because a writer
// that is not writing Atlas names is writing the fixed-width paired ones.
func Max(format migrator.MigrationDirFormat) int64 {
	if format == migrator.MigrationDirFormatAtlas {
		return AtlasMax
	}
	return PtahMax
}

// Next returns the first version above latest that format can still write, or
// an error when there is none.
//
// The check runs BEFORE the addition on purpose: on the Atlas layout
// latest+1 for latest == [AtlasMax] wraps to math.MinInt64, which renders as a
// perfectly plausible file name and is what stokaro/ptah#938 measured being
// written and then dropped. A directory reaches that value through ordinary
// use -- `migrate import --dir-format flyway` stamps a Flyway `R__` repeatable
// with [AtlasMax] so it sorts last.
func Next(latest int64, format migrator.MigrationDirFormat) (int64, error) {
	limit := Max(format)
	if latest >= limit {
		return 0, fmt.Errorf(
			"cannot allocate a migration version above %d: %s migration file names carry at most %d, "+
				"and a larger version would be written but never read back",
			latest, formatLabel(format), limit,
		)
	}
	return latest + 1, nil
}

// Check reports whether version can be written as a format file name and read
// back. It is the counterpart of [Next] for callers that advance a version
// themselves, such as a scan stepping past names that are already taken.
func Check(version int64, format migrator.MigrationDirFormat) error {
	limit := Max(format)
	if version <= 0 || version > limit {
		return fmt.Errorf(
			"migration version %d cannot be written: %s migration file names carry a version between 1 and %d, "+
				"and a version outside that range would be written but never read back",
			version, formatLabel(format), limit,
		)
	}
	return nil
}

func formatLabel(format migrator.MigrationDirFormat) string {
	if format == migrator.MigrationDirFormatAtlas {
		return string(migrator.MigrationDirFormatAtlas)
	}
	return string(migrator.MigrationDirFormatPtah)
}
