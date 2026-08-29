package generator

// Version allocation for the Ptah layout. The Atlas layout allocates its own,
// beside the verb that needs it, because the two answer to different rules
// about what a taken version means.

import (
	"os"

	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/migrationversion"
	"go.5x5.cz/ptah/migration/migrationfile"
)

// nextAvailableMigrationVersion answers the version question over the names
// listed through the writer handle the plan already holds, so the version it
// avoids colliding with comes from the directory the plan will publish into
// rather than from whatever the pathname resolves to while it is being chosen.
func nextAvailableMigrationVersion(
	writer *atlasmigrate.MigrationWriter,
	version int64,
	migrationName string,
) (int64, error) {
	names, err := migrationDirNames(writer)
	if err != nil {
		return 0, err
	}
	return nextAvailablePtahVersion(names, version, migrationName)
}

// nextAvailablePtahVersion keeps the paired layout's monotonic rule: the clock,
// bumped past the newest migration when the clock does not already outrank it.
// Nothing outside Ptah reads this layout, so no parity argument moves it off a
// version that only ever goes forwards.
//
// What did move is the arithmetic. The bump is bounded by what a ten-digit
// NNNNNNNNNN prefix can hold, because past it the writer produced an
// eleven-digit name -- `10000000000_addposts.up.sql` -- that
// [migrationfile.ParseFileName] refuses, so discovery dropped the file with
// no diagnostic and `migrations up` reported success without running it
// (stokaro/ptah#938).
func nextAvailablePtahVersion(names []string, version int64, migrationName string) (int64, error) {
	if latest := latestPtahVersionIn(names); latest >= version {
		next, err := migrationversion.Next(latest, migrationfile.DirFormatPtah)
		if err != nil {
			return 0, err
		}
		version = next
	}
	taken := nameSet(names)
	for {
		if err := migrationversion.Check(version, migrationfile.DirFormatPtah); err != nil {
			return 0, err
		}
		upName := migrationfile.FileName(version, migrationName, "up")
		downName := migrationfile.FileName(version, migrationName, "down")
		if !taken[upName] && !taken[downName] {
			return version, nil
		}
		version++
	}
}

func latestPtahVersionIn(names []string) int64 {
	var latest int64
	for _, name := range names {
		migrationFile, err := migrationfile.ParseFileName(name)
		if err != nil {
			continue
		}
		if migrationFile.Version > latest {
			latest = migrationFile.Version
		}
	}
	return latest
}

// migrationDirFileNames lists a migration directory by pathname. It is the
// reader-side counterpart of migrationDirNames, which lists the same thing
// through a bound writer handle; a directory that cannot be listed reads as
// empty, so a version scan over a missing directory starts from scratch.
func migrationDirFileNames(outputDir string) []string {
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		names = append(names, entry.Name())
	}
	return names
}

func nameSet(names []string) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, name := range names {
		set[name] = true
	}
	return set
}
