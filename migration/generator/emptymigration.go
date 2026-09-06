package generator

// An empty migration: the pair a user asks for with no diff behind it, and the
// name and version allocation each directory layout gives it.

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"ptah.run/internal/migrationversion"
	"ptah.run/internal/pathguard"
	"ptah.run/migration/migrationfile"
)

// EmptyMigrationOptions contains options for skeleton migration creation.
type EmptyMigrationOptions struct {
	// MigrationName is the descriptive migration name used in filenames and headers.
	MigrationName string
	// OutputDir is the directory where migration files will be saved.
	OutputDir string
	// AllowedOutputRoot constrains OutputDir when set.
	AllowedOutputRoot string
	// DirFormat selects the generated migration file layout. Empty generates
	// Ptah paired up/down files.
	DirFormat migrationfile.DirFormat
}

// GenerateEmptyMigration creates skeleton migration files for manual SQL
// authoring, with no database involved.
//
// What it writes follows the layout. The default (an empty DirFormat) is the
// paired Ptah layout: an up and a down file, returned as the one pair in
// [MigrationFiles]. DirFormatAtlas writes the up-only Atlas convention
// instead: a single file, a pair whose DownFile is empty, and the directory's
// atlas.sum left valid over it, because an Atlas directory with a stale sum is
// refused by the tools that read it. A caller iterating pairs must therefore
// not assume DownFile is set.
//
// The whole creation runs through one rooted migration-directory handle, bound
// before anything is read or written: the directory is materialized, the
// version scanned, and the files and any checksum committed through that one
// handle rather than through the pathname it was selected by
// (stokaro/ptah#1118). When AllowedOutputRoot is set the handle is opened
// through it, so the transaction stays inside that root even if the directory
// or one of its ancestors is replaced after the path was validated.
func GenerateEmptyMigration(opts EmptyMigrationOptions) (*MigrationFiles, error) {
	name := strings.TrimSpace(opts.MigrationName)
	if strings.TrimSpace(opts.OutputDir) == "" {
		return nil, fmt.Errorf("output directory is required")
	}
	dirFormat, err := migrationfile.ParseDirFormat(string(opts.DirFormat))
	if err != nil {
		return nil, err
	}

	outputDir, err := pathguard.ResolveWithinRoot(opts.OutputDir, opts.AllowedOutputRoot)
	if err != nil {
		return nil, fmt.Errorf("error validating output directory: %w", err)
	}
	// The Atlas layout derives its own file name from the migration name and
	// accepts an empty one, so only the paired layout validates it here -- and
	// it validates before the directory is bound, so a rejected name never
	// creates a directory.
	if dirFormat != migrationfile.DirFormatAtlas {
		if err := validateEmptyMigrationName(name); err != nil {
			return nil, err
		}
	} else if err := checkAtlasEmptyMigrationNameReadable(name); err != nil {
		return nil, err
	}

	root, err := openOutputRoot(opts.AllowedOutputRoot)
	if err != nil {
		return nil, err
	}
	defer func() { _ = closeOutputRoot(root) }()

	return writeEmptyMigration(root, outputDir, name, dirFormat)
}

// atlasVersionClock reads the wall clock an Atlas-layout version is stamped
// from. It is a variable so a test can freeze the second and then state the
// exact version a scan must choose; production never assigns it.
var atlasVersionClock = func() time.Time { return time.Now().UTC() }

func nextAtlasMigrationVersion() int64 {
	version, err := strconv.ParseInt(atlasVersionClock().Format("20060102150405"), 10, 64)
	if err != nil {
		return migrationfile.NextVersion()
	}
	return version
}

// nextAvailableAtlasMigrationVersion answers the version question for a
// directory named by pathname, for readers that are not inside a writer
// transaction. A writer holding a rooted handle asks nextAvailableAtlasVersion
// over the names it listed through that handle instead, so it never resolves
// the directory a second time.
func nextAvailableAtlasMigrationVersion(outputDir string, version int64) int64 {
	next, err := nextAvailableAtlasVersion(migrationDirFileNames(outputDir), version)
	if err != nil {
		return 0
	}
	return next
}

// nextAvailableAtlasVersion returns a version that outranks every migration in
// names. It is the CHECKPOINT rule: a checkpoint whose version does not sort
// above the history it squashes would be replayed on top of that history by a
// fresh database (stokaro/ptah#954), so here the bump past the newest migration
// is the point.
//
// It is deliberately NOT the rule `migrate new` uses -- see
// firstFreeAtlasVersion. The bump is also why this one can run out of versions:
// a directory whose newest migration is at [migrationversion.AtlasMax] has
// nothing above it, and `migrate import --dir-format flyway` puts a repeatable
// migration exactly there. Before stokaro/ptah#938 that computed
// math.MinInt64 and wrote it.
//
// The bump asks [migrationversion.Advance] rather than adding one, so it steps
// to the next real second instead of the next integer. Beside a
// `29991231235959_future.sql` the raw increment wrote a checkpoint at
// 29991231235960 -- sixty seconds past the minute, an instant time.Parse
// refuses -- and a second checkpoint then wrote 29991231235961 on top of it.
func nextAvailableAtlasVersion(names []string, version int64) (int64, error) {
	if latest := latestAtlasVersionIn(names); latest >= version {
		next, err := migrationversion.Advance(latest, migrationfile.DirFormatAtlas)
		if err != nil {
			return 0, err
		}
		version = next
	}
	taken := nameSet(names)
	for taken[atlasEmptyMigrationFileName(version, "")] {
		next, err := migrationversion.Advance(version, migrationfile.DirFormatAtlas)
		if err != nil {
			return 0, err
		}
		version = next
	}
	return version, nil
}

// firstFreeAtlasVersion returns the first version at or after version that no
// migration in names already occupies. It is the rule `migrate new` writes an
// Atlas-layout migration with, and it does NOT bump past the newest migration.
//
// Two measurements settle that. The pinned community binary v1.3.0 stamps the
// current UTC second and nothing else: into a directory holding
// `29991231235959_future.sql` it writes today's version, sorting BELOW the
// migration already there. And `migrate diff` in this binary has done the same
// since stokaro/ptah#1218 (see atlasmigrate.nextMigrationVersionFS), as does
// `migrate new` for the five external layouts (atlasmigrate.WriteSkeletonMigration).
// Bumping here made `migrate new` the one verb in the binary stamping a
// different shape: on that directory it wrote `29991231235960`, a version that
// is not a time anyone can parse back, while `migrate diff` a second later
// wrote the ordinary UTC stamp (stokaro/ptah#938).
//
// The collision step asks [migrationversion.Writable] rather than testing the
// bounds itself, so two migrations created inside the same second at :59 land
// on the next real second instead of on a sixtieth one.
func firstFreeAtlasVersion(names []string, version int64) (int64, error) {
	taken := atlasVersionsIn(names)
	for {
		free, err := migrationversion.Writable(version, migrationfile.DirFormatAtlas)
		if err != nil {
			return 0, err
		}
		version = free
		if _, ok := taken[version]; !ok {
			return version, nil
		}
		version++
	}
}

// atlasVersionsIn returns the versions names already occupy. A version is taken
// by ANY migration at it, whatever its description, which is what the reader
// orders by and what atlasmigrate.nextMigrationVersionFS already checks.
func atlasVersionsIn(names []string) map[int64]struct{} {
	taken := make(map[int64]struct{}, len(names))
	for _, name := range names {
		migrationFile, err := migrationfile.ParseAtlasFileName(name)
		if err != nil {
			continue
		}
		taken[migrationFile.Version] = struct{}{}
	}
	return taken
}

func latestAtlasVersionIn(names []string) int64 {
	var latest int64
	for _, name := range names {
		migrationFile, err := migrationfile.ParseAtlasFileName(name)
		if err != nil {
			continue
		}
		if migrationFile.Version > latest {
			latest = migrationFile.Version
		}
	}
	return latest
}

// atlasEmptyMigrationFileName composes the Atlas-layout file name `migrate new`
// writes, and it carries the caller's name unchanged.
//
// It used to rewrite the name first: spaces became hyphens and every character
// outside [-_0-9A-Za-z] was dropped, so `migrate new "add users table"` wrote
// `<version>_add-users-table.sql` and `migrate new "add_users.sql"` wrote
// `<version>_add_userssql.sql`. The Atlas layout is not ours to rename in: the
// pinned community binary v1.3.0 composes `<version>_<name>.sql` from the name
// verbatim, so the same two commands write `<version>_add users table.sql` and
// `<version>_add_users.sql.sql` there (measured 2026-08-08). The file name is
// covered by atlas.sum, so a rewritten name is also a different checksum for the
// same command (stokaro/ptah#1235 findings 8.6 and 8.7).
//
// Two rules still apply and are deliberate, not leftovers:
//
//   - [GenerateEmptyMigration] trims leading and trailing whitespace off the
//     name for every layout. That binary keeps it -- `migrate new "  padded  "`
//     writes `<version>_  padded  .sql` -- but a file name with trailing spaces
//     does not survive every filesystem this tool writes into, and no finding in
//     that register asks for one.
//   - A path separator is refused rather than stripped, on the compatibility
//     surface by checkAtlasMigrationName and here by the rooted writer, which
//     cannot open a path outside the directory it holds.
//
// [atlasCheckpointNameStem] keeps the old rewriting for `migrate checkpoint`.
// That verb has no measured counterpart on the pinned binary, so there is
// nothing to move towards; both writers now open the file through a rooted
// handle.
func atlasEmptyMigrationFileName(version int64, name string) string {
	if name == "" {
		return fmt.Sprintf("%d.sql", version)
	}
	return fmt.Sprintf("%d_%s.sql", version, name)
}

// atlasCheckpointNameStem maps a checkpoint description onto a conservative file
// name stem: spaces to hyphens, everything outside [-_0-9A-Za-z] dropped.
//
// This is what every Atlas-layout name used to go through. `migrate new` no
// longer does, because that binary was measured writing the name verbatim there.
// `migrate checkpoint` keeps it: the register that prompted the change records
// no cell for the verb, so there is nothing measured to move towards.
//
// Containment is no longer the reason. [WriteAtlasCheckpointFileWithOptions]
// now creates the file through a rooted directory handle, which refuses a name
// carrying a separator rather than following it out of the directory
// (stokaro/ptah#1118), so the stem rewriting is a naming convention and not a
// boundary. Removing it would change file names for a verb with no measured
// counterpart, which is why it stays.
func atlasCheckpointNameStem(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, " ", "-")
	var b strings.Builder
	for _, r := range name {
		if isAtlasMigrationNameChar(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func isAtlasMigrationNameChar(r rune) bool {
	return r == '-' || r == '_' ||
		('0' <= r && r <= '9') ||
		('A' <= r && r <= 'Z') ||
		('a' <= r && r <= 'z')
}

// checkAtlasEmptyMigrationNameReadable refuses a migration name whose composed
// file name this tool would read back as something other than the new up
// migration it just wrote.
//
// Writing the name verbatim means it can now reach the Atlas file-name grammar,
// and one suffix collides with it: [migrationfile.ParseAtlasFileName]
// classifies `<version>_x.down.sql` as the down half of a pair, because Atlas
// importers emit that spelling for golang-migrate directories. So `migrate new
// "x.down"` would write a file `migrate status` then refuses with `Atlas
// migration version <version> has down migration but no up migration`, exit 1 --
// one verb producing a directory another verb cannot read.
//
// The check is a round trip rather than a list of banned suffixes, so a later
// change to that grammar cannot reopen the hole silently.
//
// This refusal is stricter than the pinned community binary v1.3.0, which
// writes `<version>_x.down.sql` at exit 0 and reads it back as a pending
// migration at exit 0. Ptah refusing to WRITE it does not fix Ptah's reader:
// that binary can still hand us such a directory, and `migrate status` still
// exits 1 on it. That reader gap is a separate divergence and is reported, not
// closed, by the change that introduced this guard.
func checkAtlasEmptyMigrationNameReadable(name string) error {
	// The version is a stand-in: the grammar this probes keys on the name's
	// suffix, not on the digits in front of it, and the message quotes the shape
	// rather than a version no caller asked for.
	const probeVersion = 1
	fileName := atlasEmptyMigrationFileName(probeVersion, name)
	parsed, err := migrationfile.ParseAtlasFileName(fileName)
	if err == nil && parsed.Version == probeVersion && parsed.Direction == "up" {
		return nil
	}
	shape := "<version>" + strings.TrimPrefix(fileName, strconv.FormatInt(probeVersion, 10))
	return fmt.Errorf(
		"migration name %q composes the file name %s, which this tool does not read back as a new migration",
		name,
		shape,
	)
}

func validateEmptyMigrationName(name string) error {
	if name == "" {
		return fmt.Errorf("migration name is required")
	}

	fileName := migrationfile.FileName(1, name, "up")
	if strings.HasPrefix(fileName, "0000000001_.") {
		return fmt.Errorf("migration name must contain letters, digits, or underscores")
	}

	return nil
}

func emptyMigrationSQL(name, generatedAt, direction string) string {
	return fmt.Sprintf(`-- Migration: %s
-- Generated on: %s
-- Direction: %s

-- Add your migration SQL here.
`, name, generatedAt, direction)
}
