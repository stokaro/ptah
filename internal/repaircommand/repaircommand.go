// Package repaircommand spells the `ptah migrations repair` command that ends a
// dirty migration revision. The migrator names it in the refusals of `ptah
// migrations up --allow-dirty`, and internal/cli/internal/dirtyhint names it in
// the hint `ptah migrations status` prints under the revision. Both read it
// here, so a refusal names the command the hint names for the same row, and
// never one the repair refuses.
package repaircommand

import (
	"io/fs"
	"strings"

	"ptah.run/migration/migrationfile"
)

// Runner is the runner flag a command carries, with its leading space, or
// nothing for the default runner.
type Runner string

const (
	// DefaultRunner adds no flag.
	DefaultRunner Runner = ""
	// GolangMigrate is what every command carries for a body that opens and
	// closes transactions of its own, because only the golang-migrate runner
	// reads such a body with its transactions.
	GolangMigrate Runner = " --runner golang-migrate"
)

// Selection is the flags that make a command read the migration directory and
// the revision table the command that names it reads, each with its leading
// space, or nothing where the defaults read them. Every command a refusal or a
// hint names carries it last: a repair of v1 without --revision-format atlas
// reads the Ptah revision table, which refuses the directory.
type Selection string

const (
	// AtlasDirectory selects a directory whose Atlas files only
	// --dir-format atlas reads. It comes before [AtlasTable] where both apply.
	AtlasDirectory Selection = " --dir-format atlas"
	// AtlasTable selects the Atlas revision table.
	AtlasTable Selection = " --revision-format atlas"
)

// Reading is how a command reads the migration directory and the revision
// table: what it hands the migrator it builds.
type Reading struct {
	// Dir is the migration directory, and nil for a provider that reads none.
	Dir fs.FS
	// DirFormat is the directory format the command selected.
	DirFormat migrationfile.DirFormat
	// AtlasRepeatables reports that R__<identity>.sql files of an Atlas
	// directory are read as repeatable migrations, which only the Atlas format
	// reads.
	AtlasRepeatables bool
	// AtlasTable reports that the command keeps an Atlas revision table.
	AtlasTable bool
}

// SelectionFor returns the flags a command that reads the directory and the
// table as reading does carries: [AtlasDirectory] when it reads Atlas files the
// default directory format would not read, and [AtlasTable] for an Atlas
// revision table. It is empty where the defaults read both, so a command names
// no flag it does not need. The migrator's refusals and the hints of the
// commands that print a revision read it here, so a refusal and a hint name
// the same command for the same row.
func SelectionFor(reading Reading) Selection {
	var selection Selection
	if needsAtlasDirectory(reading) {
		selection += AtlasDirectory
	}
	if reading.AtlasTable {
		selection += AtlasTable
	}
	return selection
}

// needsAtlasDirectory reports that reading reads Atlas files the default
// directory format reads differently: Atlas repeatables are read only under
// the Atlas format, and the default reads Ptah files where the directory holds
// any. A directory with no Atlas file needs no flag, since there is nothing
// the flag would select.
func needsAtlasDirectory(reading Reading) bool {
	if reading.Dir == nil || reading.DirFormat != migrationfile.DirFormatAtlas {
		return false
	}
	if reading.AtlasRepeatables {
		versioned, _, err := migrationfile.DiscoverAtlasRepeatables(reading.Dir)
		return err == nil && len(versioned) > 0
	}
	versioned, err := migrationfile.Discover(reading.Dir, migrationfile.DirFormatAtlas)
	if err != nil || len(versioned) == 0 {
		return false
	}
	auto, err := migrationfile.Discover(reading.Dir, migrationfile.DirFormatAuto)
	return err != nil || len(auto) == 0 || auto[0].Format != migrationfile.DirFormatAtlas
}

// SetPrevious returns the command that records a rollback the operator
// finished by hand, quoted: it moves the revision boundary below the
// migration. The operator names the version before it, or 0 where it is the
// oldest migration in the directory.
func SetPrevious(selection Selection) string {
	return "'ptah migrations set --version <previous>" + string(selection) + "'"
}

// Repair returns the repair command for version, quoted, with flags after the
// version, then runner and then selection. A version a shell would split or
// interpret, such as one holding a space or a semicolon, is written as one
// shell word in single quotes, so the command between the outer quotes runs
// as printed.
func Repair(version, flags string, runner Runner, selection Selection) string {
	return "'ptah migrations repair --version " + shellWord(version) + flags + string(runner) + string(selection) + "'"
}

// shellWord returns value as one POSIX shell word: as it is when every byte is
// one no shell treats specially, and otherwise in single quotes, with each
// single quote it holds ending the quoted part, escaped, and starting the next.
func shellWord(value string) string {
	if value != "" && strings.Trim(value, shellSafe) == "" {
		return value
	}
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

// shellSafe holds the bytes a POSIX shell reads as themselves anywhere in a
// word.
const shellSafe = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-+,/:@%"

// Reconciled returns the repair command that records the migration applied
// once the operator has reconciled the database by hand. It ends every row
// whose recorded progress does not say what the database holds: a statement
// whose outcome is unknown, a transaction whose rollback left some statements
// in place and undid others, and a body whose statement count changed after
// the run. A plain repair refuses each of them, because it cannot show the
// rest of the body ran, so the command carries --force.
func Reconciled(version string, runner Runner, selection Selection) string {
	return Repair(version, " --force", runner, selection)
}
