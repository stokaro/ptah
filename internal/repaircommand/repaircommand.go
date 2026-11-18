// Package repaircommand spells the `ptah migrations repair` command that ends a
// dirty migration revision. The migrator names it in the refusals of `ptah
// migrations up --allow-dirty`, and internal/cli/internal/dirtyhint names it in
// the hint `ptah migrations status` prints under the revision. Both read it
// here, so a refusal names the command the hint names for the same row, and
// never one the repair refuses.
package repaircommand

import "strings"

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
