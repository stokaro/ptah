// Package repaircommand spells the `ptah migrations repair` command that ends a
// dirty migration revision. The migrator names it in the refusals of `ptah
// migrations up --allow-dirty`, and internal/cli/internal/dirtyhint names it in
// the hint `ptah migrations status` prints under the revision. Both read it
// here, so a refusal names the command the hint names for the same row, and
// never one the repair refuses.
package repaircommand

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

// SetPrevious is the command that records a rollback the operator finished by
// hand, quoted: it moves the revision boundary below the migration. The
// operator names the version before it, or 0 where it is the oldest migration
// in the directory.
const SetPrevious = "'ptah migrations set --version <previous>'"

// Repair returns the repair command for version, quoted, with flags after the
// version and then runner.
func Repair(version, flags string, runner Runner) string {
	return "'ptah migrations repair --version " + version + flags + string(runner) + "'"
}

// Reconciled returns the repair command that records the migration applied
// once the operator has reconciled the database by hand. It ends every row
// whose recorded progress does not say what the database holds: a statement
// whose outcome is unknown, a transaction whose rollback left some statements
// in place and undid others, and a body whose statement count changed after
// the run. A plain repair refuses each of them, because it cannot show the
// rest of the body ran, so the command carries --force.
func Reconciled(version string, runner Runner) string {
	return Repair(version, " --force", runner)
}
