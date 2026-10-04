// Package migratetimeout refuses a run-wide lock or statement timeout aimed at
// a target that cannot carry one.
//
// `ptah migrations up` and `ptah migrations down` take --lock-timeout and
// --statement-timeout as the default for every migration they run, and the
// migrator bounds each migration with them where the target has a capability
// for them (capability.MigrationLockTimeout and
// capability.MigrationStatementTimeout). A target can carry one and not the
// other: YDB bounds each statement and has no lock wait to bound. Where a
// target lacks the key, the migrator refuses the first migration that would
// run under the timeout, and a run that selects no migration accepts it in
// silence. Neither tells the operator that the value they passed is what the
// target cannot take, so the command refuses it by name: before it connects
// where the dialect the URL names settles the answer, and otherwise once it
// has connected, before the migrator reads or writes anything.
package migratetimeout

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/internal/cmdadapter"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/migration/migrationfile"
)

// The project-config paths that fill the two flags.
const (
	LockConfigKey      = "migration.lock_timeout"
	StatementConfigKey = "migration.statement_timeout"
)

// UnsupportedError reports run-wide timeouts aimed at a target that cannot
// carry them. Requests are the spellings the operator used, in the order the
// flags are declared, and Missing the timeout capability keys the target
// lacks, the lock timeout's first.
type UnsupportedError struct {
	Requests []string
	Missing  []capability.Capability
	Dialect  string
}

func (e *UnsupportedError) Error() string {
	verb := "sets a timeout"
	if len(e.Requests) > 1 {
		verb = "set timeouts"
	}
	return fmt.Sprintf(
		"%s %s for every migration, and dialect %q %s. Remove %s to run without one",
		strings.Join(e.Requests, " and "), verb, e.Dialect, e.lacks(), strings.Join(e.Requests, " and "))
}

// lacks says what the target has no way to honor: the one timeout it cannot
// carry when it carries the other, and both otherwise.
func (e *UnsupportedError) lacks() string {
	if len(e.Missing) == 1 {
		switch e.Missing[0] {
		case capability.MigrationLockTimeout:
			return fmt.Sprintf("has no lock wait Ptah can bound around a migration (target capability %s)",
				capability.MigrationLockTimeout)
		case capability.MigrationStatementTimeout:
			return fmt.Sprintf("has no statement timeout Ptah can set around a migration (target capability %s)",
				capability.MigrationStatementTimeout)
		}
	}
	return "has no lock or statement timeout Ptah can set and restore around a migration"
}

// Request is one command's run-wide timeout input: the command holding the
// flags, their names, and whether the project config supplied each value.
type Request struct {
	// Cmd is the running command, read for its flags and for the surface it
	// is running on.
	Cmd *cobra.Command
	// LockFlag and StatementFlag are the flags' names, without the dashes.
	LockFlag      string
	StatementFlag string
	// LockFromConfig and StatementFromConfig report that the project config
	// carries [LockConfigKey] and [StatementConfigKey].
	LockFromConfig      bool
	StatementFromConfig bool
	// DBURL is the target URL the command was given, read by
	// [Request.DecideFromURL].
	DBURL string
}

// DecideFromURL decides from the dialect [Request.DBURL] names, before the
// command opens anything, wherever that dialect settles the answer: a
// `sqlite://` target is refused without creating its file.
//
// The dialect settles it where its preset lacks the timeout's capability key.
// The migrator bounds timeouts on the PostgreSQL and MySQL families and the
// statement timeout on YDB, and a URL of any other dialect reaches a server of
// that dialect. A PostgreSQL-wire URL does not settle it, since `postgres://`
// may reach Spanner, which takes none; its preset carries the keys, so it
// passes here and [Request.Decide] answers it against the connected server. A
// URL Ptah cannot classify makes no claim here.
func (r Request) DecideFromURL(timeouts migrationfile.Timeouts) error {
	dialect, err := atlasurl.DialectFromURL(r.DBURL)
	if err != nil {
		return nil
	}
	return r.Decide(dialect, capability.ForDialect(dialect), timeouts)
}

// Decide refuses each timeout caps, the connected server's capabilities, has
// no key for -- capability.MigrationLockTimeout for the lock timeout and
// capability.MigrationStatementTimeout for the statement timeout -- and
// returns nil otherwise.
//
// Only a value the operator addressed to this command refuses: a flag on the
// command line, or a key in the project config. PTAH_LOCK_TIMEOUT also fills
// `ptah schema apply --lock-timeout`, where it is a lock wait, so a value
// exported for that command is not taken as a request to this one; the
// migrator still refuses it on the first migration that would run under it.
//
// A forwarded run is left alone: the compatibility surface maps its own
// flags onto these, and refusing here would name a flag the operator never
// typed.
func (r Request) Decide(dialect string, caps capability.Capabilities, timeouts migrationfile.Timeouts) error {
	lockMissing := timeouts.HasLockTimeout && !caps.Has(capability.MigrationLockTimeout)
	statementMissing := timeouts.HasStatementTimeout && !caps.Has(capability.MigrationStatementTimeout)
	if !lockMissing && !statementMissing {
		return nil
	}
	if r.Cmd == nil || cmdadapter.Forwarded(r.Cmd) {
		return nil
	}
	refused := &UnsupportedError{Dialect: dialect}
	lock := timeoutInput{flag: r.LockFlag, configKey: LockConfigKey, fromConfig: r.LockFromConfig}
	if spelling, asked := r.spelling(lock); lockMissing && asked {
		refused.Requests = append(refused.Requests, spelling)
	}
	statement := timeoutInput{flag: r.StatementFlag, configKey: StatementConfigKey, fromConfig: r.StatementFromConfig}
	if spelling, asked := r.spelling(statement); statementMissing && asked {
		refused.Requests = append(refused.Requests, spelling)
	}
	if len(refused.Requests) == 0 {
		return nil
	}
	for _, key := range []capability.Capability{capability.MigrationLockTimeout, capability.MigrationStatementTimeout} {
		if !caps.Has(key) {
			refused.Missing = append(refused.Missing, key)
		}
	}
	return refused
}

// timeoutInput is where one of the two timeouts can come from: its flag, and
// the project-config key that fills the flag when the config carries it.
type timeoutInput struct {
	flag       string
	configKey  string
	fromConfig bool
}

// spelling names where one timeout reached this run from, and reports false
// when it came from the environment, which is not addressed to this command
// alone (see [Request.Decide]).
func (r Request) spelling(in timeoutInput) (string, bool) {
	flags := r.Cmd.Flags()
	if _, fromEnv := cmdflags.AppliedEnvName(flags, in.flag); fromEnv {
		return "", false
	}
	if cmdflags.SetOnCommandLine(flags, in.flag) {
		return "--" + in.flag, true
	}
	if in.fromConfig {
		return in.configKey, true
	}
	return "", false
}
