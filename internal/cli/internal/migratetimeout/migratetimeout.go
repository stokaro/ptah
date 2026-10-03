// Package migratetimeout refuses a run-wide lock or statement timeout aimed at
// a target that cannot carry one.
//
// `ptah migrations up` and `ptah migrations down` take --lock-timeout and
// --statement-timeout as the default for every migration they run, and the
// migrator sets them around each migration where the target has a setting
// for them (capability.MigrationTimeouts). Where it has none, the migrator
// refuses the first migration that would run under them, and a run that
// selects no migration accepts them in silence. Neither tells the operator
// that the values they passed are what the target cannot take, so the
// command refuses them by name once it has connected, before the migrator
// reads or writes anything.
package migratetimeout

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/cli/internal/cmdadapter"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/migration/migrationfile"
)

// The project-config paths that fill the two flags.
const (
	LockConfigKey      = "migration.lock_timeout"
	StatementConfigKey = "migration.statement_timeout"
)

// UnsupportedError reports run-wide timeouts aimed at a target with no
// timeout setting. Requests are the spellings the operator used, in the order
// the flags are declared.
type UnsupportedError struct {
	Requests []string
	Dialect  string
}

func (e *UnsupportedError) Error() string {
	verb := "sets a timeout"
	if len(e.Requests) > 1 {
		verb = "set timeouts"
	}
	return fmt.Sprintf(
		"%s %s for every migration, and dialect %q has no lock or statement timeout Ptah can set and "+
			"restore around a migration. Remove %s to run without one",
		strings.Join(e.Requests, " and "), verb, e.Dialect, strings.Join(e.Requests, " and "))
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
}

// Decide refuses timeouts when caps, the connected server's capabilities, has
// no capability.MigrationTimeouts, and returns nil otherwise.
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
	if timeouts.IsZero() || caps.Has(capability.MigrationTimeouts) {
		return nil
	}
	if r.Cmd == nil || cmdadapter.Forwarded(r.Cmd) {
		return nil
	}
	var requests []string
	lock := timeoutInput{flag: r.LockFlag, configKey: LockConfigKey, fromConfig: r.LockFromConfig}
	if spelling, asked := r.spelling(lock); timeouts.HasLockTimeout && asked {
		requests = append(requests, spelling)
	}
	statement := timeoutInput{flag: r.StatementFlag, configKey: StatementConfigKey, fromConfig: r.StatementFromConfig}
	if spelling, asked := r.spelling(statement); timeouts.HasStatementTimeout && asked {
		requests = append(requests, spelling)
	}
	if len(requests) == 0 {
		return nil
	}
	return &UnsupportedError{Requests: requests, Dialect: dialect}
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
