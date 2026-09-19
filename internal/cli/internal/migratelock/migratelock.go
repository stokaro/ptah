// Package migratelock answers a migration advisory lock timeout aimed at a
// target whose dialect has no such lock.
//
// The versioned commands take a session-scoped advisory lock so two runners
// cannot advance one migration history at the same time. On a dialect without
// advisory-lock semantics the lock is a no-op, so a timeout bounds a wait the
// target never makes. Taking that request in silence is what stokaro/ptah#3417
// reports: the operator believes the deployment is serialized, and it is not.
//
// The refusal lives here rather than in the migrator because the migrator is
// also what `ptah-compat migrate apply` and `ptah-compat migrate down` drive,
// and that surface answers to the Atlas contract. A decision made in the CLI
// layer reaches the native verbs, and [Request.Decide] leaves a forwarded run
// alone so the compatibility surface keeps the behavior it had.
package migratelock

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/internal/cmdadapter"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/dblock"
)

// ConfigKey is the project-config path that fills the same flag as
// `--migration-lock-timeout`. A refusal names it so an operator who set it in
// `ptah.yaml` is not sent looking through a command line that never carried it.
const ConfigKey = "migration.migration_lock_timeout"

// UnsupportedError reports that a caller asked for the migration advisory lock
// on a target whose dialect has none. Request is the spelling the operator
// used, and appears at the front of the message so they read back what they
// passed.
type UnsupportedError struct {
	Request string
	Dialect string
}

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf(
		"%s requested the migration advisory lock, and dialect %q has none: "+
			"only %s take a session advisory lock. Remove %s to run without a lock",
		e.Request, e.Dialect, strings.Join(dblock.SupportedDialects(), ", "), e.Request)
}

// Ensure refuses a migration advisory lock the target cannot take. It returns
// an [UnsupportedError] when dialect has no session advisory lock, and nil
// otherwise; the dialect set it reads is [dblock.Supported]'s, so no copy of
// that list lives here.
//
// It answers from a dialect name alone, so a caller resolves it from the target
// URL and refuses before connecting: a lock request that is going to be refused
// must not first open the database.
//
// Deciding that the operator asked for a lock belongs to the caller, which is
// the only side that can tell a value it was given from a default it chose.
// Call this only for a request the operator made, or use [Request.Decide].
func Ensure(request, dialect string) error {
	if dblock.Supported(dialect) {
		return nil
	}
	return &UnsupportedError{Request: request, Dialect: dialect}
}

// Request is one command's migration lock timeout input: the command holding
// the flag, the flag's name, whether the project config supplied the value, and
// the target URL.
//
// A command builds one and asks it twice, because the dialect a URL names is
// not always the one it reaches. [Request.Decide] from the URL lands before the
// connection, which is what refuses a `sqlite://` target without creating the
// file. [Request.DecideConnected] lands after, which is the only place a
// PostgreSQL-wire server can be told apart from PostgreSQL.
type Request struct {
	// Cmd is the running command, read for its flags and for the surface it is
	// running on.
	Cmd *cobra.Command
	// FlagName is the lock timeout flag's name, without the leading dashes.
	FlagName string
	// FromConfig reports that the project config carries [ConfigKey]. It is
	// read only when neither the command line nor the environment set the
	// flag, which is exactly when the config value becomes the effective one.
	FromConfig bool
	// DBURL is the target URL the command was given.
	DBURL string
}

// Spelling names where a migration lock timeout reached this run from, and
// reports whether one did.
//
// Every spelling refuses, unlike `ptah schema apply --lock-timeout`, which
// takes the command line alone. PTAH_LOCK_TIMEOUT means a per-migration
// statement timeout on the versioned commands and a session advisory lock wait
// on the apply, so a value exported for one configures nothing about the other.
// PTAH_MIGRATION_LOCK_TIMEOUT and [ConfigKey] have no such second reading:
// every command that takes them means this lock by them, so a value that
// arrives that way is addressed to this run and asks for a lock this target
// cannot give.
//
// Presence decides, not the value: `--migration-lock-timeout ""` asks for an
// unbounded wait, which is equally a wait this target never makes.
func (r Request) Spelling() (string, bool) {
	if r.Cmd == nil {
		return "", false
	}
	flags := r.Cmd.Flags()
	if envName, fromEnv := cmdflags.AppliedEnvName(flags, r.FlagName); fromEnv {
		return envName, true
	}
	if cmdflags.SetOnCommandLine(flags, r.FlagName) {
		return "--" + r.FlagName, true
	}
	if r.FromConfig {
		return ConfigKey, true
	}
	return "", false
}

// Decide refuses the request when dialect has no migration advisory lock, and
// returns nil otherwise.
//
// An empty dialect is no evidence either way, so it makes no claim rather than
// a wrong one: a URL Ptah cannot classify reaches its own refusal from the
// connector.
//
// A forwarded run is left alone. `ptah-compat migrate down --lock-timeout`
// reaches this command through [cmdadapter], which maps the Atlas spelling onto
// the native flag, so refusing here would change the compatibility surface and
// would name a flag the operator never typed. What the Atlas CLI does with
// `--lock-timeout` on a dialect that cannot lock is not measured in this
// repository, so the compatibility surface keeps accepting it.
func (r Request) Decide(dialect string) error {
	// A docker:// URL names a dev engine Ptah would start, not a target that
	// exists. DialectFromURL reads the engine out of it, ConnectToDatabase has
	// no arm for the scheme, and an operator who passed one has to read that
	// refusal rather than one about a lock on a database they never named.
	if dialect == "" || atlasurl.IsDockerURL(r.DBURL) {
		return nil
	}
	if r.Cmd == nil || cmdadapter.Forwarded(r.Cmd) {
		return nil
	}
	request, asked := r.Spelling()
	if !asked {
		return nil
	}
	return Ensure(request, dialect)
}

// DecideFromURL decides from the dialect [Request.DBURL] names, which is the
// answer a caller can have before it opens anything. It is what refuses a
// `sqlite://` target without creating the file.
//
// A URL Ptah cannot classify makes no claim here, and the connector answers it
// with a refusal of its own; a URL that resolves to a dialect the server then
// contradicts is answered by [Request.DecideConnected].
func (r Request) DecideFromURL() error {
	dialect, err := atlasurl.DialectFromURL(r.DBURL)
	if err != nil {
		return nil
	}
	return r.Decide(dialect)
}

// DecideConnected repeats the decision against the dialect the server reported,
// which is the authoritative one.
//
// A PostgreSQL-wire URL does not name its product. The connector reads the
// server's own banner and answers cockroachdb, yugabytedb or spanner for a
// server that names itself, so `postgres://` reaches a target that may have no
// session advisory lock. Deciding from the URL alone runs unlocked under an
// explicit timeout, against CockroachDB and Spanner.
//
// It decides only where the server disagrees with the URL, so a request already
// refused from the URL is not weighed twice, and a URL whose dialect did not
// resolve is decided here instead of nowhere.
func (r Request) DecideConnected(connected string) error {
	if urlDialect, err := atlasurl.DialectFromURL(r.DBURL); err == nil && urlDialect == connected {
		return nil
	}
	return r.Decide(connected)
}
