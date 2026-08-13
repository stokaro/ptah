// Package migrationintegrity owns the one rule every Ptah verb that EXECUTES
// SQL from a migration directory runs before executing any of it: a directory
// that carries an integrity file has to match it.
//
// The rule started life inside `ptah migrations up` (stokaro/ptah#955) as a
// private helper, and keeping it there is what produced the defect this package
// exists to close. Measured on one hashed directory whose `_init.down.sql` was
// rewritten and whose ptah.sum was left stale, `migrations up` exited 2 and
// refused, while `migrations down --target 0 --confirm` exited 0 and executed
// the rewritten file — a post-run table census showed the attacker's table
// present and the migration's own table gone. The same held for
// `migrations test`, which rolls forward and back through the same files, for
// the shadow replays inside `migrations checkpoint` and `migrations baseline`,
// for the `--dev-url` replay inside `migrations lint`, and for
// `migrations repair --resume-from`, which executes the remaining statements of
// the body that failed straight out of the file.
//
// The membership test is "does this verb execute SQL taken from the directory",
// and it is not answered by the verb's name. `repair` reads as metadata
// maintenance and is exactly that WITHOUT --resume-from; with it, the verb
// executes migration statements and belongs here. The gate is therefore applied
// per invocation there, not per verb — see cmd/migraterepair. The enumeration
// itself lives beside the namespace that registers the verbs, in
// cmd/migrations/integrity_class_test.go, so a new verb has a visible place
// where its verdict is missing.
//
// Verification guarding only the constructive direction is backwards. `down` is
// the direction where an operator cannot inspect the result afterwards, because
// the objects the run was supposed to remove are gone either way; and
// `checkpoint` is worse still, because it replays the tampered history onto a
// shadow database and then writes what it observed there into a NEW migration
// under a FRESH integrity file — laundering the tampering into a directory that
// verifies clean. That is the same shape stokaro/ptah#1095 closed on the
// compat surface, where `migrate import` was rewriting a directory
// `migrate apply` refuses.
//
// So the predicate is expressed once, here, and called by every verb in the
// class rather than reimplemented per verb. The compat surface reached the same
// conclusion for the same reason; see the file comment on
// cmd/atlas/migrate_integrity_gate.go, which records that leaving the rule
// inside `migrate apply` produced #974 and then #1095.
//
// # What the gate does NOT change
//
// An UNHASHED directory keeps its ungated behavior, exactly as it does on
// `migrations up`. A directory nobody ever hashed has no recorded intent to
// compare against, and refusing it would remove a capability rather than
// protect one. The gate fires on drift from a sum the directory actually
// carries.
//
// The stricter `migrations up --verify-sum` contract — where a MISSING sum file
// is itself an error — stays with that flag and is deliberately not part of
// this shared core. It is an explicit operator request for more than the
// default, and [AllowUnverifiedEnvVar] does not relax it; see
// cmd/migrateup/migrateup.go.
package migrationintegrity

import (
	"fmt"
	"io"
	"io/fs"

	"go.5x5.cz/ptah/internal/envbool"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// AllowUnverifiedEnvVar is the escape hatch: set it to a true value and a
// hashed directory that does NOT match its integrity file is executed anyway.
//
// It exists because refusing outright would remove a capability the tool has
// today, which repository policy forbids. An operator recovering from a botched
// edit genuinely may need to roll back through a directory whose sum is stale —
// that is precisely the moment the down files matter and precisely the moment
// re-hashing first would record the botched bytes as intended.
//
// It is an environment variable and NOT a flag on purpose. The conformance
// `cli-surface` tier asserts flag parity against the community binary, so a new
// flag on any verb this gate guards would break it. Every boolean `PTAH_*` is
// declared once through [go.5x5.cz/ptah/internal/envbool]; see
// cmd/internal/envboolguard for the guard that enforces it.
//
// Using it is never silent. A flag leaves a trace in the command line an
// operator can read back afterwards; an environment variable exported three
// shells ago does not, so a run whose integrity gate was overridden has to say
// so itself, and say what it skipped — see [Gate].
const AllowUnverifiedEnvVar = "PTAH_ALLOW_UNVERIFIED_MIGRATION_DIR"

// allowUnverified is the declaration of the variable, made once, in the package
// that owns the rule it relaxes.
//
// The default is false, which is the strict side. Every boolean `PTAH_*` in
// this tree opts IN to the more permissive behavior, so a typo lands on the
// strict default and fails closed.
var allowUnverified = envbool.New(AllowUnverifiedEnvVar, false)

// Gate verifies fsys against the integrity file it carries and reports which
// file verified, or the empty string when nothing was checked.
//
// Call it before the verb executes anything, and — where the verb connects to a
// database — before it connects. A gate that fires after the connection has
// already told the operator less than one that fires instead of it.
//
// The returned name is what the CALLER may claim it verified. It is empty both
// when the directory was never hashed and when [AllowUnverifiedEnvVar]
// suppressed a real failure, so a run that skipped the check cannot go on to
// report that a check passed. `migrations up` uses exactly that to decide
// whether its movable-OCI-tag provenance warning has anything to qualify.
//
// notice receives the override announcement, and receives it only when an
// override actually suppressed a refusal: a clean directory skips nothing, so
// it says nothing. Pass the command's stderr.
//
// The environment variable is resolved BEFORE the directory is read, so an
// unparseable value refuses the command whatever state the directory is in. A
// value that only fails the run on already-drifted directories would let
// `PTAH_ALLOW_UNVERIFIED_MIGRATION_DIR=yes` sit unnoticed in a CI environment
// file until the day it was load-bearing, which is the reverse of what the
// envbool contract is for.
func Gate(notice io.Writer, fsys fs.FS, format migrator.MigrationDirFormat) (string, error) {
	allow, err := allowUnverified.Resolve()
	if err != nil {
		return "", err
	}

	result, hashed, err := migratesum.VerifyHashed(fsys, format)
	if err != nil {
		return "", fmt.Errorf("migration sum verification failed: %w", err)
	}
	if !hashed {
		return "", nil
	}
	if result.OK() {
		return result.SumFileName, nil
	}
	if !allow {
		return "", fmt.Errorf("migration sum verification failed:\n%s", result.Describe())
	}
	fmt.Fprintf(notice,
		"warning: %s is set; %s verification was SKIPPED and this run is executing "+
			"migration SQL that no reviewed checksum covers:\n%s\n",
		AllowUnverifiedEnvVar,
		sumFileNameOf(result),
		result.Describe(),
	)
	return "", nil
}

// sumFileNameOf names the integrity file a drifted result belongs to, falling
// back to the Ptah spelling when the result did not record one.
func sumFileNameOf(result *migratesum.Result) string {
	if result.SumFileName != "" {
		return result.SumFileName
	}
	return migratesum.FileName
}
