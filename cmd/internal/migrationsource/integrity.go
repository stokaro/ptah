package migrationsource

import (
	"fmt"
	"io"

	"go.5x5.cz/ptah/cmd/internal/cliobs"
	"go.5x5.cz/ptah/internal/migrationintegrity"
	"go.5x5.cz/ptah/migration/migrator"
)

// VerifyOptions selects how strict one [Verify] call is and where it reports.
type VerifyOptions struct {
	// RequireSum is the `--verify-sum` contract: a MISSING integrity file is
	// an error rather than a pass. See [migrationintegrity.Options].
	RequireSum bool
	// Verbose prints the positive confirmation that a directory verified.
	// Without it a successful verification says nothing, which is the right
	// default for a verb whose output is consumed by another tool.
	Verbose bool
}

// Verify runs the migration integrity gate over source and then qualifies what
// the verification actually established.
//
// It exists because the qualification is the half that kept being forgotten.
// The gate itself moved into [migrationintegrity] after `migrations up` owning
// it privately let five other verbs execute directories `up` refused
// (stokaro/ptah#928 item 4). The provenance sentence had the same shape of
// problem one level up: `up` computed it, and every other verb that verifies a
// sum pulled through a movable OCI tag would have had to remember to compute it
// again. Both halves are therefore stated once, here, beside the [Source] type
// that carries the provenance bits the sentence is built from.
//
// notice receives the override announcement from the gate; pass the command's
// standard error. emit receives the operator-facing warning and the verbose
// confirmation. runtime records the same warning as structured output, so a run
// whose provenance was qualified is visible to a log collector and not only to
// whoever was watching the terminal.
func Verify(
	notice io.Writer,
	emit cliobs.Emitter,
	runtime *cliobs.Runtime,
	source Source,
	format migrator.MigrationDirFormat,
	opts VerifyOptions,
) error {
	verifiedSumFile, err := migrationintegrity.GateWith(
		notice,
		source.FileSystem,
		format,
		migrationintegrity.Options{RequireSum: opts.RequireSum},
	)
	if err != nil {
		return err
	}
	if opts.Verbose && verifiedSumFile != "" {
		emit.Printf("%s verified: migrations directory is intact\n", verifiedSumFile)
	}
	// Every error path returned above, so a run that refused never reaches
	// here and never qualifies a claim it did not make.
	warning := MutableTagSumWarning(source, verifiedSumFile)
	if warning == "" {
		return nil
	}
	runtime.Logger().Warn(
		"migration sum verified through a movable OCI tag",
		"reference", source.OCI.Reference,
		"digest", source.OCI.Descriptor.Digest.String(),
		"sum_file", verifiedSumFile,
	)
	emit.Printf("Warning: %s\n", warning)
	return nil
}

// MutableTagSumWarning returns the provenance sentence a sum verification
// cannot carry on its own, or the empty string when there is nothing to say.
//
// A sum file proves that a directory matches the sum recorded beside it. For a
// local directory that sum was reviewed in version control next to the
// migrations. For an OCI artifact the sum travels inside the artifact, so the
// check is self-referential: anyone who can push to the repository can rewrite
// the migrations, rehash them, and repoint a tag, and the verification still
// passes over bytes nobody reviewed (stokaro/ptah#944). Only the reference's
// own shape separates the two cases, so the warning fires exactly when a
// verification actually ran and the reference was a tag rather than a digest.
//
// verifiedSumFile is the integrity file that verified; the empty string means
// nothing was verified, so nothing was claimed and nothing needs qualifying.
func MutableTagSumWarning(source Source, verifiedSumFile string) string {
	if verifiedSumFile == "" || source.OCI == nil || source.OCI.PinnedByDigest {
		return ""
	}
	return fmt.Sprintf(
		"%s is a movable tag: %s travels inside the artifact, so verifying it proves the pulled "+
			"files are internally consistent, not that they are the reviewed ones. "+
			"This tag resolved to %s; pass %s to pin these exact bytes.",
		source.OCI.Reference,
		verifiedSumFile,
		source.OCI.Descriptor.Digest.String(),
		source.OCI.DigestReference,
	)
}

// VerifySumQualifier is the sentence every `--verify-sum` help text ends with,
// on every verb that registers the flag.
//
// It is a shared constant rather than repeated prose because the wording is
// itself the deliverable of stokaro/ptah#928 item 5. The flag must not read as
// tamper detection anywhere: rewriting a migration AND re-hashing it produces
// an artifact that passes `--verify-sum` at exit 0 and installs whatever the
// rewrite said. `migrations up` was reworded for that in stokaro/ptah#1093 and
// `migrations push` was not, so one surface went on promising more than it
// checks for months. Sharing the sentence makes that drift impossible rather
// than merely fixed once, and the flag-surface gate in cmd/migrations asserts
// every registration ends with it.
const VerifySumQualifier = "A sum checks a directory against the sum stored beside it, so an " +
	"oci:// tag source proves internal consistency only; pin a digest for authenticity"

// VerifySumUsage builds one verb's `--verify-sum` help from its own lead and
// the shared qualifier.
//
// The lead differs per verb because what the flag ADDS differs per verb: on a
// verb that already runs the always-on hashed gate it adds only the demand that
// a sum exist, while on one that runs no gate at all it adds the whole check.
// Stating that per verb is what stops the help from being accurate on one and
// wrong on the next; the qualifier, which is about what a sum can prove at all,
// is the half that is identical everywhere.
func VerifySumUsage(lead string) string {
	return lead + ". " + VerifySumQualifier
}
