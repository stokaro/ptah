package atlas

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/migration/migrator"
)

// checkFlywayBaselineHistory refuses an apply that would run a Flyway baseline
// against a database whose history the baseline does not stand above
// (stokaro/ptah#1003).
//
// A Flyway `B` file is a squash: it restates the schema the migrations it
// supersedes built. Applying it to a database that already ran those migrations
// is not a forward step, and the two tools disagreed about what to do with it.
// Measured against the pinned Atlas CE v1.3.0, CE has three answers for one
// class of file, chosen by how the version TOKEN compares as a string:
//
//	V2 applied, B10 added        -> silent skip, `No migration files to execute`, exit 0
//	V1,V2,V3 applied, B2.5 added -> `B2.5__base.sql was added out of order`, exit 1
//	V2 applied, B3 added         -> executes the baseline, exit 0
//
// Ptah keys pending-ness on the recorded version set, so before this check it
// ran the baseline in all three. Reproducing the skip was rejected under the
// parity rule's half (b): it drops the author's SQL with no exit code, no
// stdout byte and no `migrate status` line to say so, and once the squash
// retires the superseded files from atlas.sum nothing can re-derive the missing
// object — a production database and a fresh one built from the same directory
// diverge permanently, both reported OK. Running it blindly is not the answer
// either: in the common squash the baseline restates applied DDL, fails, and
// strands a dirty revision. So the run stops, and the operator decides.
//
// It fires only where the baseline cannot be read as a forward migration:
//
//   - this run would actually execute the baseline. That is asked of the
//     selected plan rather than of the recorded set, so every way a version
//     stops being pending answers it at once — already recorded, or covered by
//     a --baseline revision, whose assumed-applied set a dry run and the apply
//     it predicts must agree on;
//   - and either an already-applied migration is still covered by this
//     directory (so the baseline did not squash the history away), or a
//     migration carrying the baseline's own version has already been applied.
//     A fresh database satisfies neither, so it still runs the baseline, which
//     is the whole point of shipping one.
//
// Two deliberate abstentions. Under --exec-order=non-linear or linear-skip the
// operator has already made this decision explicitly, and non-linear is the
// escape hatch the refusal points at — measured, CE applies a below-mark
// baseline under it too. And when some OTHER pending migration is out of order,
// that refusal is the one to report: it is the one CE reports for the same
// directory (measured: V1,V3 applied plus B0__base.sql and V2__b.sql names
// V2__b.sql, not the baseline), and it names flags that resolve it.
func checkFlywayBaselineHistory(
	baseline *atlasmigrateimport.FlywayBaseline,
	execOrder migrator.ExecOrder,
	plan atlasmigrate.ApplyPlan,
) error {
	if baseline == nil || plan.Status == nil || execOrder != migrator.ExecOrderLinear {
		return nil
	}
	if !slices.Contains(plan.SelectedVersions, baseline.AtlasVersion) {
		return nil
	}
	applied := plan.Status.AppliedMigrations
	var covered []int64
	for _, version := range baseline.CoveredVersions {
		if slices.Contains(applied, version) {
			covered = append(covered, version)
		}
	}
	conflict := flywayBaselineConflict{
		covered:            covered,
		sameVersionApplied: slices.Contains(applied, baseline.SequenceVersion),
	}
	if len(conflict.covered) == 0 && !conflict.sameVersionApplied {
		return nil
	}
	for _, version := range plan.Status.OutOfOrderMigrations {
		if version != baseline.AtlasVersion {
			return nil
		}
	}
	return errors.New(flywayBaselineRefusal(baseline, conflict))
}

// flywayBaselineConflict is why one baseline was refused: the already-applied
// migrations this directory still covers, and whether the baseline's own
// version is one the database has already applied. Either alone is enough.
type flywayBaselineConflict struct {
	covered            []int64
	sameVersionApplied bool
}

// flywayBaselineRefusal renders the refusal, including the way through.
//
// Only routes measured to do what they promise are offered. Recording the
// baseline as applied without executing it is not one of them, on either tool:
// `migrate set` moves the database to EXACTLY the version given and deletes the
// rows above it, and a converted baseline sorts below every migration a
// database has actually run. Measured on both, `migrate set` at the baseline's
// version reports `1 set, 1 removed` and retires the real history, after which
// the next apply re-runs it and fails on `table s2 already exists`. `--baseline`
// refuses outright once the revisions table is non-empty. Saying so beats
// printing a command that quietly makes it worse.
func flywayBaselineRefusal(baseline *atlasmigrateimport.FlywayBaseline, conflict flywayBaselineConflict) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s is a Flyway baseline and this database already has migration history, "+
		"so it will not be treated as a fresh install; nothing has been applied", baseline.Source)

	fmt.Fprintf(&b, "\n\nthe baseline carries version %q and converts to Atlas version %d, which no revision records",
		baseline.Version, baseline.AtlasVersion)
	if conflict.sameVersionApplied {
		fmt.Fprintf(&b, ", while version %d — a migration of the same version — is already applied",
			baseline.SequenceVersion)
	}
	if len(conflict.covered) > 0 {
		fmt.Fprintf(&b, ", while %d already-applied migration(s) are still covered by this directory:\n", len(conflict.covered))
		for _, version := range conflict.covered {
			fmt.Fprintf(&b, "  %d\n", version)
		}
	} else {
		b.WriteString("\n")
	}

	b.WriteString("\nexecuting it would run a squashed schema on top of that history, and skipping it would leave " +
		"its SQL unapplied here while a database created from this same directory runs it, with both reported OK. " +
		"Neither is Ptah's decision to make silently.\n")

	b.WriteString("\nways forward:\n")
	fmt.Fprintf(&b, "  - execute %s against this database now: re-run with --exec-order=non-linear\n", baseline.Source)
	b.WriteString("  - keep the baseline for new environments: apply this directory to databases with no recorded " +
		"history, and let this one continue from the migrations it already has\n")
	b.WriteString("\nrecording it as applied without executing it has no safe route here: `migrate set` moves the " +
		"database to exactly the version given and removes the revisions above it, and this baseline sorts below " +
		"every migration this database has already run.\n")
	return b.String()
}
