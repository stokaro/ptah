package atlas

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/revisiontable"
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
//   - this run would execute the baseline, or the exact source token makes it
//     look applied under a different migration. The first is asked of the
//     selected plan. For the second, the converted provider records a surviving
//     B file with the Atlas baseline revision type. CE records both V and B as
//     applied, so this is a deliberate Ptah marker: it preserves exact token
//     interoperability while retaining the one bit needed to distinguish a
//     settled B2 from an unsafe V2 -> B2 transition;
//   - and either an already-applied migration is still covered by this
//     directory (so the baseline did not squash the history away), or a
//     migration carrying the baseline's own version has already been applied.
//     A fresh database satisfies neither, so it still runs the baseline, which
//     is the whole point of shipping one.
//
// The second clause is an IDENTITY question and must not be answered with the
// int64 ordering key, which collapses "2", "02" and "002" on purpose. Asking it
// that way refused twelve directories that Atlas CE, and Ptah's previous
// release, both apply — zero-padded versions being ordinary Flyway practice.
// atlasmigrateimport.FlywaySurvivingBaseline answers it on the version TOKEN
// instead and hands back the migrations, not the numbers; see
// flywaySameVersionMigrations there.
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
	applied := plan.Status.AppliedMigrations
	conflict := flywayBaselineConflict{
		covered:     appliedFlywayMigrations(baseline.Covered, applied),
		sameVersion: appliedFlywayMigrations(baseline.SameVersion, applied),
	}
	selected := slices.Contains(plan.SelectedVersions, baseline.AtlasVersion)
	if !selected {
		conflict.sameVersion = appliedFlywaySameVersionConflict(baseline, plan)
		conflict.exactIdentity = len(conflict.sameVersion) > 0
		conflict.covered = nil
	}
	if len(conflict.covered) == 0 && len(conflict.sameVersion) == 0 {
		return nil
	}
	if !selected {
		return errors.New(flywayBaselineRefusal(baseline, conflict))
	}
	for _, version := range plan.Status.OutOfOrderMigrations {
		if version != baseline.AtlasVersion {
			return nil
		}
	}
	return errors.New(flywayBaselineRefusal(baseline, conflict))
}

// flywayBaselineConflict is why one baseline was refused: the already-applied
// migrations this directory still covers, and the already-applied migrations
// carrying the baseline's own version token. Either alone is enough.
//
// Both are file lists rather than version lists because the refusal has to name
// something the operator can find. The int64 versions are Ptah's projection of
// Atlas CE's version strings, and Atlas CE reports the very same migrations as
// `2` and `02`; a message asserting that "version 4611686018427510315 is already
// applied" names a number that appears in no file name and in no Atlas output.
type flywayBaselineConflict struct {
	covered       []atlasmigrateimport.FlywayMigrationVersion
	sameVersion   []atlasmigrateimport.FlywayMigrationVersion
	exactIdentity bool
}

// appliedFlywaySameVersionConflict recognizes the transition from a versioned
// migration to a baseline carrying the same exact Flyway token. Exact Atlas
// revision identity makes the baseline look applied before checksum validation.
// The revision row contains the token, normalized description, and body
// checksum, but not the V/B prefix or source filename. Even a matching checksum
// cannot distinguish V2 from B2 when their SQL is byte-identical. The provider
// therefore marks a surviving B migration with the combined baseline/applied
// type on first execution so a later run is provable without changing its exact
// token. An explicit --baseline keeps Atlas's pure baseline type and carries a
// durable source-baseline marker only when the selected source was B. A V2
// baselined before B2 appeared remains indistinguishable and fails closed, as
// does an ordinary applied row.
func appliedFlywaySameVersionConflict(
	baseline *atlasmigrateimport.FlywayBaseline,
	plan atlasmigrate.ApplyPlan,
) []atlasmigrateimport.FlywayMigrationVersion {
	if len(baseline.SameVersion) == 0 || !slices.Contains(plan.Status.AppliedMigrationKeys, baseline.Version) {
		return nil
	}
	for _, revision := range plan.Revisions {
		if revision.RevisionVersion() != baseline.Version {
			continue
		}
		sourceBaseline := revision.AtlasType == migrator.AtlasRevisionTypeBaseline &&
			revision.OperatorVersion == revisiontable.SourceBaselineOperatorVersion
		executedBaseline := revision.AtlasType&(migrator.AtlasRevisionTypeBaseline|migrator.AtlasRevisionTypeApplied) ==
			migrator.AtlasRevisionTypeBaseline|migrator.AtlasRevisionTypeApplied
		if sourceBaseline || executedBaseline {
			return nil
		}
	}
	return baseline.SameVersion
}

// appliedFlywayMigrations keeps the migrations a database has already recorded.
func appliedFlywayMigrations(
	migrations []atlasmigrateimport.FlywayMigrationVersion,
	applied []int64,
) []atlasmigrateimport.FlywayMigrationVersion {
	var out []atlasmigrateimport.FlywayMigrationVersion
	for _, migration := range migrations {
		if slices.Contains(applied, migration.Version) {
			out = append(out, migration)
		}
	}
	return out
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

	if conflict.exactIdentity {
		fmt.Fprintf(&b, "\n\nthe baseline carries exact revision identity %q, which this database already records "+
			"for another migration:\n", baseline.Version)
	} else {
		fmt.Fprintf(&b, "\n\nthe baseline carries version %q and no revision here records it, "+
			"but this database has already applied:\n", baseline.Version)
	}
	for _, migration := range conflict.sameVersion {
		fmt.Fprintf(&b, "  %s — a migration carrying the same version %q\n", migration.Source, baseline.Version)
	}
	for _, migration := range conflict.covered {
		fmt.Fprintf(&b, "  %s — still covered by this directory\n", migration.Source)
	}

	b.WriteString("\nexecuting it would run a squashed schema on top of that history, and skipping it would leave " +
		"its SQL unapplied here while a database created from this same directory runs it, with both reported OK. " +
		"Neither is Ptah's decision to make silently.\n")

	b.WriteString("\nways forward:\n")
	if conflict.exactIdentity {
		fmt.Fprintf(&b, "  - if %s must run here, review and execute its SQL manually; the migration runner cannot record "+
			"two different migrations under exact revision identity %q\n", baseline.Source, baseline.Version)
	} else {
		fmt.Fprintf(&b, "  - execute %s against this database now: re-run with --exec-order=non-linear\n", baseline.Source)
	}
	b.WriteString("  - keep the baseline for new environments: apply this directory to databases with no recorded " +
		"history, and let this one continue from the migrations it already has\n")
	if !conflict.exactIdentity {
		b.WriteString("\nrecording it as applied without executing it has no safe route here: `migrate set` moves the " +
			"database to exactly the version given and removes the revisions above it, and this baseline sorts below " +
			"every migration this database has already run.\n")
	}
	return b.String()
}
