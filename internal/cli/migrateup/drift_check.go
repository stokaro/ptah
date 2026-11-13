package migrateup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/cliobs"
	"ptah.run/internal/cli/internal/engineruntime"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/revisiontable"
	"ptah.run/migration/migrator"
	"ptah.run/migration/shadow"
)

const (
	checkDriftFlag   = "check-drift"
	shadowDBFlag     = "shadow-db"
	driftExcludeFlag = "drift-exclude"
)

// driftCheck is --check-drift: a comparison of the target with the schema its
// applied history produces, made under the migration lock before any pending
// migration runs.
//
// The comparison baseline is the history the revision table records, not the
// directory's latest state, so a pending migration's changes are never read as
// drift. It runs where the selection does, after the history read the
// selection was made from, so a cooperating runner cannot move the history
// between the check and the apply. DDL issued by something that does not take
// the migration lock can still land after the check; the lock coordinates
// runners, not every session that can alter the schema.
type driftCheck struct {
	shadowURL      string
	exclude        []string
	connectTimeout time.Duration
	runtime        *engine.Runtime
	conn           *dbschema.DatabaseConnection
	mig            *migrator.Migrator
	// metadata are the target's bookkeeping tables under the names this run
	// configured, which no migration creates.
	metadata []string
	// result is what the check found, once it ran.
	result *migrator.DriftCheck
}

// prepareDriftCheck is the check --check-drift asks for, with its shadow
// database provisioned when the URL is a docker:// one, or nil when it was not
// asked for. The release function is always safe to call.
func prepareDriftCheck(
	ctx context.Context,
	opts options,
	dbURL string,
	settings parsedMigrationSettings,
	conn *dbschema.DatabaseConnection,
	mig *migrator.Migrator,
) (*driftCheck, func(), error) {
	if !opts.checkDrift {
		return nil, func() {}, nil
	}
	runtime, err := engineruntime.Resolve(ctx, engineruntime.DatabaseURL(dbURL))
	if err != nil {
		return nil, func() {}, err
	}
	// Before the migration lock, so the lock is not held while a container
	// starts.
	shadowURL, release, err := devdocker.Resolve(ctx, opts.shadowDB, devdocker.Options{})
	if err != nil {
		return nil, func() {}, err
	}
	return newDriftCheck(opts, shadowURL, settings.connectTimeout, runtime, conn, mig, settings.revisionFormat, opts.migrationsTable),
		release, nil
}

// newDriftCheck is the check --check-drift asks for, or nil when it was not
// asked for. revisionFormat and migrationsTable are the run's resolved
// revision-table settings.
func newDriftCheck(
	opts options,
	shadowURL string,
	connectTimeout time.Duration,
	runtime *engine.Runtime,
	conn *dbschema.DatabaseConnection,
	mig *migrator.Migrator,
	revisionFormat migrator.RevisionTableFormat,
	migrationsTable string,
) *driftCheck {
	if !opts.checkDrift {
		return nil
	}
	return &driftCheck{
		shadowURL:      shadowURL,
		exclude:        opts.driftExclude,
		connectTimeout: connectTimeout,
		runtime:        runtime,
		conn:           conn,
		mig:            mig,
		metadata:       revisiontable.Configured(string(revisionFormat), migrationsTable),
	}
}

// validateDriftFlags refuses a drift setting that would be silently ignored:
// a shadow database or an exclusion typed without --check-drift, and the
// check without a shadow database to replay into. shadowURL is the resolved
// value, which a project's dev_url may supply.
func validateDriftFlags(checkDrift, shadowTyped, excludeTyped bool, shadowURL string) error {
	switch {
	case checkDrift && strings.TrimSpace(shadowURL) == "":
		return fmt.Errorf("--%s requires --%s: the applied history is replayed on a disposable shadow database",
			checkDriftFlag, shadowDBFlag)
	case !checkDrift && shadowTyped:
		return fmt.Errorf("--%s is used only by --%s", shadowDBFlag, checkDriftFlag)
	case !checkDrift && excludeTyped:
		return fmt.Errorf("--%s is used only by --%s", driftExcludeFlag, checkDriftFlag)
	}
	return nil
}

// DriftError is a target whose schema differs from the schema its applied
// history produces. The run applied nothing.
type DriftError struct {
	Check migrator.DriftCheck
}

func (e *DriftError) Error() string {
	return fmt.Sprintf("the database schema differs from the schema its %s, in %d %s, "+
		"so no pending migration was run; first: %s",
		appliedMigrations(len(e.Check.Replayed)), len(e.Check.Findings), plural(len(e.Check.Findings), "place", "places"),
		e.Check.Findings[0].Message)
}

// DriftCheckUnavailableError is a drift check that could not decide whether
// the target drifted. It is never a clean check, and the run applied nothing.
type DriftCheckUnavailableError struct {
	Check migrator.DriftCheck
	Err   error
}

func (e *DriftCheckUnavailableError) Error() string {
	return fmt.Sprintf("the drift check could not compare the database with its applied history, "+
		"so no pending migration was run: %s", e.Check.Message)
}

func (e *DriftCheckUnavailableError) Unwrap() error { return e.Err }

// planGuards are the checks a selection must pass before anything runs: the
// approved sequence first, because it is the cheaper one and a selection
// nobody approved is refused whatever the schema holds, then the drift check.
type planGuards struct {
	expected *expectedSequence
	drift    *driftCheck
}

func (g planGuards) guard() migrator.MigrationPlanGuard {
	expected, drift := g.expected.guard(), g.drift.guard()
	switch {
	case expected == nil:
		return drift
	case drift == nil:
		return expected
	}
	return func(ctx context.Context, plan migrator.MigrationPlan) error {
		if err := expected(ctx, plan); err != nil {
			return err
		}
		return drift(ctx, plan)
	}
}

// found is what the check found, or nil when it was not asked for or never
// ran.
func (d *driftCheck) found() *migrator.DriftCheck {
	if d == nil {
		return nil
	}
	return d.result
}

// reportDriftCheck tells a person what the check found. A clean check is a
// line of the run's output; a drifted one is the diagnostic, on standard
// error, that the run's exit status stands for. An unavailable check is
// reported by the error the run returns.
func reportDriftCheck(cmd *cobra.Command, emit cliobs.Emitter, drift *driftCheck) {
	check := drift.found()
	switch {
	case check == nil:
	case check.Outcome == migrator.DriftClean:
		emit.Printf("Drift check: the database matches the schema its %s\n", appliedMigrations(len(check.Replayed)))
	case check.Outcome == migrator.DriftDrifted:
		writeDriftReport(cmd.ErrOrStderr(), *check)
	}
}

// driftExitStatus gives a drifted check exit status 1, the expected negative
// result, and leaves every other error to the root, which prints it and exits
// 2. writeDriftReport has printed the drift.
func driftExitStatus(err error) error {
	if _, drifted := errors.AsType[*DriftError](err); drifted {
		return exitcode.New(1, err)
	}
	return err
}

// guard is the check as the migrator runs it: under the lock, for every
// selection including an empty one. A nil check guards nothing.
func (d *driftCheck) guard() migrator.MigrationPlanGuard {
	if d == nil {
		return nil
	}
	return func(ctx context.Context, _ migrator.MigrationPlan) error {
		check, err := d.run(ctx)
		d.result = &check
		return err
	}
}

func (d *driftCheck) run(ctx context.Context) (migrator.DriftCheck, error) {
	history, err := d.mig.AppliedHistory(ctx)
	if err != nil {
		check := migrator.DriftCheck{Outcome: migrator.DriftUnable, Stage: "history", Reason: "history_read_error", Message: err.Error()}
		if unreplayable, ok := errors.AsType[*migrator.UnreplayableHistoryError](err); ok {
			check.Reason = unreplayable.Reason
		}
		return check, &DriftCheckUnavailableError{Check: check, Err: err}
	}
	check := migrator.DriftCheck{Replayed: history.Versions(), ReplayedKeys: history.VersionKeys()}
	if len(check.Replayed) == 0 {
		check.Replayed, check.ReplayedKeys = nil, nil
	}

	selection := atlasschema.LiveSelection{Exclude: d.exclude}
	info := d.conn.Info()
	result, err := shadow.VerifyApplied(selection.ReadContext(ctx, d.runtime.Selections()), shadow.AppliedVerifyOptions{
		Runtime:           d.runtime,
		ShadowDatabaseURL: d.shadowURL,
		TargetConn:        d.conn,
		Migrations:        history.Migrations,
		Dialect:           info.Dialect,
		Capabilities:      info.Capabilities,
		MetadataTables:    d.metadata,
		Scope:             d.scope(selection),
		ConnectTimeout:    d.connectTimeout,
	})
	if err != nil {
		check.Outcome, check.Stage, check.Reason, check.Message = migrator.DriftUnable, "shadow", "verification_error", err.Error()
		if verification, ok := errors.AsType[*shadow.VerificationError](err); ok {
			check.Stage = verification.Result.Stage
			if len(verification.Result.Mismatches) > 0 {
				check.Reason = verification.Result.Mismatches[0].Kind
			}
		}
		return check, &DriftCheckUnavailableError{Check: check, Err: err}
	}
	switch {
	case len(result.Drift) > 0:
		check.Outcome, check.Findings = migrator.DriftDrifted, driftFindings(result.Drift)
		return check, &DriftError{Check: check}
	case len(result.Undecided) > 0:
		check.Outcome, check.Stage, check.Reason = migrator.DriftUnable, "schema-match", "incomplete_coverage"
		check.Findings = driftFindings(result.Undecided)
		check.Message = fmt.Sprintf("the database read could not describe %d %s the applied migrations create, "+
			"so whether the database holds %s is unknown; first: %s",
			len(result.Undecided), plural(len(result.Undecided), "object", "objects"),
			plural(len(result.Undecided), "it", "them"), result.Undecided[0].Message)
		return check, &DriftCheckUnavailableError{Check: check}
	default:
		check.Outcome = migrator.DriftClean
		return check, nil
	}
}

// scope applies --drift-exclude to both sides of the comparison, the way
// `schema diff --exclude` does.
func (d *driftCheck) scope(selection atlasschema.LiveSelection) func(
	context.Context, *catalog.Database, *schemamodel.Database,
) (*catalog.Database, *schemamodel.Database, error) {
	if len(selection.Exclude) == 0 {
		return nil
	}
	info := d.conn.Info()
	return func(ctx context.Context, target *catalog.Database, replayed *schemamodel.Database) (*catalog.Database, *schemamodel.Database, error) {
		return selection.Apply(ctx, d.runtime, info, target, replayed)
	}
}

func driftFindings(mismatches []shadow.Mismatch) []migrator.DriftFinding {
	findings := make([]migrator.DriftFinding, 0, len(mismatches))
	for _, mismatch := range mismatches {
		findings = append(findings, migrator.DriftFinding{
			Kind: mismatch.Kind, Object: mismatch.Object, Table: mismatch.Table,
			Column: mismatch.Column, Constraint: mismatch.Constraint, Message: mismatch.Message,
		})
	}
	return findings
}

// writeDriftReport tells a person what a drifted check found, one line per
// difference. The run returns its own exit status for drift, so this is the
// diagnostic the root does not print.
func writeDriftReport(out io.Writer, check migrator.DriftCheck) {
	_, _ = fmt.Fprintf(out, "Drift check: the database differs from the schema its %s:\n", appliedMigrations(len(check.Replayed)))
	for _, finding := range check.Findings {
		_, _ = fmt.Fprintf(out, "  - %s\n", finding.Message)
	}
	_, _ = fmt.Fprintln(out, "No pending migration was run. Bring the database back to its history, "+
		"or record the change in a migration, then run again.")
}

// appliedMigrations is "N applied migrations produce", agreed in number.
func appliedMigrations(count int) string {
	return fmt.Sprintf("%d applied %s", count, plural(count, "migration produces", "migrations produce"))
}

func plural(count int, one, many string) string {
	if count == 1 {
		return one
	}
	return many
}
