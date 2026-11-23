// Package driftgate is the drift check of a migration target: a comparison of
// the target with the schema its applied history produces, made under the
// migration lock. A migration run makes it before any pending migration runs,
// and a report-only run makes it and applies nothing.
//
// The comparison baseline is the history the revision table records, not the
// directory's latest state, so a pending migration's changes are never read as
// drift. The check runs where the selection does, after the history read the
// selection was made from, so a cooperating runner cannot move the history
// between the check and the apply. DDL issued by something that does not take
// the migration lock can still land after the check; the lock coordinates
// runners, not every session that can alter the schema.
//
// `ptah migrations up --check-drift`, the atlas.hcl `check "migrate_apply" {
// drift {} }` block of both binaries and `ptah migrations drift` run this one
// check. They differ only in the [Options] their surface selects, and in
// whether the migrator runs the check as a [Check.Guard] or the caller runs it
// with [Check.Run].
package driftgate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/backslashmode"
	"ptah.run/internal/planstyle"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
	"ptah.run/migration/shadow"
)

// OnDrift says what a check that did not come back clean does to the run.
type OnDrift int

const (
	// Refuse stops the run before any pending migration: a drifted target is
	// a [*DriftError] and a check that could not decide is an
	// [*UnavailableError].
	Refuse OnDrift = iota
	// Continue records what the check found and lets the run go on. The caller
	// reports it, with [WriteReport] or [WriteUnavailable].
	Continue
)

// HistoryReader reads the applied history the check replays.
// [*migrator.Migrator] is one.
type HistoryReader interface {
	AppliedHistory(ctx context.Context) (migrator.AppliedHistory, error)
}

// Options configures a [Check].
type Options struct {
	// ShadowURL is the disposable database the applied history is replayed
	// on. It must be empty and distinct from the target.
	ShadowURL string
	// Selection is what the comparison leaves out, with where each selector
	// came from and the stream that is told about one that matched nothing.
	Selection      atlasschema.LiveSelection
	ConnectTimeout time.Duration
	Runtime        *engine.Runtime
	// Conn is the target.
	Conn *dbschema.DatabaseConnection
	// History reads the history the target's revision table records.
	History HistoryReader
	// MetadataTables are the target's bookkeeping tables under the names the
	// run configured, which no migration creates.
	MetadataTables []string
	OnDrift        OnDrift
	// SkipWithoutHistory skips the check on a target with no applied
	// migration, where there is no history to compare with. Without it the
	// check compares the target with an empty schema.
	SkipWithoutHistory bool
}

// Check is one run's drift check. A nil *Check is a check nobody asked for: it
// guards nothing and found nothing.
type Check struct {
	opts    Options
	result  *migrator.DriftCheck
	skipped bool
}

// New returns the check opts describe.
func New(opts Options) *Check {
	return &Check{opts: opts}
}

// OnDrift is what a check that did not come back clean does to the run, or
// [Refuse] for a nil check.
func (c *Check) OnDrift() OnDrift {
	if c == nil {
		return Refuse
	}
	return c.opts.OnDrift
}

// Guard is the check as the migrator runs it: under the lock, for every
// selection including an empty one. A nil check guards nothing.
func (c *Check) Guard() migrator.MigrationPlanGuard {
	if c == nil {
		return nil
	}
	return func(ctx context.Context, _ migrator.MigrationPlan) error {
		_, err := c.Run(ctx)
		if c.opts.OnDrift == Continue {
			return nil
		}
		return err
	}
}

// Result is what one run of a check found.
type Result struct {
	// Check is the outcome, in the form a run document reports it.
	Check migrator.DriftCheck
	// Diff is the change that brings the target back to the schema its
	// applied history produces, which Check.Findings itemize. It is nil unless
	// Check.Outcome is [migrator.DriftDrifted].
	Diff *difftypes.SchemaDiff
	// Undecided are the objects the applied history creates that the target
	// read could not describe, when the check found drift as well: the drift
	// decides the outcome, and Diff does not cover these. A check that found
	// no drift reports them in Check.Findings, as one that could not decide.
	Undecided []migrator.DriftFinding
}

// Run makes the check now and returns what it found, for a caller that runs
// it itself rather than through the migrator. The caller holds the target's
// migration lock, as [migrator.Migrator.HoldMigrationLock] does, so the history
// it reads is the history the target still has when the target is read.
//
// A drifted target comes back with a [*DriftError], and a check that could not
// decide with an [*UnavailableError]; the result says what was found either
// way. [Options.OnDrift] does not apply here: the caller decides what the
// outcome does. A check skipped under [Options.SkipWithoutHistory] returns the
// zero Result and no error, and [Check.Skipped] then reports true. What Run
// found is also what [Check.Found] returns afterwards.
func (c *Check) Run(ctx context.Context) (Result, error) {
	result, skipped, err := c.run(ctx)
	c.skipped = skipped
	if skipped {
		return Result{}, nil
	}
	c.result = &result.Check
	return result, err
}

// Found is what the check found, or nil when it was not asked for, never ran,
// or was skipped.
func (c *Check) Found() *migrator.DriftCheck {
	if c == nil {
		return nil
	}
	return c.result
}

// Skipped reports that the check ran on a target with no applied migration and
// compared nothing, as [Options.SkipWithoutHistory] asks.
func (c *Check) Skipped() bool {
	return c != nil && c.skipped
}

// DriftError is a target whose schema differs from the schema its applied
// history produces. The run applied nothing.
type DriftError struct {
	Check migrator.DriftCheck
}

func (e *DriftError) Error() string {
	return fmt.Sprintf("the database schema differs from the schema its %s, in %d %s, "+
		"so no pending migration was run; first: %s",
		AppliedMigrations(e.Check), len(e.Check.Findings), plural(len(e.Check.Findings), "place", "places"),
		e.Check.Findings[0].Message)
}

// UnavailableError is a drift check that could not decide whether the target
// drifted. It is never a clean check, and the run applied nothing.
type UnavailableError struct {
	Check migrator.DriftCheck
	Err   error
}

func (e *UnavailableError) Error() string {
	return fmt.Sprintf("the drift check could not compare the database with its applied history, "+
		"so no pending migration was run: %s", e.Check.Message)
}

func (e *UnavailableError) Unwrap() error { return e.Err }

func (c *Check) run(ctx context.Context) (Result, bool, error) {
	history, err := c.opts.History.AppliedHistory(ctx)
	if err != nil {
		check := migrator.DriftCheck{Outcome: migrator.DriftUnable, Stage: "history", Reason: "history_read_error", Message: err.Error()}
		if unreplayable, ok := errors.AsType[*migrator.UnreplayableHistoryError](err); ok {
			check.Reason = unreplayable.Reason
		}
		return Result{Check: check}, false, &UnavailableError{Check: check, Err: err}
	}
	check := migrator.DriftCheck{Replayed: history.Versions(), ReplayedKeys: history.VersionKeys()}
	if len(check.Replayed) == 0 {
		check.Replayed, check.ReplayedKeys = nil, nil
	}
	for _, repeatable := range history.Repeatables {
		check.ReplayedRepeatables = append(check.ReplayedRepeatables, repeatable.Identity)
	}
	if c.opts.SkipWithoutHistory && len(check.Replayed) == 0 && len(check.ReplayedRepeatables) == 0 {
		return Result{}, true, nil
	}

	info := c.opts.Conn.Info()
	result, err := shadow.VerifyApplied(c.opts.Selection.ReadContext(ctx, c.opts.Runtime.Selections()), shadow.AppliedVerifyOptions{
		Runtime:           c.opts.Runtime,
		ShadowDatabaseURL: c.opts.ShadowURL,
		TargetConn:        c.opts.Conn,
		Migrations:        history.Migrations,
		Repeatables:       history.Repeatables,
		Dialect:           info.Dialect,
		Capabilities:      info.Capabilities,
		MetadataTables:    c.opts.MetadataTables,
		Scope:             c.scope(),
		ConnectTimeout:    c.opts.ConnectTimeout,
	})
	if err != nil {
		check.Outcome, check.Stage, check.Reason, check.Message = migrator.DriftUnable, "shadow", "verification_error", err.Error()
		if verification, ok := errors.AsType[*shadow.VerificationError](err); ok {
			check.Stage = verification.Result.Stage
			if len(verification.Result.Mismatches) > 0 {
				check.Reason = verification.Result.Mismatches[0].Kind
			}
		}
		return Result{Check: check}, false, &UnavailableError{Check: check, Err: err}
	}
	switch {
	case len(result.Drift) > 0:
		check.Outcome, check.Findings = migrator.DriftDrifted, findings(result.Drift)
		found := Result{Check: check, Diff: result.Diff}
		if len(result.Undecided) > 0 {
			found.Undecided = findings(result.Undecided)
		}
		return found, false, &DriftError{Check: check}
	case len(result.Undecided) > 0:
		check.Outcome, check.Stage, check.Reason = migrator.DriftUnable, "schema-match", "incomplete_coverage"
		check.Findings = findings(result.Undecided)
		check.Message = fmt.Sprintf("the database read could not describe %d %s the applied migrations create, "+
			"so whether the database holds %s is unknown; first: %s",
			len(result.Undecided), plural(len(result.Undecided), "object", "objects"),
			plural(len(result.Undecided), "it", "them"), result.Undecided[0].Message)
		return Result{Check: check}, false, &UnavailableError{Check: check}
	default:
		check.Outcome = migrator.DriftClean
		return Result{Check: check}, false, nil
	}
}

// Reconcile plans the change that brings a drifted target back to the schema
// its applied history produces: diff, the [Result.Diff] of a drifted check,
// planned and rendered as text for the server info describes. The plan's
// statements are written for a session nobody has opened yet, as `schema
// compare` writes the ones it prints. Nothing runs them, and a person reviews
// them before anything does: a drop of what no applied migration created is
// among them, and the data it holds goes with it.
func Reconcile(ctx context.Context, runtime *engine.Runtime, info catalog.ServerInfo, diff *difftypes.SchemaDiff) (planner.RenderedPlan, error) {
	return planner.GenerateSchemaDiffTextPlan(ctx, runtime, diff, info.Dialect, planext.Options{
		Capabilities: backslashmode.ForLater(info.Capabilities),
		Statements:   planstyle.Statements,
	})
}

// scope applies the selection to both sides of the comparison, the way
// `schema diff --exclude` does, and warns about a selector that matched
// nothing on either side.
func (c *Check) scope() func(
	context.Context, *catalog.Database, *schemamodel.Database,
) (*catalog.Database, *schemamodel.Database, error) {
	if len(c.opts.Selection.Exclude) == 0 {
		return nil
	}
	info := c.opts.Conn.Info()
	return func(ctx context.Context, target *catalog.Database, replayed *schemamodel.Database) (*catalog.Database, *schemamodel.Database, error) {
		return c.opts.Selection.Apply(ctx, c.opts.Runtime, info, target, replayed)
	}
}

func findings(mismatches []shadow.Mismatch) []migrator.DriftFinding {
	found := make([]migrator.DriftFinding, 0, len(mismatches))
	for _, mismatch := range mismatches {
		found = append(found, migrator.DriftFinding{
			Kind: mismatch.Kind, Object: mismatch.Object, Table: mismatch.Table,
			Column: mismatch.Column, Constraint: mismatch.Constraint, Message: mismatch.Message,
		})
	}
	return found
}

// WriteReport tells a person what a drifted check found, one line per
// difference, and what the run did about it under onDrift.
func WriteReport(out io.Writer, check migrator.DriftCheck, onDrift OnDrift) {
	_, _ = fmt.Fprintf(out, "Drift check: the database differs from the schema its %s:\n", AppliedMigrations(check))
	for _, finding := range check.Findings {
		_, _ = fmt.Fprintf(out, "  - %s\n", finding.Message)
	}
	if onDrift == Continue {
		_, _ = fmt.Fprintln(out, "The pending migrations run anyway, because the check is set to continue on drift.")
		return
	}
	_, _ = fmt.Fprintln(out, "No pending migration was run. Bring the database back to its history, "+
		"or record the change in a migration, then run again.")
}

// WriteUnavailable tells a person that a check set to continue could not
// decide whether the target drifted, and that the run went on. A check set to
// refuse reports the same through its [*UnavailableError].
func WriteUnavailable(out io.Writer, check migrator.DriftCheck) {
	_, _ = fmt.Fprintf(out, "Drift check: could not compare the database with its applied history: %s\n", check.Message)
	_, _ = fmt.Fprintln(out, "The pending migrations run anyway, because the check is set to continue on drift.")
}

// AppliedMigrations says what the check replayed, as the subject of the verb
// that ends the phrase: "N applied migrations produce", agreed in number.
func AppliedMigrations(check migrator.DriftCheck) string {
	versions, repeatables := len(check.Replayed), len(check.ReplayedRepeatables)
	if repeatables == 0 {
		return fmt.Sprintf("%d applied %s", versions, plural(versions, "migration produces", "migrations produce"))
	}
	return fmt.Sprintf("%d applied %s and %d repeatable %s produce",
		versions, plural(versions, "migration", "migrations"),
		repeatables, plural(repeatables, "migration", "migrations"))
}

func plural(count int, one, many string) string {
	if count == 1 {
		return one
	}
	return many
}
