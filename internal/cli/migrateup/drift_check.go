package migrateup

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/cliobs"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/engineruntime"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/driftgate"
	"ptah.run/internal/revisiontable"
	"ptah.run/migration/migrator"
)

const (
	checkDriftFlag   = "check-drift"
	shadowDBFlag     = "shadow-db"
	driftExcludeFlag = "drift-exclude"
)

// resolveDriftRequest decides whether the run checks drift and what a check
// that does not come back clean does. --check-drift, given either way, wins:
// `--check-drift` refuses on drift, and `--check-drift=false` turns a declared
// check off. Without the flag, the drift check the selected atlas.hcl env
// declares runs, and its `on_error` decides.
func resolveDriftRequest(cmd *cobra.Command, opts options, projectCfg projectconfig.Config) options {
	opts.driftDeclared = projectCfg.PreApplyDrift
	opts.driftOnDrift = driftgate.Refuse
	if cmd.Flags().Changed(checkDriftFlag) || opts.driftDeclared == nil {
		return opts
	}
	opts.checkDrift = true
	if opts.driftDeclared.OnError == projectconfig.DriftOnErrorContinue {
		opts.driftOnDrift = driftgate.Continue
	}
	return opts
}

// prepareDriftCheck is the check --check-drift or the atlas.hcl drift block
// asks for, with its shadow database provisioned when the URL is a docker://
// one, or nil when neither asked for it. The release function is always safe
// to call.
//
// The check leaves out the project's exclude list together with
// --drift-exclude, as `schema drift` leaves out the list with its --exclude:
// an object the project leaves to another tool is not drift. A declared
// drift block that sets `exclude` replaces the env's list with its own for the
// check. A selector that matched nothing on either side is a warning on
// standard error naming where it was written, as `schema drift` gives one, and
// the check goes on.
func prepareDriftCheck(
	cmd *cobra.Command,
	opts options,
	projectCfg projectconfig.Config,
	dbURL string,
	settings parsedMigrationSettings,
	conn *dbschema.DatabaseConnection,
	mig *migrator.Migrator,
) (*driftgate.Check, func(), error) {
	if !opts.checkDrift {
		return nil, func() {}, nil
	}
	ctx := cmd.Context()
	runtime, err := engineruntime.Resolve(ctx, engineruntime.DatabaseURL(dbURL))
	if err != nil {
		return nil, func() {}, err
	}
	exclude, sources, err := dbcli.EffectiveDriftExclude(cmd, driftExcludeFlag, opts.driftExclude, projectCfg, runtime.Selections())
	if err != nil {
		return nil, func() {}, err
	}
	selection := atlasschema.LiveSelection{Exclude: exclude, ExcludeSources: sources, Diagnostics: cmd.ErrOrStderr()}
	// Before the migration lock, so the lock is not held while a container
	// starts.
	shadowURL, release, err := dbcli.ResolveShadowDB(ctx, opts.shadowDB)
	if err != nil {
		return nil, func() {}, err
	}
	return driftgate.New(driftgate.Options{
		ShadowURL:      shadowURL,
		Selection:      selection,
		ConnectTimeout: settings.connectTimeout,
		Runtime:        runtime,
		Conn:           conn,
		History:        mig,
		MetadataTables: revisiontable.Configured(string(settings.revisionFormat), opts.migrationsTable),
		OnDrift:        opts.driftOnDrift,
		Trust:          dbcli.ShadowReplayTrust(cmd.Context()),
	}), release, nil
}

// validateDriftFlags refuses a drift setting that would be silently ignored:
// a shadow database, an exclusion or trust in the replayed SQL typed without
// --check-drift, and the check without a shadow database to replay into. shadowURL is the resolved
// value, which a project's dev_url may supply. declared is the drift check the
// atlas.hcl env declares, named in the refusal when it is what asked for the
// check.
func validateDriftFlags(
	checkDrift, shadowTyped, excludeTyped, trustTyped bool,
	shadowURL string,
	declared *projectconfig.PreApplyDrift,
) error {
	switch {
	case checkDrift && strings.TrimSpace(shadowURL) == "" && declared != nil:
		return fmt.Errorf("%s needs the env's dev database, or --%s: the applied history is replayed on it",
			declared.Origin(), shadowDBFlag)
	case checkDrift && strings.TrimSpace(shadowURL) == "":
		return fmt.Errorf("--%s requires --%s: the applied history is replayed on a disposable shadow database",
			checkDriftFlag, shadowDBFlag)
	case !checkDrift && shadowTyped:
		return fmt.Errorf("--%s is used only by --%s", shadowDBFlag, checkDriftFlag)
	case !checkDrift && excludeTyped:
		return fmt.Errorf("--%s is used only by --%s", driftExcludeFlag, checkDriftFlag)
	case !checkDrift && trustTyped:
		return fmt.Errorf("--%s is used only by --%s: no other step of the run replays SQL",
			dbcli.TrustReplaySQLFlagName, checkDriftFlag)
	}
	return nil
}

// planGuards are the checks a selection must pass before anything runs: the
// approved sequence first, because it is the cheaper one and a selection
// nobody approved is refused whatever the schema holds, then the drift check.
type planGuards struct {
	expected *expectedSequence
	drift    *driftgate.Check
}

func (g planGuards) guard() migrator.MigrationPlanGuard {
	expected, drift := g.expected.guard(), g.drift.Guard()
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

// reportDriftCheck tells a person what the check found. A clean check is a
// line of the run's output; a drifted one is the diagnostic, on standard
// error, that the run's exit status stands for, or a warning when the check
// is set to continue. An unavailable check that refused is reported by the
// error the run returns.
func reportDriftCheck(cmd *cobra.Command, emit cliobs.Emitter, drift *driftgate.Check) {
	check := drift.Found()
	switch {
	case check == nil:
	case check.Outcome == migrator.DriftClean:
		emit.Printf("Drift check: the database matches the schema its %s\n", driftgate.AppliedMigrations(*check))
	case check.Outcome == migrator.DriftDrifted:
		driftgate.WriteReport(cmd.ErrOrStderr(), *check, drift.OnDrift())
	case drift.OnDrift() == driftgate.Continue:
		driftgate.WriteUnavailable(cmd.ErrOrStderr(), *check)
	}
}

// driftExitStatus gives a drifted check exit status 1, the expected negative
// result, and leaves every other error to the root, which prints it and exits
// 2. reportDriftCheck has printed the drift.
func driftExitStatus(err error) error {
	if _, drifted := errors.AsType[*driftgate.DriftError](err); drifted {
		return exitcode.New(1, err)
	}
	return err
}
