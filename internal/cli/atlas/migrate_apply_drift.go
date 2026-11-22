package atlas

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/driftgate"
	"ptah.run/internal/revisiontable"
	"ptah.run/migration/migrator"
)

// The pre-apply drift check of `migrate apply` is the atlas.hcl block
//
//	check "migrate_apply" {
//	  drift {
//	    on_error = FAIL
//	  }
//	}
//
// run through the drift check `ptah migrations up --check-drift` runs. The
// pinned community binary has no drift check: it ignores the block and applies
// on top of a drifted database, and it refuses `on_error = FAIL` as an unknown
// variable. Strict compatibility refuses the block before connecting; see
// [atlascompatpolicy.Policy.ValidateProjectConfig].
//
// The block's contract is Atlas's documented one, adapted here and nowhere
// else. The applied history is replayed on the env's `dev` database, in the
// --tx-mode the apply runs its pending files in, as `ptah migrations up
// --check-drift` replays it. `exclude` replaces the env's `exclude` for the
// check, and the revision table is left out either way. A target with no
// applied migration is not checked. `on_error = CONTINUE` reports what the
// check found and applies anyway.

// atlasApplyDriftDevURL is the dev database the declared drift check replays
// the history on, or a refusal when the env declares a check and no dev
// database. It reads the project only, so the refusal comes before the run
// connects.
func atlasApplyDriftDevURL(config projectconfig.Config) (string, error) {
	drift := config.PreApplyDrift
	if drift == nil {
		return "", nil
	}
	devURL := strings.TrimSpace(config.DevURL)
	if devURL == "" {
		return "", fmt.Errorf("%s needs the env's dev database: the applied history is replayed on it", drift.Origin())
	}
	return devURL, nil
}

// prepareAtlasApplyDrift is the drift check the env declares, for the target
// conn and the plan that applies to it in txMode, or nil when the env declares
// none. The dev database is provisioned here when its URL is a docker:// one,
// before the migration lock is taken. The release function is always safe to
// call.
func prepareAtlasApplyDrift(
	cmd *cobra.Command,
	config projectconfig.Config,
	targetURL, devURL string,
	conn *dbschema.DatabaseConnection,
	plan atlasmigrate.ApplyPlan,
	txMode migrator.MigrationTxMode,
) (*driftgate.Check, func(), error) {
	drift := config.PreApplyDrift
	if drift == nil {
		return nil, func() {}, nil
	}
	ctx := cmd.Context()
	runtime, err := atlasRuntime(ctx, []string{targetURL}, devURL)
	if err != nil {
		return nil, func() {}, err
	}
	// `migrate apply` has no --exclude, so the project's lists are the only
	// selectors.
	exclude, sources, err := dbcli.EffectiveDriftExclude(cmd, "exclude", nil, config, runtime.Selections())
	if err != nil {
		return nil, func() {}, err
	}
	shadowURL, release, err := devdocker.Resolve(ctx, devURL, devdocker.Options{})
	if err != nil {
		return nil, func() {}, err
	}
	onDrift := driftgate.Refuse
	if drift.OnError == projectconfig.DriftOnErrorContinue {
		onDrift = driftgate.Continue
	}
	_, revisions := plan.RevisionTable()
	return driftgate.New(driftgate.Options{
		ShadowURL: shadowURL,
		Selection: atlasschema.LiveSelection{
			Exclude: exclude, ExcludeSources: sources, Diagnostics: cmd.ErrOrStderr(),
		},
		Runtime:            runtime,
		Conn:               conn,
		History:            plan,
		MetadataTables:     revisiontable.Configured("atlas", revisions),
		TxMode:             txMode,
		OnDrift:            onDrift,
		SkipWithoutHistory: true,
	}), release, nil
}

// reportAtlasApplyDrift tells a person what the declared drift check did, on
// out, which is standard error: the apply's own report on standard output
// stays the one the community binary writes.
func reportAtlasApplyDrift(out io.Writer, drift *driftgate.Check) {
	if drift.Skipped() {
		_, _ = fmt.Fprintln(out, "Drift check: skipped, because no migration has been applied to compare the database with")
		return
	}
	check := drift.Found()
	switch {
	case check == nil:
	case check.Outcome == migrator.DriftClean:
		_, _ = fmt.Fprintf(out, "Drift check: the database matches the schema its %s\n", driftgate.AppliedMigrations(*check))
	case check.Outcome == migrator.DriftDrifted:
		driftgate.WriteReport(out, *check, drift.OnDrift())
	case drift.OnDrift() == driftgate.Continue:
		driftgate.WriteUnavailable(out, *check)
	}
}

// atlasApplyDriftRefusal is the drift check's refusal err carries, or nil. It
// is returned as it is, without the apply's wrapping: no file ran, and the
// check is what refused.
func atlasApplyDriftRefusal(err error) error {
	if drifted, ok := errors.AsType[*driftgate.DriftError](err); ok {
		return drifted
	}
	if unavailable, ok := errors.AsType[*driftgate.UnavailableError](err); ok {
		return unavailable
	}
	return nil
}
