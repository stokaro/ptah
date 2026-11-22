package atlasmigrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlascheckform"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/devclean"
	"ptah.run/migration/migrationfile"
)

// replayedDiffPlanner plans the files of a `migrate diff` against the
// migration directory replayed on the dev database.
type replayedDiffPlanner struct {
	conn      *dbschema.DatabaseConnection
	runtime   diffRuntime
	source    fs.FS
	opts      DiffOptions
	schemas   []string
	naming    atlasfilter.Naming
	desired   *schemamodel.Database
	selection comparisonSelection
	format    string
	// probeChecks is whether the desired state is a declaration whose CHECKs
	// the dev database may spell differently; see [atlascheckform].
	probeChecks bool
}

// replayedDiffPlan is what one replay planned: the files, or that the
// directory is synced, or a diff that writes a CHECK the dev database has to
// spell before the files can be planned.
type replayedDiffPlan struct {
	contents []MigrationFileContent
	synced   bool
	checks   *difftypes.SchemaDiff
}

// planWithDevCheckSpellings plans the files. Where they would write a CHECK
// the desired declaration holds and the dev server rewrites, it gives up the
// replay, asks the dev database for each CHECK's spelling, and replays the
// directory again to plan the files with it. The pinned community binary
// plans from the desired schema as the dev database holds it, so a CHECK it
// adds reads as that server prints it.
//
// Whether a CHECK is written is known only once the replay is compared, and
// the probe needs the dev database the replay holds, so a run that writes one
// replays twice and every other run replays once and probes nothing.
func (p replayedDiffPlanner) planWithDevCheckSpellings(ctx context.Context) (replayedDiffPlan, error) {
	planned, err := p.plan(ctx, atlascheckform.Spellings{})
	if err != nil || planned.checks == nil {
		return planned, err
	}
	spellings, err := atlasschema.DevCheckSpellings(ctx, p.opts.Runtime, p.conn, p.desired, planned.checks, p.opts.Diagnostics)
	if err != nil {
		return replayedDiffPlan{}, err
	}
	// The second replay plans with the spellings and asks for no probe.
	second := p
	second.probeChecks = false
	return second.plan(ctx, spellings)
}

// plan replays the directory and plans the files while the replayed state is
// still on the dev database: whether the migrator would run a file in a
// transaction depends on that catalog, and the replay is reset when the
// callback returns. Each CHECK the files write takes its form from spellings.
//
// With probeChecks set, a diff that writes a CHECK the dev server rewrites is
// returned instead of files. What that replay reports is then dropped, since
// the replay that plans the files reports it again.
func (p replayedDiffPlanner) plan(
	ctx context.Context,
	spellings atlascheckform.Spellings,
) (planned replayedDiffPlan, err error) {
	opts := p.opts
	if p.probeChecks {
		var held bytes.Buffer
		opts.Diagnostics = &held
		defer func() {
			if planned.checks == nil {
				err = errors.Join(err, reportHeld(p.opts.Diagnostics, &held))
			}
		}()
	}
	err = p.runtime.withReplayedSnapshot(
		ctx,
		p.conn,
		p.source,
		migrationfile.DirFormatAtlas,
		func(replayConn *dbschema.DatabaseConnection, baseline devclean.Baseline) error {
			// Placed against the connection the comparison runs on, so a name the
			// desired schema writes without a schema means the schema it is
			// compared in, and the file names it; see
			// [atlasschema.PlaceUnqualifiedNames].
			desired, err := atlasschema.PlaceUnqualifiedNames(replayConn.Info(), p.desired)
			if err != nil {
				return err
			}
			opts.Policy = opts.Policy.ForDialect(replayConn.Info().Dialect)
			compared, err := compareReplayedState(
				ctx, replayConn, p.runtime, p.schemas, p.naming, desired, baseline, p.selection,
				opts.Diagnostics, opts.ValidateLiveObject, opts.Policy, opts.Runtime,
			)
			if err != nil {
				return err
			}
			current, selected, diff := compared.current, compared.desired, compared.diff
			if err := opts.Policy.ReviewOtherDialects(opts.Diagnostics, diff, replayConn.Info().Dialect, replayConn.Info().Capabilities); err != nil {
				return err
			}
			// The file carries no comment for a change the policy left out:
			// the pinned community binary writes the same file without the
			// change and without a word about it, and the file is what
			// atlas.sum hashes. The notice goes beside the result instead.
			diff, omitted := atlasschema.ApplyDiffPolicy(diff, opts.Policy, replayConn.Info().Dialect)
			atlasschema.ReportOmitted(opts.Diagnostics, omitted)
			if !diff.HasChanges() {
				planned.synced = true
				return nil
			}
			if p.probeChecks && len(atlascheckform.Plan(p.conn.Info().Dialect, p.desired, diff)) > 0 {
				planned.checks = diff
				return nil
			}
			respelled := *diff
			spellings.Respell(&respelled)
			files, err := planDiffFileContents(ctx, &respelled, selected, current, p.selection.info, p.format, opts)
			if err != nil {
				return err
			}
			planned.contents, err = markFileModeRefusals(ctx, replayConn, opts, files)
			return err
		},
		diffReplayOptions(opts.DirFormat)...,
	)
	if err != nil {
		return replayedDiffPlan{}, err
	}
	return planned, nil
}

// reportHeld copies what a replay reported into diag, which may be nil.
func reportHeld(diag io.Writer, held *bytes.Buffer) error {
	if diag == nil || held.Len() == 0 {
		return nil
	}
	if _, err := held.WriteTo(diag); err != nil {
		return fmt.Errorf("write diagnostics: %w", err)
	}
	return nil
}
