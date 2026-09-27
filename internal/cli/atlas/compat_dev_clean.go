package atlas

import (
	"context"
	"errors"

	"github.com/spf13/cobra"

	"ptah.run/internal/atlassource"
	"ptah.run/internal/cli/internal/devsnapshot"
	"ptah.run/internal/migrateclean"
)

// The refusals below reproduce the pinned binary's check of a dev database for
// each verb that decides from its sources whether it uses one; see
// [devsnapshot] for the measurement.

// atlasInspectDevRefusal words an inspection's refusal of a dev database the
// way the binary does. `schema inspect` resets the dev database for every
// source that uses it, so the reset's own refusal is always what stops the
// run; only the words are this surface's. Any other error is returned as is.
func atlasInspectDevRefusal(
	opts atlasSchemaInspectOptions,
	projectEnv atlassource.ProjectEnv,
	err error,
) error {
	notClean, ok := errors.AsType[*migrateclean.NotCleanError](err)
	if !ok {
		return err
	}
	set, classifyErr := atlassource.ClassifySet("--url", []string{opts.url}, projectEnv)
	if classifyErr != nil {
		return err
	}
	check, _ := devsnapshot.ForSources(set)
	return check.Wrap(notClean)
}

// atlasDiffDevCheck is the refusal for `schema diff`, which uses the dev
// database when either side is not a database. The diff runs it after every
// local source has passed validation, so a refused source is reported before
// the dev database is opened.
func atlasDiffDevCheck(devURL string) func(context.Context, atlassource.Set, atlassource.Set) error {
	return func(ctx context.Context, from, to atlassource.Set) error {
		check, uses := devsnapshot.ForSources(from, to)
		if !uses {
			return nil
		}
		return devsnapshot.Refuse(ctx, devURL, check)
	}
}

// refuseUncleanAtlasApplyDev is the refusal for `schema apply`, which uses the
// dev database when --to is not a database, whether or not there is a change
// to rehearse.
func refuseUncleanAtlasApplyDev(
	cmd *cobra.Command,
	opts atlasSchemaApplyOptions,
	projectEnv atlassource.ProjectEnv,
) error {
	// validateAtlasSchemaApplyOptions classified --to and refused an error
	// already.
	set, err := atlassource.ClassifySet("--to", opts.toURLs, projectEnv)
	if err != nil {
		return nil //nolint:nilerr // the verb reported the classification error already
	}
	check, uses := devsnapshot.ForSources(set)
	if !uses {
		return nil
	}
	return devsnapshot.Refuse(cmd.Context(), opts.devURL, check)
}
