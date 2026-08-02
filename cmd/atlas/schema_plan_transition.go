package atlas

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlasschema"
)

// The Atlas command names these verbs answer to. They appear verbatim in
// diagnostics, so a sub-verb's error names the command the operator ran.
const (
	atlasSchemaPlanVerb         = "atlas schema plan"
	atlasSchemaPlanNewVerb      = "atlas schema plan new"
	atlasSchemaPlanValidateVerb = "atlas schema plan validate"
)

// atlasSchemaPlanTransitionFlags holds the schema-state transition every
// `schema plan` verb describes: the --from database the plan applies to, the
// --to desired state, and the dev database and object filters used to compare
// them.
//
// Atlas registers the same ten transition flags on `schema plan` and on each of
// its local sub-verbs, per the published Atlas CLI reference
// (https://atlasgo.io/cli-reference, entries "atlas schema plan",
// "atlas schema plan new" and "atlas schema plan validate", retrieved
// 2026-08-02). Registering and refusing them in one place is what stops a
// sub-verb from drifting away from the parent as flags land;
// TestAtlasSchemaPlanVerbFlagSetsMatchAtlas pins each verb's registered set
// against that published list.
type atlasSchemaPlanTransitionFlags struct {
	fromURLs []string
	toURLs   []string
	devURL   string
	exclude  []string
	schemas  []string
}

// registerAtlasSchemaPlanTransitionFlags registers the transition flag set
// shared by `schema plan` and its local sub-verbs. Every flag here is either
// consumed by the plan computation or declared for CLI-surface parity and
// refused by rejectUnimplementedAtlasSchemaPlanFlags.
func registerAtlasSchemaPlanTransitionFlags(flags *pflag.FlagSet, opts *atlasSchemaPlanTransitionFlags) {
	flags.StringArrayVar(&opts.fromURLs, atlasFromFlagName, nil,
		"Current schema state URL: the target database the plan applies to")
	flags.StringArrayVar(&opts.toURLs, "to", nil, "Desired schema state URL")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used by Atlas for planning")
	flags.StringArrayVar(&opts.exclude, "exclude", nil, "Schema objects to exclude from planning")
	registerAtlasSchemaFlag(flags, &opts.schemas, "Schemas to plan when database URLs are used")
	// Declared for CLI-surface parity and rejected loudly: see
	// rejectUnimplementedAtlasSchemaPlanFlags. --auto-approve is the one
	// exception — a locally saved plan file is approved by operator review, so
	// there is no prompt for it to skip and accepting it changes nothing.
	flags.Bool("auto-approve", false, "Approve the plan without asking for confirmation")
	flags.String("repo", "", "URL to the schema repository")
	flags.String("format", "", "Atlas Go template output format")
	flags.StringArray("include", nil, "Schema objects to include in planning")
	flags.String("lock-timeout", "", "Timeout for acquiring the database lock")
}

// resolveAtlasSchemaPlanTransitionConfig fills every transition value the
// operator did not pass explicitly from the selected atlas.hcl env, and
// returns the diff policy that env declares. verb names the invoked command
// for the one diagnostic that mentions it.
func resolveAtlasSchemaPlanTransitionConfig(
	cmd *cobra.Command,
	verb string,
	in atlasSchemaPlanTransitionFlags,
) (atlasSchemaPlanTransitionFlags, atlasschema.DiffPolicy, error) {
	policy := atlasschema.DiffPolicy{}
	projectCfg, loaded, err := loadOptionalAtlasProjectConfigForCommand(cmd)
	if needsAtlasSchemaPlanConfig(cmd) {
		projectCfg, loaded, err = loadRequiredAtlasProjectConfigForCommand(cmd)
	}
	if err != nil {
		return in, policy, err
	}
	if loaded {
		databaseURL := projectCfg.StringValue(projectconfig.StringDatabaseURL)
		if !cmd.Flags().Changed(atlasFromFlagName) && databaseURL.Present {
			in.fromURLs = []string{databaseURL.Value}
		}
		in.toURLs = effectiveStringArray(cmd, "to", in.toURLs, projectCfg.SchemaSources)
		in.devURL = dbcli.EffectiveString(
			cmd,
			"dev-url",
			in.devURL,
			projectCfg.StringValue(projectconfig.StringDevURL),
		)
		in.exclude = effectiveAtlasExclude(cmd, in.exclude, projectCfg)
		policy, err = atlasDiffPolicy(projectCfg)
		if err != nil {
			return in, policy, err
		}
	}
	if loaded && !cmd.Flags().Changed("to") && len(projectCfg.SchemaSources) > 0 {
		in.toURLs, err = atlasProjectConfigSchemaURLs(cmd, in.toURLs)
		if err != nil {
			return in, policy, fmt.Errorf("atlas.hcl schema.src: %w", err)
		}
	}
	// Schema plan resolves local schema files only (LocalFilesOnly), so an env
	// whose desired state is an external schema program cannot feed it yet.
	if loaded && !cmd.Flags().Changed("to") && atlasExternalSchemaConfigured(projectCfg) {
		return in, policy, fmt.Errorf(
			"%s does not support atlas.hcl data.external_schema desired state yet; pass --to explicitly", verb)
	}
	return in, policy, nil
}

// validateAtlasSchemaPlanTransition checks the transition every `schema plan`
// verb needs: exactly one --from database URL and at least one local --to
// desired-state source, plus the shared refusals.
func validateAtlasSchemaPlanTransition(
	cmd *cobra.Command,
	verb string,
	transition atlasSchemaPlanTransitionFlags,
) error {
	if err := rejectUnimplementedAtlasSchemaPlanFlags(cmd, verb, transition); err != nil {
		return err
	}
	if len(transition.fromURLs) == 0 {
		return fmt.Errorf("--from is required")
	}
	if len(transition.fromURLs) > 1 {
		return fmt.Errorf("%s accepts multiple --from URLs, but Ptah plans against one target database URL", verb)
	}
	if err := ensureAtlasPlanDatabaseURL(verb, transition.fromURLs[0]); err != nil {
		return err
	}
	if len(transition.toURLs) == 0 {
		return errAtlasSchemaPlanMissingTo
	}
	return ensureLocalSchemaURLs("--to", transition.toURLs)
}

// errAtlasSchemaPlanMissingTo reports an absent --to desired state. It is a
// sentinel rather than a fresh error so `schema plan validate` can substitute
// Atlas's own wording for exactly this case, and only this case: matching on
// "--to came back empty" instead would also swallow a missing --from, which is
// reported first and is the more useful diagnostic.
var errAtlasSchemaPlanMissingTo = errors.New("--to is required")
