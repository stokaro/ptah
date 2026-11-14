package dbcli

import (
	"fmt"
	"slices"

	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/core/selectext"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/cli/internal/cmdflags"
)

// EffectiveExclude returns the exclude selectors a native comparison applies,
// and where the run read each one: the project config's exclude list,
// followed by flagValues, the values the command's exclude flag flagName
// carries (typed, or set by its PTAH_* variable). A selector both name is kept
// once, and its source is the project file. A command with no exclude flag of
// its own passes an empty flagName and no values.
//
// The flag adds to the project list and never replaces it, unlike the values
// [EffectiveString] resolves. A project list names objects something else
// owns, and a flag that replaced it would turn a one-off `--exclude` into a
// plan that creates or drops every object the file protects. The pinned
// Atlas community binary v1.3.0 does replace `env.exclude` with `--exclude`,
// and the compatibility surface keeps that rule. A run that must compare an
// object the project excludes selects an env whose `exclude` is empty, or
// another config file.
//
// The sources name a project selector by the file that lists it, as the run
// spelled it, and a flag value by the PTAH_* variable that set it or by the
// flag with its dashes, so a diagnostic about a selector that matched nothing
// points where the operator wrote it.
//
// The project list is validated here, under selections, so a malformed
// selector in it is refused naming the file that holds it rather than a flag
// the command line never carried. flagValues are validated where the command
// validates its flag. A project list that names no selector returns
// flagValues unchanged.
func EffectiveExclude(
	cmd *cobra.Command,
	flagName string,
	flagValues []string,
	config projectconfig.Config,
	selections selectext.Set,
) ([]string, atlasfilter.ExcludeSources, error) {
	sources := atlasfilter.ExcludeSources{}
	if !slices.ContainsFunc(config.Exclude, atlasfilter.NamesSelector) {
		sources.Add(flagSource(cmd, flagName), flagValues...)
		return flagValues, sources, nil
	}
	if err := atlasfilter.ValidateExcludeSelectors(config.Exclude, selections); err != nil {
		return nil, nil, fmt.Errorf("invalid exclude in %s: %w", excludeSource(config), err)
	}
	sources.Add(excludeSource(config), config.Exclude...)
	sources.Add(flagSource(cmd, flagName), flagValues...)
	combined := make([]string, 0, len(config.Exclude)+len(flagValues))
	for _, value := range slices.Concat(config.Exclude, flagValues) {
		if !slices.Contains(combined, value) {
			combined = append(combined, value)
		}
	}
	return combined, sources, nil
}

// flagSource names where the values of the flag called name came from: the
// PTAH_* variable that set it, or the flag itself.
func flagSource(cmd *cobra.Command, name string) string {
	if cmd != nil {
		if variable, set := cmdflags.AppliedEnvName(cmd.Flags(), name); set {
			return variable
		}
	}
	return "--" + name
}

// excludeSource names the file a project exclude list came from, for a
// diagnostic about it.
func excludeSource(config projectconfig.Config) string {
	if config.ExcludeOrigin != "" {
		return config.ExcludeOrigin
	}
	return "the project config"
}
