package dbcli

import (
	"fmt"
	"slices"

	"ptah.run/config/projectconfig"
	"ptah.run/core/selectext"
	"ptah.run/internal/atlasfilter"
)

// EffectiveExclude returns the exclude selectors a native comparison applies:
// the project config's exclude list, followed by flagValues, the values the
// command's own exclude flag carries (typed, or set by its PTAH_* variable).
// A selector both name is kept once.
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
// The project list is validated here, under selections, so a malformed
// selector in it is refused naming the file that holds it rather than a flag
// the command line never carried. flagValues are validated where the command
// validates its flag. A project list that names no selector returns
// flagValues unchanged.
func EffectiveExclude(config projectconfig.Config, flagValues []string, selections selectext.Set) ([]string, error) {
	if !slices.ContainsFunc(config.Exclude, atlasfilter.NamesSelector) {
		return flagValues, nil
	}
	if err := atlasfilter.ValidateExcludeSelectors(config.Exclude, selections); err != nil {
		return nil, fmt.Errorf("invalid exclude in %s: %w", excludeSource(config), err)
	}
	combined := make([]string, 0, len(config.Exclude)+len(flagValues))
	for _, value := range slices.Concat(config.Exclude, flagValues) {
		if !slices.Contains(combined, value) {
			combined = append(combined, value)
		}
	}
	return combined, nil
}

// excludeSource names the file a project exclude list came from, for a
// diagnostic about it.
func excludeSource(config projectconfig.Config) string {
	if config.ExcludeOrigin != "" {
		return config.ExcludeOrigin
	}
	return "the project config"
}
