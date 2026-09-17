package dbcli

import (
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"ptah.run/config"
	"ptah.run/config/projectconfig"
)

// IgnoreExtensionFlagName names an extension the comparison must plan no change
// for.
const IgnoreExtensionFlagName = "ignore-extension"

// RegisterIgnoreExtensionFlag registers the repeatable extension ignore list on
// a command that compares a desired schema with a live database.
//
// The list adds to Ptah's defaults rather than replacing them, which is what a
// caller reaching for it means: an extension a bootstrap step creates and the
// schema deliberately does not describe. Declaring such an extension instead
// makes Ptah manage it, and management includes removal -- and dropping one
// cascades into everything built on it (stokaro/ptah#3373).
func RegisterIgnoreExtensionFlag(flags *pflag.FlagSet, target *[]string) {
	flags.StringArrayVar(target, IgnoreExtensionFlagName, nil,
		"Database extension the comparison must leave alone, neither created nor dropped (repeatable; adds to the defaults)")
}

// CompareOptionsIgnoringExtensions builds comparison options whose ignored
// extensions are the defaults plus whatever this invocation asked for.
//
// The value comes from the flag when the flag was given, and otherwise from the
// project config, which is the precedence [EffectiveString] applies to every
// other setting a command reads from both.
//
// base carries the options a caller has already built -- the server-resolved
// expression maps, say -- and is not modified; nil asks for the defaults. The
// returned options are always non-nil, so a caller can hand them straight to a
// comparison rather than deciding what nil means there.
func CompareOptionsIgnoringExtensions(
	cmd *cobra.Command,
	flagValue []string,
	cfg projectconfig.Config,
	base *config.CompareOptions,
) *config.CompareOptions {
	opts := config.DefaultCompareOptions()
	if base != nil {
		clone := *base
		opts = &clone
		if len(base.IgnoredExtensions) == 0 {
			opts.IgnoredExtensions = config.DefaultCompareOptions().IgnoredExtensions
		}
	}
	opts.IgnoredExtensions = appendIgnoredExtensions(
		opts.IgnoredExtensions,
		effectiveIgnoredExtensions(cmd, flagValue, cfg.IgnoredExtensionsValue()),
	)
	return opts
}

// effectiveIgnoredExtensions resolves the requested list the way every other
// setting with both a flag and a config key is resolved: an explicit flag
// first, then a present config value, then nothing.
func effectiveIgnoredExtensions(
	cmd *cobra.Command,
	flagValue []string,
	configValue projectconfig.Value[[]string],
) []string {
	if flagChanged(cmd, IgnoreExtensionFlagName) || !configValue.Present {
		return flagValue
	}
	return configValue.Value
}

// appendIgnoredExtensions adds the requested names to the list already in
// effect, dropping blanks and names the list already carries.
//
// A duplicate is not an error -- naming plpgsql, which Ptah ignores anyway, is
// a reasonable thing for a project to write down -- and the match is the exact,
// case-sensitive one [config.CompareOptions.IsExtensionIgnored] performs, so
// the de-duplication cannot make a name stop matching.
func appendIgnoredExtensions(existing, requested []string) []string {
	result := slices.Clone(existing)
	for _, name := range requested {
		name = strings.TrimSpace(name)
		if name == "" || slices.Contains(result, name) {
			continue
		}
		result = append(result, name)
	}
	return result
}
