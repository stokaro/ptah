package atlas

import "github.com/spf13/pflag"

const (
	atlasFileFlagName        = "file"
	atlasFileFlagShorthand   = "f"
	atlasFromFlagName        = "from"
	atlasFromFlagShorthand   = "f"
	atlasSchemaFlagName      = "schema"
	atlasSchemaFlagShorthand = "s"
)

// registerAtlasSchemaFlag declares -s/--schema as a string slice, the type the
// pinned community binary declares: each value is one line of comma-separated
// fields, so `"a,b"` names one schema, `"one"` names one, and `a"b` is refused
// while the flags are parsed. The list a project file sets goes through the same
// grammar in [effectiveAtlasSchemas], and the packages below this one split a
// name no further.
func registerAtlasSchemaFlag(flags *pflag.FlagSet, target *[]string, usage string) {
	flags.StringSliceVarP(target, atlasSchemaFlagName, atlasSchemaFlagShorthand, nil, usage)
}
