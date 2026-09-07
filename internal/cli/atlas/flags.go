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

func registerAtlasSchemaFlag(flags *pflag.FlagSet, target *[]string, usage string) {
	flags.StringArrayVarP(target, atlasSchemaFlagName, atlasSchemaFlagShorthand, nil, usage)
}
