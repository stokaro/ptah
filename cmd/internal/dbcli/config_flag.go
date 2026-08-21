package dbcli

import (
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/config/projectconfig"
)

// ConfigFlagName is the shared Ptah config path flag name.
const ConfigFlagName = "config"

// RegisterConfigFlag registers the flag for the project config path used by
// the migration commands. A ptah.yaml supplies the online_ddl section that
// routes ALTER TABLE statements on large MySQL/MariaDB tables through gh-ost
// or pt-online-schema-change (issue #173); a path ending in .hcl is read as an
// Atlas project config instead, so one living anywhere but ./atlas.hcl is
// reachable from the native binary (stokaro/ptah#1215).
func RegisterConfigFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, ConfigFlagName, "",
		"Path to a "+projectconfig.PtahFileName+" config file, or an Atlas project config ending in "+
			AtlasProjectConfigExtension+" (default: ./"+projectconfig.PtahFileName+
			" and ./"+projectconfig.AtlasFileName+" when present)")
}
