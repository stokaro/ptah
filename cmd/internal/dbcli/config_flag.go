package dbcli

import (
	"github.com/spf13/pflag"

	"github.com/stokaro/ptah/config/projectconfig"
)

// ConfigFlagName is the shared Ptah config path flag name.
const ConfigFlagName = "config"

// RegisterConfigFlag registers the flag for the ptah.yaml config path
// used by the migration commands. Its online_ddl section routes ALTER TABLE
// statements on large MySQL/MariaDB tables through gh-ost or
// pt-online-schema-change (issue #173).
func RegisterConfigFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, ConfigFlagName, "", "Path to a ptah.yaml config file (default: ./"+projectconfig.PtahFileName+" when present)")
}
