package dbcli

import (
	"fmt"
	"os"

	"github.com/go-extras/cobraflags"

	"github.com/stokaro/ptah/migration/onlineddl"
)

// ConfigFlagName is the CLI flag name exposed by [NewConfigFlag].
const ConfigFlagName = "config"

// NewConfigFlag returns the flag definition for the ptah.yaml config path
// used by the migration commands. Its online_ddl section routes ALTER TABLE
// statements on large MySQL/MariaDB tables through gh-ost or
// pt-online-schema-change (issue #173).
func NewConfigFlag() cobraflags.Flag {
	return &cobraflags.StringFlag{
		Name:  ConfigFlagName,
		Value: "",
		Usage: "Path to a ptah.yaml config file (default: ./" + onlineddl.ConfigFileName + " when present)",
	}
}

// LoadOnlineDDLConfig loads the online_ddl section for a migration command.
// An explicitly passed path must exist; the conventional ./ptah.yaml is
// optional and yields a disabled config when absent.
func LoadOnlineDDLConfig(path string) (*onlineddl.Config, error) {
	if path == "" {
		return onlineddl.LoadConfig(onlineddl.ConfigFileName)
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("ptah config %s: %w", path, err)
	}
	return onlineddl.LoadConfig(path)
}
