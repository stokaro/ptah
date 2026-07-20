package dbcli

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/go-extras/cobraflags"
	"go.yaml.in/yaml/v3"

	"github.com/stokaro/ptah/internal/onlineddl"
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
	return LoadOnlineDDLConfigForEnv(path, "")
}

// LoadOnlineDDLConfigForEnv loads the online_ddl section after applying a
// selected project environment.
func LoadOnlineDDLConfigForEnv(path, envName string) (*onlineddl.Config, error) {
	if path == "" {
		return onlineddl.LoadConfigForEnv(onlineddl.ConfigFileName, envName)
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("ptah config %s: %w", path, err)
	}
	return onlineddl.LoadConfigForEnv(path, envName)
}

// MigrateGenerateConfig is the migrate.generate section of ptah.yaml.
type MigrateGenerateConfig struct {
	// ShadowDatabaseURL is the disposable shadow database used by migrate
	// generate to verify candidate migrations before writing files.
	ShadowDatabaseURL string `yaml:"shadow_db"`
}

type migrateGeneratePtahConfig struct {
	Migrate struct {
		Generate MigrateGenerateConfig `yaml:"generate"`
	} `yaml:"migrate"`
}

// LoadMigrateGenerateConfig loads the migrate.generate section for the
// migration-generation command. An explicitly passed path must exist; the
// conventional ./ptah.yaml is optional and yields an empty config when absent.
func LoadMigrateGenerateConfig(path string) (*MigrateGenerateConfig, error) {
	if path == "" {
		path = onlineddl.ConfigFileName
	} else if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("ptah config %s: %w", path, err)
	}

	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return &MigrateGenerateConfig{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read ptah config %s: %w", path, err)
	}

	var cfg migrateGeneratePtahConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse ptah config %s: %w", path, err)
	}
	return &cfg.Migrate.Generate, nil
}
