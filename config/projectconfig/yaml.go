package projectconfig

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"go.yaml.in/yaml/v3"
)

type yamlConfig struct {
	URL       string            `yaml:"url"`
	Dev       string            `yaml:"dev"`
	Exclude   []string          `yaml:"exclude"`
	Migration yamlMigration     `yaml:"migration"`
	Lint      yamlLint          `yaml:"lint"`
	Migrate   yamlMigrateConfig `yaml:"migrate"`
	OnlineDDL any               `yaml:"online_ddl"`
}

type yamlMigration struct {
	Dir             string `yaml:"dir"`
	Format          string `yaml:"format"`
	RevisionsSchema string `yaml:"revisions_schema"`
	RevisionFormat  string `yaml:"revision_format"`
	LockTimeout     string `yaml:"lock_timeout"`
	ExecOrder       string `yaml:"exec_order"`
}

type yamlLint struct {
	Latest *int `yaml:"latest"`
}

type yamlMigrateConfig struct {
	Generate yamlMigrateGenerateConfig `yaml:"generate"`
}

type yamlMigrateGenerateConfig struct {
	ShadowDatabaseURL string `yaml:"shadow_db"`
}

// LoadPtahFile loads Ptah's project config file. A missing file returns an
// empty config.
func LoadPtahFile(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return Config{}, nil
	case err != nil:
		return Config{}, fmt.Errorf("failed to read ptah config %s: %w", path, err)
	}

	var cfg yamlConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("failed to parse ptah config %s: %w", path, err)
	}
	return cfg.projectConfig(), nil
}

func (c yamlConfig) projectConfig() Config {
	dev := c.Dev
	if dev == "" {
		dev = c.Migrate.Generate.ShadowDatabaseURL
	}
	return Config{
		DatabaseURL: c.URL,
		DevURL:      dev,
		Exclude:     c.Exclude,
		Migration: MigrationConfig{
			Dir:             c.Migration.Dir,
			Format:          c.Migration.Format,
			RevisionsSchema: c.Migration.RevisionsSchema,
			RevisionFormat:  c.Migration.RevisionFormat,
			LockTimeout:     c.Migration.LockTimeout,
			ExecOrder:       c.Migration.ExecOrder,
		},
		Lint: LintConfig{
			Latest: c.Lint.Latest,
		},
	}
}
