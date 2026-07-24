package projectconfig

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"

	"go.yaml.in/yaml/v3"

	"github.com/stokaro/ptah/migration/diffpolicy"
)

type yamlDocument struct {
	yamlSettings `yaml:",inline"`
	Env          map[string]yamlSettings `yaml:"env"`
}

type yamlSettings struct {
	URL       string            `yaml:"url"`
	Dev       string            `yaml:"dev"`
	Schemas   *[]string         `yaml:"schemas"`
	Exclude   *[]string         `yaml:"exclude"`
	Migration yamlMigration     `yaml:"migration"`
	Lint      yamlLint          `yaml:"lint"`
	Migrate   yamlMigrateConfig `yaml:"migrate"`
	OnlineDDL yamlOnlineDDL     `yaml:"online_ddl"`
	Diff      yamlDiff          `yaml:"diff"`
}

// yamlDiff is the ptah.yaml diff policy block. skip lists destructive change
// kinds to omit from generated migrations; concurrent_index requests
// CREATE INDEX CONCURRENTLY for newly added indexes. concurrent_index is a
// pointer so an explicit false is distinguishable from an unset value.
type yamlDiff struct {
	Skip            []string `yaml:"skip"`
	ConcurrentIndex *bool    `yaml:"concurrent_index"`
}

type yamlMigration struct {
	Dir                  string `yaml:"dir"`
	Format               string `yaml:"format"`
	RevisionsSchema      string `yaml:"revisions_schema"`
	RevisionsTable       string `yaml:"revisions_table"`
	RevisionFormat       string `yaml:"revision_format"`
	LockTimeout          string `yaml:"lock_timeout"`
	StatementTimeout     string `yaml:"statement_timeout"`
	ConnectTimeout       string `yaml:"connect_timeout"`
	MigrationLockTimeout string `yaml:"migration_lock_timeout"`
	ExecOrder            string `yaml:"exec_order"`
	TxMode               string `yaml:"tx_mode"`
	PreUpHook            string `yaml:"pre_up_hook"`
	PreDownHook          string `yaml:"pre_down_hook"`
	PostgresDumpTo       string `yaml:"pg_dump_to"`
	MySQLDumpTo          string `yaml:"mysqldump_to"`
	Webhook              string `yaml:"webhook"`
}

type yamlLint struct {
	Dialect       string    `yaml:"dialect"`
	DisabledRules *[]string `yaml:"disabled-rules"`
	Latest        *int      `yaml:"latest"`
}

type yamlOnlineDDL struct {
	Tool          string   `yaml:"tool"`
	ThresholdRows int64    `yaml:"threshold_rows"`
	Args          []string `yaml:"args"`
	Fallback      string   `yaml:"fallback"`
}

type yamlMigrateConfig struct {
	Generate yamlMigrateGenerateConfig `yaml:"generate"`
}

type yamlMigrateGenerateConfig struct {
	ShadowDatabaseURL string `yaml:"shadow_db"`
}

// LoadPtahFile loads Ptah's project config file. A missing file returns an
// empty config.
func LoadPtahFile(path, envName string) (Config, error) {
	raw, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return Config{}, nil
	case err != nil:
		return Config{}, fmt.Errorf("failed to read ptah config %s: %w", path, err)
	}

	return ParsePtah(raw, path, envName)
}

// ParsePtah parses Ptah's strict YAML project config.
func ParsePtah(data []byte, filename, envName string) (Config, error) {
	if filename == "" {
		filename = PtahFileName
	}
	var doc yamlDocument
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil {
		return Config{}, fmt.Errorf("failed to parse ptah config %s: %w", filename, err)
	}
	return selectPtahEnv(doc, envName)
}

func selectPtahEnv(doc yamlDocument, envName string) (Config, error) {
	base, err := doc.yamlSettings.projectConfig()
	if err != nil {
		return Config{}, err
	}
	if len(doc.Env) == 0 {
		return base, nil
	}
	if envName != "" {
		env, ok := doc.Env[envName]
		if !ok {
			return Config{}, fmt.Errorf("ptah env %q not found", envName)
		}
		selected, err := env.projectConfig()
		if err != nil {
			return Config{}, err
		}
		selected.EnvName = envName
		return Merge(base, selected), nil
	}
	if len(doc.Env) > 1 {
		return Config{}, fmt.Errorf("ptah.yaml contains multiple env blocks; pass --env")
	}
	for name, env := range doc.Env {
		selected, err := env.projectConfig()
		if err != nil {
			return Config{}, err
		}
		selected.EnvName = name
		return Merge(base, selected), nil
	}
	return base, nil
}

func (c yamlSettings) projectConfig() (Config, error) {
	dev := c.Dev
	if dev == "" {
		dev = c.Migrate.Generate.ShadowDatabaseURL
	}
	cfg := Config{
		DatabaseURL: c.URL,
		DevURL:      dev,
		Migration: MigrationConfig{
			Dir:                  c.Migration.Dir,
			Format:               c.Migration.Format,
			RevisionsSchema:      c.Migration.RevisionsSchema,
			RevisionsTable:       c.Migration.RevisionsTable,
			RevisionFormat:       c.Migration.RevisionFormat,
			LockTimeout:          c.Migration.LockTimeout,
			StatementTimeout:     c.Migration.StatementTimeout,
			ConnectTimeout:       c.Migration.ConnectTimeout,
			MigrationLockTimeout: c.Migration.MigrationLockTimeout,
			ExecOrder:            c.Migration.ExecOrder,
			TxMode:               c.Migration.TxMode,
			PreUpHook:            c.Migration.PreUpHook,
			PreDownHook:          c.Migration.PreDownHook,
			PostgresDumpTo:       c.Migration.PostgresDumpTo,
			MySQLDumpTo:          c.Migration.MySQLDumpTo,
			Webhook:              c.Migration.Webhook,
		},
		Lint: LintConfig{
			Dialect: c.Lint.Dialect,
			Latest:  c.Lint.Latest,
		},
	}
	if c.Schemas != nil {
		cfg.Schemas = slices.Clone(*c.Schemas)
		cfg.presence.schemas = true
	}
	if c.Exclude != nil {
		cfg.Exclude = slices.Clone(*c.Exclude)
		cfg.presence.exclude = true
	}
	if c.Lint.DisabledRules != nil {
		cfg.Lint.DisabledRules = slices.Clone(*c.Lint.DisabledRules)
		cfg.presence.lintDisabledRules = true
	}
	diff, err := c.Diff.diffConfig()
	if err != nil {
		return Config{}, err
	}
	cfg.Diff = diff
	return cfg, nil
}

// diffConfig maps the ptah.yaml diff block onto the DiffConfig IR, validating
// skip kinds against the shared diffpolicy vocabulary.
func (d yamlDiff) diffConfig() (DiffConfig, error) {
	var cfg DiffConfig
	for _, raw := range d.Skip {
		kind, err := diffpolicy.ParseChangeKind(raw)
		if err != nil {
			return DiffConfig{}, fmt.Errorf("ptah.yaml diff.skip: %w", err)
		}
		setDiffSkipKind(&cfg.Skip, kind)
	}
	if d.ConcurrentIndex != nil {
		cfg.ConcurrentIndex.Create = ConfigBool{Value: *d.ConcurrentIndex, Set: true}
	}
	return cfg, nil
}

func setDiffSkipKind(skip *DiffSkipConfig, kind diffpolicy.ChangeKind) {
	enabled := ConfigBool{Value: true, Set: true}
	switch kind {
	case diffpolicy.DropTable:
		skip.DropTable = enabled
	case diffpolicy.DropColumn:
		skip.DropColumn = enabled
	case diffpolicy.DropIndex:
		skip.DropIndex = enabled
	case diffpolicy.DropEnum:
		skip.DropEnum = enabled
	}
}
