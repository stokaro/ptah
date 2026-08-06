package projectconfig

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"

	"go.yaml.in/yaml/v3"

	"go.5x5.cz/ptah/migration/diffpolicy"
)

type yamlDocument struct {
	yamlSettings `yaml:",inline"`
	Env          map[string]yamlSettings `yaml:"env"`
}

type yamlSettings struct {
	URL            *string            `yaml:"url"`
	Dev            *string            `yaml:"dev"`
	Schemas        *[]string          `yaml:"schemas"`
	Exclude        *[]string          `yaml:"exclude"`
	Migration      yamlMigration      `yaml:"migration"`
	Lint           yamlLint           `yaml:"lint"`
	Migrate        yamlMigrateConfig  `yaml:"migrate"`
	OnlineDDL      yamlOnlineDDL      `yaml:"online_ddl"`
	Diff           yamlDiff           `yaml:"diff"`
	ExternalSchema yamlExternalSchema `yaml:"external_schema"`
}

// yamlExternalSchema is the ptah.yaml external_schema block: program is an
// explicit argv list (first element is the executable, run without a shell),
// format is the stdout format (default sql), working_dir is the program's
// working directory, and env holds extra "KEY=VALUE" entries. program and env
// are pointers so an explicit empty list is distinguishable from an unset value.
type yamlExternalSchema struct {
	Program    *[]string `yaml:"program"`
	Format     *string   `yaml:"format"`
	WorkingDir *string   `yaml:"working_dir"`
	Env        *[]string `yaml:"env"`
}

// yamlDiff is the ptah.yaml diff policy block. skip lists destructive change
// kinds to omit from generated migrations; concurrent_index requests
// CREATE INDEX CONCURRENTLY for newly added indexes and
// concurrent_index_drop requests DROP INDEX CONCURRENTLY for standalone index
// removals. Both are pointers so an explicit false is distinguishable from an
// unset value.
type yamlDiff struct {
	Skip                *[]string `yaml:"skip"`
	ConcurrentIndex     *bool     `yaml:"concurrent_index"`
	ConcurrentIndexDrop *bool     `yaml:"concurrent_index_drop"`
}

type yamlMigration struct {
	Dir                  *string `yaml:"dir"`
	Format               *string `yaml:"format"`
	RevisionsSchema      *string `yaml:"revisions_schema"`
	RevisionsTable       *string `yaml:"revisions_table"`
	RevisionFormat       *string `yaml:"revision_format"`
	LockTimeout          *string `yaml:"lock_timeout"`
	StatementTimeout     *string `yaml:"statement_timeout"`
	ConnectTimeout       *string `yaml:"connect_timeout"`
	MigrationLockTimeout *string `yaml:"migration_lock_timeout"`
	ExecOrder            *string `yaml:"exec_order"`
	TxMode               *string `yaml:"tx_mode"`
	PreUpHook            *string `yaml:"pre_up_hook"`
	PreDownHook          *string `yaml:"pre_down_hook"`
	PostgresDumpTo       *string `yaml:"pg_dump_to"`
	MySQLDumpTo          *string `yaml:"mysqldump_to"`
	Webhook              *string `yaml:"webhook"`
}

type yamlLint struct {
	Dialect       *string   `yaml:"dialect"`
	DisabledRules *[]string `yaml:"disabled-rules"`
	Latest        *int      `yaml:"latest"`
}

type yamlOnlineDDL struct {
	Tool          *string   `yaml:"tool"`
	ThresholdRows *int64    `yaml:"threshold_rows"`
	Args          *[]string `yaml:"args"`
	Fallback      *string   `yaml:"fallback"`
}

type yamlMigrateConfig struct {
	Generate yamlMigrateGenerateConfig `yaml:"generate"`
}

type yamlMigrateGenerateConfig struct {
	ShadowDatabaseURL *string `yaml:"shadow_db"`
}

// LoadPtahFile loads Ptah's project config file. A missing file returns an
// empty config.
func LoadPtahFile(path, envName string) (Config, error) {
	return loadPtahFile(path, envName, discoveredPtahConfig)
}

// loadPtahFile reads the config file at path. source carries how the path was
// chosen: an explicit --config path must exist, and is the only case in which a
// diagnostic may blame that flag; a discovered ./ptah.yaml is optional.
func loadPtahFile(path, envName string, source ptahConfigSource) (Config, error) {
	raw, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist) && source == discoveredPtahConfig:
		return Config{}, nil
	case err != nil:
		return Config{}, fmt.Errorf("failed to read ptah config %s: %w", path, err)
	}

	return parsePtah(raw, path, envName, source)
}

// ParsePtah parses Ptah's strict YAML project config.
func ParsePtah(data []byte, filename, envName string) (Config, error) {
	return parsePtah(data, filename, envName, discoveredPtahConfig)
}

func parsePtah(data []byte, filename, envName string, source ptahConfigSource) (Config, error) {
	if filename == "" {
		filename = PtahFileName
	}
	var doc yamlDocument
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil {
		return Config{}, translatePtahYAMLError(err, filename, source)
	}
	cfg, err := selectPtahEnv(doc, envName)
	if err != nil {
		return Config{}, err
	}
	if err := cfg.OnlineDDL.Validate(); err != nil {
		if cfg.EnvName != "" {
			return Config{}, fmt.Errorf(
				"invalid online_ddl config in %s for env %q: %w",
				filename,
				cfg.EnvName,
				err,
			)
		}
		return Config{}, fmt.Errorf("invalid online_ddl config in %s: %w", filename, err)
	}
	return cfg, nil
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
	cfg := Config{}
	applyYAMLString(c.URL, &cfg.DatabaseURL, fieldDatabaseURL, &cfg.presence)
	switch {
	case c.Dev != nil:
		applyYAMLString(c.Dev, &cfg.DevURL, fieldDevURL, &cfg.presence)
	case c.Migrate.Generate.ShadowDatabaseURL != nil:
		applyYAMLString(
			c.Migrate.Generate.ShadowDatabaseURL,
			&cfg.DevURL,
			fieldDevURL,
			&cfg.presence,
		)
	}
	applyYAMLString(c.Migration.Dir, &cfg.Migration.Dir, fieldMigrationDir, &cfg.presence)
	applyYAMLString(c.Migration.Format, &cfg.Migration.Format, fieldMigrationFormat, &cfg.presence)
	applyYAMLString(
		c.Migration.RevisionsSchema,
		&cfg.Migration.RevisionsSchema,
		fieldMigrationRevisionsSchema,
		&cfg.presence,
	)
	applyYAMLString(
		c.Migration.RevisionsTable,
		&cfg.Migration.RevisionsTable,
		fieldMigrationRevisionsTable,
		&cfg.presence,
	)
	applyYAMLString(
		c.Migration.RevisionFormat,
		&cfg.Migration.RevisionFormat,
		fieldMigrationRevisionFormat,
		&cfg.presence,
	)
	applyYAMLString(
		c.Migration.LockTimeout,
		&cfg.Migration.LockTimeout,
		fieldMigrationLockTimeout,
		&cfg.presence,
	)
	applyYAMLString(
		c.Migration.StatementTimeout,
		&cfg.Migration.StatementTimeout,
		fieldMigrationStatementTimeout,
		&cfg.presence,
	)
	applyYAMLString(
		c.Migration.ConnectTimeout,
		&cfg.Migration.ConnectTimeout,
		fieldMigrationConnectTimeout,
		&cfg.presence,
	)
	applyYAMLString(
		c.Migration.MigrationLockTimeout,
		&cfg.Migration.MigrationLockTimeout,
		fieldMigrationMigrationLock,
		&cfg.presence,
	)
	applyYAMLString(
		c.Migration.ExecOrder,
		&cfg.Migration.ExecOrder,
		fieldMigrationExecOrder,
		&cfg.presence,
	)
	applyYAMLString(c.Migration.TxMode, &cfg.Migration.TxMode, fieldMigrationTxMode, &cfg.presence)
	applyYAMLString(
		c.Migration.PreUpHook,
		&cfg.Migration.PreUpHook,
		fieldMigrationPreUpHook,
		&cfg.presence,
	)
	applyYAMLString(
		c.Migration.PreDownHook,
		&cfg.Migration.PreDownHook,
		fieldMigrationPreDownHook,
		&cfg.presence,
	)
	applyYAMLString(
		c.Migration.PostgresDumpTo,
		&cfg.Migration.PostgresDumpTo,
		fieldMigrationPostgresDumpTo,
		&cfg.presence,
	)
	applyYAMLString(
		c.Migration.MySQLDumpTo,
		&cfg.Migration.MySQLDumpTo,
		fieldMigrationMySQLDumpTo,
		&cfg.presence,
	)
	applyYAMLString(
		c.Migration.Webhook,
		&cfg.Migration.Webhook,
		fieldMigrationWebhook,
		&cfg.presence,
	)
	applyYAMLString(c.Lint.Dialect, &cfg.Lint.Dialect, fieldLintDialect, &cfg.presence)
	if c.Lint.Latest != nil {
		cfg.Lint.Latest = clonePointer(c.Lint.Latest)
		cfg.presence.mark(fieldLintLatest)
	}
	applyYAMLString(
		c.ExternalSchema.Format,
		&cfg.ExternalSchema.Format,
		fieldExternalSchemaFormat,
		&cfg.presence,
	)
	applyYAMLString(
		c.ExternalSchema.WorkingDir,
		&cfg.ExternalSchema.WorkingDir,
		fieldExternalSchemaWorkingDir,
		&cfg.presence,
	)
	c.OnlineDDL.applyTo(&cfg)
	if c.ExternalSchema.Program != nil {
		cfg.ExternalSchema.Program = slices.Clone(*c.ExternalSchema.Program)
		cfg.ExternalSchema.Origin = PtahFileName
		cfg.presence.mark(fieldExternalSchemaProgram)
	}
	if c.ExternalSchema.Env != nil {
		cfg.ExternalSchema.Env = slices.Clone(*c.ExternalSchema.Env)
		cfg.presence.mark(fieldExternalSchemaEnv)
	}
	if c.Schemas != nil {
		cfg.Schemas = slices.Clone(*c.Schemas)
		cfg.presence.mark(fieldSchemas)
	}
	if c.Exclude != nil {
		cfg.Exclude = slices.Clone(*c.Exclude)
		cfg.presence.mark(fieldExclude)
	}
	if c.Lint.DisabledRules != nil {
		cfg.Lint.DisabledRules = slices.Clone(*c.Lint.DisabledRules)
		cfg.presence.mark(fieldLintDisabledRules)
	}
	diff, err := c.Diff.diffConfig()
	if err != nil {
		return Config{}, err
	}
	cfg.Diff = diff
	return cfg, nil
}

func applyYAMLString(
	value *string,
	destination *string,
	field configField,
	presence *configPresence,
) {
	if value == nil {
		return
	}
	*destination = *value
	presence.mark(field)
}

func (c yamlOnlineDDL) applyTo(cfg *Config) {
	if c.Tool != nil {
		cfg.OnlineDDL.Tool = *c.Tool
		cfg.presence.mark(fieldOnlineDDLTool)
	}
	if c.ThresholdRows != nil {
		cfg.OnlineDDL.ThresholdRows = *c.ThresholdRows
		cfg.presence.mark(fieldOnlineDDLThresholdRows)
	}
	if c.Args != nil {
		cfg.OnlineDDL.Args = slices.Clone(*c.Args)
		cfg.presence.mark(fieldOnlineDDLArgs)
	}
	if c.Fallback != nil {
		cfg.OnlineDDL.Fallback = *c.Fallback
		cfg.presence.mark(fieldOnlineDDLFallback)
	}
}

// diffConfig maps the ptah.yaml diff block onto the DiffConfig IR, validating
// skip kinds against the shared diffpolicy vocabulary.
func (d yamlDiff) diffConfig() (DiffConfig, error) {
	var cfg DiffConfig
	if d.Skip != nil {
		cfg.Skip = DiffSkipConfig{
			DropTable:  ConfigBool{Set: true},
			DropColumn: ConfigBool{Set: true},
			DropIndex:  ConfigBool{Set: true},
			DropEnum:   ConfigBool{Set: true},
		}
		for _, raw := range *d.Skip {
			kind, err := diffpolicy.ParseChangeKind(raw)
			if err != nil {
				return DiffConfig{}, fmt.Errorf("ptah.yaml diff.skip: %w", err)
			}
			setDiffSkipKind(&cfg.Skip, kind)
		}
	}
	if d.ConcurrentIndex != nil {
		cfg.ConcurrentIndex.Create = ConfigBool{Value: *d.ConcurrentIndex, Set: true}
	}
	if d.ConcurrentIndexDrop != nil {
		cfg.ConcurrentIndex.Drop = ConfigBool{Value: *d.ConcurrentIndexDrop, Set: true}
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
