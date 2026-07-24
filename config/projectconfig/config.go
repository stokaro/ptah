// Package projectconfig loads Ptah project-level configuration from Ptah and
// Atlas project config files.
package projectconfig

import (
	"slices"

	"github.com/stokaro/ptah/migration/diffpolicy"
)

const (
	// PtahFileName is the conventional Ptah project config file.
	PtahFileName = "ptah.yaml"
	// AtlasFileName is the conventional Atlas project config file.
	AtlasFileName = "atlas.hcl"
)

// Config is Ptah's project-level configuration IR. Loaders translate supported
// file formats into this shape; command code should consume this type instead
// of branching on the original file format.
type Config struct {
	// EnvName is the selected project env name, when the source had one.
	EnvName string
	// DatabaseURL is the target database URL used by migration commands.
	DatabaseURL string
	// DevURL is the disposable dev/shadow database URL.
	DevURL string
	// SchemaSources are desired schema source URLs.
	SchemaSources []string
	// Schemas restricts database introspection to selected schemas.
	Schemas []string
	// Exclude lists schema patterns excluded by project config.
	Exclude []string
	// Schema holds Atlas declarative schema settings.
	Schema SchemaConfig
	// Migration holds migration-directory and runtime settings.
	Migration MigrationConfig
	// Lint holds migration-lint settings.
	Lint LintConfig
	// Format holds Atlas-compatible output templates.
	Format FormatConfig
	// Diff holds Atlas-compatible schema diff policy.
	Diff DiffConfig

	presence configPresence
}

type configPresence struct {
	databaseURL       bool
	devURL            bool
	schemaSources     bool
	schemas           bool
	exclude           bool
	lintDisabledRules bool
}

// ConfigBool preserves whether a boolean project config value was set
// explicitly, which is needed for Atlas global/env inheritance.
type ConfigBool struct {
	Value bool
	Set   bool
}

// SchemaConfig holds Atlas env.schema settings.
type SchemaConfig struct {
	Mode SchemaModeConfig
}

// SchemaModeConfig holds Atlas env.schema.mode settings.
type SchemaModeConfig struct {
	Funcs       ConfigBool
	Objects     ConfigBool
	Permissions ConfigBool
	Roles       ConfigBool
	Tables      ConfigBool
	Triggers    ConfigBool
	Types       ConfigBool
	Views       ConfigBool
}

// MigrationConfig is the migration section of the project config IR.
type MigrationConfig struct {
	Dir                  string
	Format               string
	RevisionsSchema      string
	RevisionsTable       string
	RevisionFormat       string
	LockTimeout          string
	StatementTimeout     string
	ConnectTimeout       string
	MigrationLockTimeout string
	ExecOrder            string
	TxMode               string
	PreUpHook            string
	PreDownHook          string
	PostgresDumpTo       string
	MySQLDumpTo          string
	Webhook              string
}

// LintConfig is the lint section of the project config IR.
type LintConfig struct {
	Dialect       string
	DisabledRules []string
	RuleConfigs   map[string]LintRuleConfig
	Latest        *int
	GitBase       string
	GitDir        string
}

// LintRuleConfig holds project-level overrides for one lint rule code or
// rule-family prefix.
type LintRuleConfig struct {
	Severity string
	Exclude  []string
}

// FormatConfig holds Atlas env.format command templates.
type FormatConfig struct {
	Migrate MigrateFormatConfig
	Schema  SchemaFormatConfig
}

// MigrateFormatConfig holds Atlas env.format.migrate templates.
type MigrateFormatConfig struct {
	Apply  string
	Diff   string
	Lint   string
	Status string
}

// SchemaFormatConfig holds Atlas env.format.schema templates.
type SchemaFormatConfig struct {
	Apply   string
	Clean   string
	Diff    string
	Inspect string
}

// DiffConfig holds Atlas diff policy blocks.
type DiffConfig struct {
	Skip            DiffSkipConfig
	ConcurrentIndex DiffConcurrentIndexConfig
}

// DiffSkipConfig holds the diff.skip policy: the destructive change kinds a
// project omits from generated migrations. Each field is a tri-state so an
// explicit false can override an inherited true.
type DiffSkipConfig struct {
	DropTable  ConfigBool
	DropColumn ConfigBool
	DropIndex  ConfigBool
	DropEnum   ConfigBool
}

// SkipChangeKinds returns the change kinds this policy skips, in the canonical
// diffpolicy order. It is the bridge from the config IR to the planner/generator
// vocabulary.
func (c DiffConfig) SkipChangeKinds() []diffpolicy.ChangeKind {
	var kinds []diffpolicy.ChangeKind
	if c.Skip.DropTable.Value {
		kinds = append(kinds, diffpolicy.DropTable)
	}
	if c.Skip.DropColumn.Value {
		kinds = append(kinds, diffpolicy.DropColumn)
	}
	if c.Skip.DropIndex.Value {
		kinds = append(kinds, diffpolicy.DropIndex)
	}
	if c.Skip.DropEnum.Value {
		kinds = append(kinds, diffpolicy.DropEnum)
	}
	return kinds
}

// ConcurrentIndexCreate reports whether the policy requests
// CREATE INDEX CONCURRENTLY for newly added indexes.
func (c DiffConfig) ConcurrentIndexCreate() bool {
	return c.ConcurrentIndex.Create.Value
}

// DiffConcurrentIndexConfig holds Atlas diff.concurrent_index policy.
type DiffConcurrentIndexConfig struct {
	Create ConfigBool
	Drop   ConfigBool
}

// ExcludePatterns returns Atlas-style exclude filters for explicitly disabled
// schema.mode resources.
func (m SchemaModeConfig) ExcludePatterns() []string {
	patterns := make([]string, 0, 8)
	patterns = appendDisabledMode(patterns, m.Tables, "*[type=table]")
	patterns = appendDisabledMode(patterns, m.Views, "*[type=view|materialized_view]")
	patterns = appendDisabledMode(patterns, m.Triggers, "*[type=trigger]")
	patterns = appendDisabledMode(patterns, m.Funcs, "*[type=function]")
	patterns = appendDisabledMode(patterns, m.Types, "*[type=enum]")
	patterns = appendDisabledMode(patterns, m.Roles, "*[type=role]")
	patterns = appendDisabledMode(patterns, m.Permissions, "*[type=grant]")
	patterns = appendDisabledMode(patterns, m.Objects, "*[type=extension]")
	return patterns
}

func appendDisabledMode(patterns []string, option ConfigBool, pattern string) []string {
	if option.Set && !option.Value {
		return append(patterns, pattern)
	}
	return patterns
}

// Merge returns base overridden by non-zero values from override.
func Merge(base, override Config) Config {
	result := base
	if override.EnvName != "" {
		result.EnvName = override.EnvName
	}
	if override.presence.databaseURL || override.DatabaseURL != "" {
		result.DatabaseURL = override.DatabaseURL
		result.presence.databaseURL = true
	}
	if override.presence.devURL || override.DevURL != "" {
		result.DevURL = override.DevURL
		result.presence.devURL = true
	}
	if override.presence.schemaSources || len(override.SchemaSources) > 0 {
		result.SchemaSources = slices.Clone(override.SchemaSources)
		result.presence.schemaSources = true
	}
	if override.presence.schemas || len(override.Schemas) > 0 {
		result.Schemas = slices.Clone(override.Schemas)
		result.presence.schemas = true
	}
	if override.presence.exclude || len(override.Exclude) > 0 {
		result.Exclude = slices.Clone(override.Exclude)
		result.presence.exclude = true
	}
	result.Schema = mergeSchema(result.Schema, override.Schema)
	result.Migration = mergeMigration(result.Migration, override.Migration)
	result.Lint = mergeLint(result.Lint, override.Lint, override.presence)
	result.Format = mergeFormat(result.Format, override.Format)
	result.Diff = mergeDiff(result.Diff, override.Diff)
	if override.presence.lintDisabledRules || len(override.Lint.DisabledRules) > 0 {
		result.presence.lintDisabledRules = true
	}
	return result
}

func mergeSchema(base, override SchemaConfig) SchemaConfig {
	result := base
	result.Mode.Funcs = mergeBool(result.Mode.Funcs, override.Mode.Funcs)
	result.Mode.Objects = mergeBool(result.Mode.Objects, override.Mode.Objects)
	result.Mode.Permissions = mergeBool(result.Mode.Permissions, override.Mode.Permissions)
	result.Mode.Roles = mergeBool(result.Mode.Roles, override.Mode.Roles)
	result.Mode.Tables = mergeBool(result.Mode.Tables, override.Mode.Tables)
	result.Mode.Triggers = mergeBool(result.Mode.Triggers, override.Mode.Triggers)
	result.Mode.Types = mergeBool(result.Mode.Types, override.Mode.Types)
	result.Mode.Views = mergeBool(result.Mode.Views, override.Mode.Views)
	return result
}

func mergeMigration(base, override MigrationConfig) MigrationConfig {
	result := base
	if override.Dir != "" {
		result.Dir = override.Dir
	}
	if override.Format != "" {
		result.Format = override.Format
	}
	if override.RevisionsSchema != "" {
		result.RevisionsSchema = override.RevisionsSchema
	}
	if override.RevisionsTable != "" {
		result.RevisionsTable = override.RevisionsTable
	}
	if override.RevisionFormat != "" {
		result.RevisionFormat = override.RevisionFormat
	}
	if override.LockTimeout != "" {
		result.LockTimeout = override.LockTimeout
	}
	if override.StatementTimeout != "" {
		result.StatementTimeout = override.StatementTimeout
	}
	if override.ConnectTimeout != "" {
		result.ConnectTimeout = override.ConnectTimeout
	}
	if override.MigrationLockTimeout != "" {
		result.MigrationLockTimeout = override.MigrationLockTimeout
	}
	if override.ExecOrder != "" {
		result.ExecOrder = override.ExecOrder
	}
	if override.TxMode != "" {
		result.TxMode = override.TxMode
	}
	if override.PreUpHook != "" {
		result.PreUpHook = override.PreUpHook
	}
	if override.PreDownHook != "" {
		result.PreDownHook = override.PreDownHook
	}
	if override.PostgresDumpTo != "" {
		result.PostgresDumpTo = override.PostgresDumpTo
	}
	if override.MySQLDumpTo != "" {
		result.MySQLDumpTo = override.MySQLDumpTo
	}
	if override.Webhook != "" {
		result.Webhook = override.Webhook
	}
	return result
}

func mergeLint(base, override LintConfig, presence configPresence) LintConfig {
	result := base
	if override.Dialect != "" {
		result.Dialect = override.Dialect
	}
	if presence.lintDisabledRules || len(override.DisabledRules) > 0 {
		result.DisabledRules = slices.Clone(override.DisabledRules)
	}
	if len(override.RuleConfigs) > 0 {
		result.RuleConfigs = mergeLintRuleConfigs(result.RuleConfigs, override.RuleConfigs)
	}
	if override.Latest != nil {
		latest := *override.Latest
		result.Latest = &latest
		result.GitBase = ""
		result.GitDir = ""
	}
	if override.GitBase != "" {
		result.GitBase = override.GitBase
		result.Latest = nil
	}
	if override.GitDir != "" {
		result.GitDir = override.GitDir
	}
	return result
}

func mergeLintRuleConfigs(
	base map[string]LintRuleConfig,
	override map[string]LintRuleConfig,
) map[string]LintRuleConfig {
	result := cloneLintRuleConfigs(base)
	if result == nil {
		result = make(map[string]LintRuleConfig, len(override))
	}
	for code, config := range override {
		baseConfig := result[code]
		if config.Severity != "" {
			baseConfig.Severity = config.Severity
		}
		if len(config.Exclude) > 0 {
			baseConfig.Exclude = slices.Clone(config.Exclude)
		}
		result[code] = baseConfig
	}
	return result
}

func cloneLintRuleConfigs(values map[string]LintRuleConfig) map[string]LintRuleConfig {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]LintRuleConfig, len(values))
	for code, config := range values {
		config.Exclude = slices.Clone(config.Exclude)
		cloned[code] = config
	}
	return cloned
}

func mergeFormat(base, override FormatConfig) FormatConfig {
	result := base
	if override.Migrate.Apply != "" {
		result.Migrate.Apply = override.Migrate.Apply
	}
	if override.Migrate.Diff != "" {
		result.Migrate.Diff = override.Migrate.Diff
	}
	if override.Migrate.Lint != "" {
		result.Migrate.Lint = override.Migrate.Lint
	}
	if override.Migrate.Status != "" {
		result.Migrate.Status = override.Migrate.Status
	}
	if override.Schema.Apply != "" {
		result.Schema.Apply = override.Schema.Apply
	}
	if override.Schema.Clean != "" {
		result.Schema.Clean = override.Schema.Clean
	}
	if override.Schema.Diff != "" {
		result.Schema.Diff = override.Schema.Diff
	}
	if override.Schema.Inspect != "" {
		result.Schema.Inspect = override.Schema.Inspect
	}
	return result
}

func mergeDiff(base, override DiffConfig) DiffConfig {
	result := base
	result.Skip.DropTable = mergeBool(result.Skip.DropTable, override.Skip.DropTable)
	result.Skip.DropColumn = mergeBool(result.Skip.DropColumn, override.Skip.DropColumn)
	result.Skip.DropIndex = mergeBool(result.Skip.DropIndex, override.Skip.DropIndex)
	result.Skip.DropEnum = mergeBool(result.Skip.DropEnum, override.Skip.DropEnum)
	result.ConcurrentIndex.Create = mergeBool(result.ConcurrentIndex.Create, override.ConcurrentIndex.Create)
	result.ConcurrentIndex.Drop = mergeBool(result.ConcurrentIndex.Drop, override.ConcurrentIndex.Drop)
	return result
}

func mergeBool(base, override ConfigBool) ConfigBool {
	if override.Set {
		return override
	}
	return base
}
