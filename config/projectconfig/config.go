// Package projectconfig loads Ptah project-level configuration from Ptah and
// Atlas project config files.
package projectconfig

import (
	"fmt"
	"slices"

	"github.com/stokaro/ptah/migration/diffpolicy"
)

const (
	// PtahFileName is the conventional Ptah project config file.
	PtahFileName = "ptah.yaml"
	// AtlasFileName is the conventional Atlas project config file.
	AtlasFileName = "atlas.hcl"
)

// Canonical online-DDL tool names accepted in project configuration.
const (
	// OnlineDDLToolGhost routes ALTERs through GitHub's gh-ost.
	OnlineDDLToolGhost = "ghost"
	// OnlineDDLToolPTOSC routes ALTERs through Percona's
	// pt-online-schema-change.
	OnlineDDLToolPTOSC = "pt-osc"
)

// Canonical online-DDL fallback policies accepted in project configuration.
const (
	// OnlineDDLFallbackError aborts instead of degrading to a plain ALTER
	// TABLE.
	OnlineDDLFallbackError = "error"
	// OnlineDDLFallbackPlain lets the migrator execute the plain ALTER TABLE.
	OnlineDDLFallbackPlain = "plain"
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
	// OnlineDDL configures automatic online-DDL routing.
	OnlineDDL OnlineDDLConfig
	// Lint holds migration-lint settings.
	Lint LintConfig
	// Format holds Atlas-compatible output templates.
	Format FormatConfig
	// Diff holds Atlas-compatible schema diff policy.
	Diff DiffConfig
	// ExternalSchema configures an external program that emits the desired
	// schema to stdout.
	ExternalSchema ExternalSchemaConfig

	presence configPresence
}

type configPresence struct {
	databaseURL            bool
	devURL                 bool
	schemaSources          bool
	schemas                bool
	exclude                bool
	lintDisabledRules      bool
	externalSchemaProgram  bool
	externalSchemaEnv      bool
	onlineDDLTool          bool
	onlineDDLThresholdRows bool
	onlineDDLArgs          bool
	onlineDDLFallback      bool
}

// OnlineDDLConfig configures automatic online-DDL routing for ALTER TABLE
// statements.
type OnlineDDLConfig struct {
	// Tool selects the online-DDL tool: "ghost" or "pt-osc". Empty disables
	// automatic routing; per-migration directives keep working.
	Tool string
	// ThresholdRows routes ALTER TABLE statements when the target table's
	// estimated row count is at or above this value. Zero disables automatic
	// routing.
	ThresholdRows int64
	// Args are extra arguments appended to every online-DDL tool invocation.
	Args []string
	// Fallback controls what happens when a selected online-DDL path cannot be
	// used: "error" aborts and "plain" executes the plain ALTER TABLE. Empty
	// uses the source default.
	Fallback string
}

// Enabled reports whether automatic threshold routing is configured.
// Directive-based routing works regardless.
func (c OnlineDDLConfig) Enabled() bool {
	return c.Tool != "" && c.ThresholdRows > 0
}

// Validate checks the online-DDL configuration values.
func (c OnlineDDLConfig) Validate() error {
	switch c.Tool {
	case "", OnlineDDLToolGhost, OnlineDDLToolPTOSC:
	default:
		return fmt.Errorf(
			"unknown online_ddl tool %q: expected %s or %s",
			c.Tool,
			OnlineDDLToolGhost,
			OnlineDDLToolPTOSC,
		)
	}
	if c.ThresholdRows < 0 {
		return fmt.Errorf("online_ddl threshold_rows must not be negative, got %d", c.ThresholdRows)
	}
	if c.ThresholdRows > 0 && c.Tool == "" {
		return fmt.Errorf("online_ddl threshold_rows is set but no tool is configured")
	}
	switch c.Fallback {
	case "", OnlineDDLFallbackError, OnlineDDLFallbackPlain:
		return nil
	default:
		return fmt.Errorf(
			"unknown online_ddl fallback %q: expected %s or %s",
			c.Fallback,
			OnlineDDLFallbackError,
			OnlineDDLFallbackPlain,
		)
	}
}

// ExternalSchemaConfig configures an external program whose standard output is
// the desired schema. It is Ptah's open equivalent of Atlas's external_schema
// data source: Program is run directly (no shell) and must print the complete
// desired schema as SQL, HCL, or YAML to stdout.
type ExternalSchemaConfig struct {
	// Program is the executable and its arguments as an explicit argv list.
	// Program[0] is the command; it is run without a shell.
	Program []string
	// Format is the stdout format: "sql" (default), "hcl", or "yaml".
	Format string
	// WorkingDir is the directory the program runs in; empty uses the current
	// working directory.
	WorkingDir string
	// Env holds extra "KEY=VALUE" entries passed to the program.
	Env []string
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
	result.OnlineDDL = mergeOnlineDDL(result.OnlineDDL, override.OnlineDDL, override.presence)
	result.Lint = mergeLint(result.Lint, override.Lint, override.presence)
	result.Format = mergeFormat(result.Format, override.Format)
	result.Diff = mergeDiff(result.Diff, override.Diff)
	result.ExternalSchema = mergeExternalSchema(result.ExternalSchema, override.ExternalSchema, override.presence)
	result.presence = mergeConfigPresence(result.presence, override)
	return result
}

func mergeConfigPresence(result configPresence, override Config) configPresence {
	if override.presence.lintDisabledRules || len(override.Lint.DisabledRules) > 0 {
		result.lintDisabledRules = true
	}
	if override.presence.externalSchemaProgram || len(override.ExternalSchema.Program) > 0 {
		result.externalSchemaProgram = true
	}
	if override.presence.externalSchemaEnv || len(override.ExternalSchema.Env) > 0 {
		result.externalSchemaEnv = true
	}
	if override.presence.onlineDDLTool || override.OnlineDDL.Tool != "" {
		result.onlineDDLTool = true
	}
	if override.presence.onlineDDLThresholdRows || override.OnlineDDL.ThresholdRows != 0 {
		result.onlineDDLThresholdRows = true
	}
	if override.presence.onlineDDLArgs || len(override.OnlineDDL.Args) > 0 {
		result.onlineDDLArgs = true
	}
	if override.presence.onlineDDLFallback || override.OnlineDDL.Fallback != "" {
		result.onlineDDLFallback = true
	}
	return result
}

func mergeOnlineDDL(
	base OnlineDDLConfig,
	override OnlineDDLConfig,
	presence configPresence,
) OnlineDDLConfig {
	result := base
	result.Args = slices.Clone(base.Args)
	toolSet := presence.onlineDDLTool || override.Tool != ""
	thresholdSet := presence.onlineDDLThresholdRows || override.ThresholdRows != 0
	if toolSet {
		result.Tool = override.Tool
		if override.Tool == "" && !thresholdSet {
			result.ThresholdRows = 0
		}
	}
	if thresholdSet {
		result.ThresholdRows = override.ThresholdRows
	}
	if presence.onlineDDLArgs || len(override.Args) > 0 {
		result.Args = slices.Clone(override.Args)
	}
	if presence.onlineDDLFallback || override.Fallback != "" {
		result.Fallback = override.Fallback
	}
	return result
}

func mergeExternalSchema(base, override ExternalSchemaConfig, presence configPresence) ExternalSchemaConfig {
	result := base
	if presence.externalSchemaProgram || len(override.Program) > 0 {
		result.Program = slices.Clone(override.Program)
	}
	if override.Format != "" {
		result.Format = override.Format
	}
	if override.WorkingDir != "" {
		result.WorkingDir = override.WorkingDir
	}
	if presence.externalSchemaEnv || len(override.Env) > 0 {
		result.Env = slices.Clone(override.Env)
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
