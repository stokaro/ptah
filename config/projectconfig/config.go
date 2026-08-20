// Package projectconfig loads Ptah project-level configuration from Ptah and
// Atlas project config files.
package projectconfig

import (
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"strings"

	"go.5x5.cz/ptah/migration/diffpolicy"
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
	// IgnoredConstructs lists the atlas.hcl names that were accepted and not
	// acted on, under Atlas CE's unknown-name policy.
	//
	// Atlas CE reports nothing for these names. Ptah records them so callers can
	// make the no-op visible; the Ptah CLIs warn on stderr while preserving the
	// command's stdout and exit code.
	IgnoredConstructs []IgnoredAtlasConstruct
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
	// schemaSourceVars maps a SchemaSources entry to the variable values the
	// atlas.hcl `data "hcl_schema"` block that minted it scopes to its files.
	// Read through [Config.SchemaSourceVars]; see there for the rule it carries.
	schemaSourceVars     map[string]map[string]string
	migrationDirectories map[string]MigrationDirectorySource
}

// SchemaSourceVars reports the variable values scoped to one desired-state
// schema source, and whether that source came from an atlas.hcl
// `data "hcl_schema"` block at all.
//
// The bool is the load-bearing half. A data source that declares NO `vars`
// still closes the boundary, so `ok` cannot be derived from a non-empty map.
// Measured on the pinned Atlas community binary v1.3.0 with
// `schema apply --env local --dry-run` against a schema file declaring
// `variable "tenant" { type = string }` with no default, exit codes read
// directly from unpiped invocations:
//
//	src = data.hcl_schema.app.url, vars = { tenant = "acme" }   -> 0
//	src = data.hcl_schema.app.url, no vars                      -> 1  missing value
//	                                                                  for required
//	                                                                  variable "tenant"
//	  ... the same run with --var tenant=acme                   -> 1  (the leak probe:
//	                                                                  the flag does not
//	                                                                  cross the boundary)
//	src = "file://s.hcl" with --var tenant=acme                 -> 0  (the control:
//	                                                                  --var DOES reach a
//	                                                                  file that no data
//	                                                                  source selected)
//
// The control is what separates "the boundary is closed" from "this binary has
// no --var": the same flag, the same file, the same command, and only the way
// the env names the source differs.
func (c Config) SchemaSourceVars(rawURL string) (map[string]string, bool) {
	values, ok := c.schemaSourceVars[rawURL]
	if !ok {
		return nil, false
	}
	return maps.Clone(values), true
}

// MigrationDirectorySource is the rendered filesystem and source path behind
// one data.template_dir URL. FileSystem is the immutable view commands read;
// Path is relative to the atlas.hcl root and is where writing migration verbs
// synchronize newly created files.
type MigrationDirectorySource struct {
	FileSystem fs.FS
	Path       string
	// ReadOnly marks a source with no local directory behind it, so a writing
	// verb is refused rather than given a path to create.
	//
	// A rendered template directory has a real path under the project root and
	// a writer synchronizes into it. A directory fetched from a registry does
	// not: giving the writer an OCI reference to join to the root would create
	// a literal `oci:/registry/repo:tag` directory and report success while the
	// registry stayed unchanged (stokaro/ptah#1210).
	ReadOnly bool
}

// MigrationDirectoryFS returns the immutable filesystem produced for a
// data.template_dir URL. The boolean is false for ordinary file:// and remote
// migration directories.
func (c Config) MigrationDirectoryFS(rawURL string) (fs.FS, bool) {
	source, ok := c.migrationDirectories[rawURL]
	return source.FileSystem, ok
}

// MigrationDirectorySource returns the rendered filesystem and sandboxed
// backing path produced for a data.template_dir URL. The boolean is false for
// ordinary file:// and remote migration directories.
func (c Config) MigrationDirectorySource(rawURL string) (MigrationDirectorySource, bool) {
	source, ok := c.migrationDirectories[rawURL]
	return source, ok
}

func cloneMigrationDirectories(
	source map[string]MigrationDirectorySource,
) map[string]MigrationDirectorySource {
	if len(source) == 0 {
		return nil
	}
	return maps.Clone(source)
}

// Value carries a project-config value together with whether a source or
// programmatic override supplied it. Command defaults must apply only when
// Present is false.
type Value[T any] struct {
	Value   T
	Present bool
}

// StringField identifies one string-valued Config field for presence-aware
// command-default resolution.
type StringField uint8

// String-valued project-config fields.
const (
	StringEnvName StringField = iota
	StringDatabaseURL
	StringDevURL
	StringMigrationDir
	StringMigrationFormat
	StringMigrationRevisionsSchema
	StringMigrationRevisionsTable
	StringMigrationRevisionFormat
	StringMigrationLockTimeout
	StringMigrationStatementTimeout
	StringMigrationConnectTimeout
	StringMigrationMigrationLockTimeout
	StringMigrationExecOrder
	StringMigrationTxMode
	StringMigrationPreUpHook
	StringMigrationPreDownHook
	StringMigrationPostgresDumpTo
	StringMigrationMySQLDumpTo
	StringMigrationWebhook
	StringOnlineDDLTool
	StringOnlineDDLFallback
	StringLintDialect
	StringLintGitBase
	StringLintGitDir
	StringFormatMigrateApply
	StringFormatMigrateDiff
	StringFormatMigrateLint
	StringFormatMigrateStatus
	StringFormatSchemaApply
	StringFormatSchemaClean
	StringFormatSchemaDiff
	StringFormatSchemaInspect
	StringExternalSchemaFormat
	StringExternalSchemaWorkingDir
	// StringMigrationBaseline is appended rather than filed next to the other
	// migration names on purpose: StringField is an exported iota enum, and
	// inserting into the middle would renumber every constant after it.
	StringMigrationBaseline
	stringFieldCount
)

type configField string

const (
	fieldEnvName                   configField = "env.name"
	fieldDatabaseURL               configField = "database.url"
	fieldDevURL                    configField = "database.dev_url"
	fieldSchemaSources             configField = "schema.sources"
	fieldSchemas                   configField = "database.schemas"
	fieldExclude                   configField = "database.exclude"
	fieldMigrationDir              configField = "migration.dir"
	fieldMigrationFormat           configField = "migration.format"
	fieldMigrationRevisionsSchema  configField = "migration.revisions_schema"
	fieldMigrationRevisionsTable   configField = "migration.revisions_table"
	fieldMigrationRevisionFormat   configField = "migration.revision_format"
	fieldMigrationLockTimeout      configField = "migration.lock_timeout"
	fieldMigrationStatementTimeout configField = "migration.statement_timeout"
	fieldMigrationConnectTimeout   configField = "migration.connect_timeout"
	fieldMigrationMigrationLock    configField = "migration.migration_lock_timeout"
	fieldMigrationExecOrder        configField = "migration.exec_order"
	fieldMigrationTxMode           configField = "migration.tx_mode"
	fieldMigrationPreUpHook        configField = "migration.pre_up_hook"
	fieldMigrationPreDownHook      configField = "migration.pre_down_hook"
	fieldMigrationPostgresDumpTo   configField = "migration.pg_dump_to"
	fieldMigrationMySQLDumpTo      configField = "migration.mysqldump_to"
	fieldMigrationWebhook          configField = "migration.webhook"
	fieldMigrationBaseline         configField = "migration.baseline"
	fieldOnlineDDLTool             configField = "online_ddl.tool"
	fieldOnlineDDLThresholdRows    configField = "online_ddl.threshold_rows"
	fieldOnlineDDLArgs             configField = "online_ddl.args"
	fieldOnlineDDLFallback         configField = "online_ddl.fallback"
	fieldLintDialect               configField = "lint.dialect"
	fieldLintDisabledRules         configField = "lint.disabled_rules"
	fieldLintRuleConfigs           configField = "lint.rules"
	fieldLintLatest                configField = "lint.latest"
	fieldLintGitBase               configField = "lint.git.base"
	fieldLintGitDir                configField = "lint.git.dir"
	fieldFormatMigrateApply        configField = "format.migrate.apply"
	fieldFormatMigrateDiff         configField = "format.migrate.diff"
	fieldFormatMigrateLint         configField = "format.migrate.lint"
	fieldFormatMigrateStatus       configField = "format.migrate.status"
	fieldFormatSchemaApply         configField = "format.schema.apply"
	fieldFormatSchemaClean         configField = "format.schema.clean"
	fieldFormatSchemaDiff          configField = "format.schema.diff"
	fieldFormatSchemaInspect       configField = "format.schema.inspect"
	fieldExternalSchemaProgram     configField = "external_schema.program"
	fieldExternalSchemaFormat      configField = "external_schema.format"
	fieldExternalSchemaWorkingDir  configField = "external_schema.working_dir"
	fieldExternalSchemaEnv         configField = "external_schema.env"
	fieldSchemaRepoName            configField = "schema.repo.name"
	lintRuleConfigFieldPrefix                  = "lint.rules."
)

type configPresence map[configField]struct{}

func (p configPresence) has(field configField) bool {
	_, ok := p[field]
	return ok
}

func (p *configPresence) mark(field configField) {
	if *p == nil {
		*p = make(configPresence)
	}
	(*p)[field] = struct{}{}
}

func (p *configPresence) unmark(field configField) {
	delete(*p, field)
}

func (p *configPresence) removePrefix(prefix string) {
	for field := range *p {
		if strings.HasPrefix(string(field), prefix) {
			delete(*p, field)
		}
	}
}

func (p configPresence) clone() configPresence {
	if len(p) == 0 {
		return nil
	}
	result := make(configPresence, len(p))
	for field := range p {
		result[field] = struct{}{}
	}
	return result
}

func lintRuleSeverityField(code string) configField {
	return configField(lintRuleConfigFieldPrefix + code + ".severity")
}

func lintRuleExcludeField(code string) configField {
	return configField(lintRuleConfigFieldPrefix + code + ".exclude")
}

type stringFieldDescriptor struct {
	presence configField
	value    func(Config) string
}

var stringFieldDescriptors = [stringFieldCount]stringFieldDescriptor{
	StringEnvName: {
		presence: fieldEnvName,
		value:    func(c Config) string { return c.EnvName },
	},
	StringDatabaseURL: {
		presence: fieldDatabaseURL,
		value:    func(c Config) string { return c.DatabaseURL },
	},
	StringDevURL: {
		presence: fieldDevURL,
		value:    func(c Config) string { return c.DevURL },
	},
	StringMigrationDir: {
		presence: fieldMigrationDir,
		value:    func(c Config) string { return c.Migration.Dir },
	},
	StringMigrationFormat: {
		presence: fieldMigrationFormat,
		value:    func(c Config) string { return c.Migration.Format },
	},
	StringMigrationRevisionsSchema: {
		presence: fieldMigrationRevisionsSchema,
		value:    func(c Config) string { return c.Migration.RevisionsSchema },
	},
	StringMigrationBaseline: {
		presence: fieldMigrationBaseline,
		value:    func(c Config) string { return c.Migration.Baseline },
	},
	StringMigrationRevisionsTable: {
		presence: fieldMigrationRevisionsTable,
		value:    func(c Config) string { return c.Migration.RevisionsTable },
	},
	StringMigrationRevisionFormat: {
		presence: fieldMigrationRevisionFormat,
		value:    func(c Config) string { return c.Migration.RevisionFormat },
	},
	StringMigrationLockTimeout: {
		presence: fieldMigrationLockTimeout,
		value:    func(c Config) string { return c.Migration.LockTimeout },
	},
	StringMigrationStatementTimeout: {
		presence: fieldMigrationStatementTimeout,
		value:    func(c Config) string { return c.Migration.StatementTimeout },
	},
	StringMigrationConnectTimeout: {
		presence: fieldMigrationConnectTimeout,
		value:    func(c Config) string { return c.Migration.ConnectTimeout },
	},
	StringMigrationMigrationLockTimeout: {
		presence: fieldMigrationMigrationLock,
		value:    func(c Config) string { return c.Migration.MigrationLockTimeout },
	},
	StringMigrationExecOrder: {
		presence: fieldMigrationExecOrder,
		value:    func(c Config) string { return c.Migration.ExecOrder },
	},
	StringMigrationTxMode: {
		presence: fieldMigrationTxMode,
		value:    func(c Config) string { return c.Migration.TxMode },
	},
	StringMigrationPreUpHook: {
		presence: fieldMigrationPreUpHook,
		value:    func(c Config) string { return c.Migration.PreUpHook },
	},
	StringMigrationPreDownHook: {
		presence: fieldMigrationPreDownHook,
		value:    func(c Config) string { return c.Migration.PreDownHook },
	},
	StringMigrationPostgresDumpTo: {
		presence: fieldMigrationPostgresDumpTo,
		value:    func(c Config) string { return c.Migration.PostgresDumpTo },
	},
	StringMigrationMySQLDumpTo: {
		presence: fieldMigrationMySQLDumpTo,
		value:    func(c Config) string { return c.Migration.MySQLDumpTo },
	},
	StringMigrationWebhook: {
		presence: fieldMigrationWebhook,
		value:    func(c Config) string { return c.Migration.Webhook },
	},
	StringOnlineDDLTool: {
		presence: fieldOnlineDDLTool,
		value:    func(c Config) string { return c.OnlineDDL.Tool },
	},
	StringOnlineDDLFallback: {
		presence: fieldOnlineDDLFallback,
		value:    func(c Config) string { return c.OnlineDDL.Fallback },
	},
	StringLintDialect: {
		presence: fieldLintDialect,
		value:    func(c Config) string { return c.Lint.Dialect },
	},
	StringLintGitBase: {
		presence: fieldLintGitBase,
		value:    func(c Config) string { return c.Lint.GitBase },
	},
	StringLintGitDir: {
		presence: fieldLintGitDir,
		value:    func(c Config) string { return c.Lint.GitDir },
	},
	StringFormatMigrateApply: {
		presence: fieldFormatMigrateApply,
		value:    func(c Config) string { return c.Format.Migrate.Apply },
	},
	StringFormatMigrateDiff: {
		presence: fieldFormatMigrateDiff,
		value:    func(c Config) string { return c.Format.Migrate.Diff },
	},
	StringFormatMigrateLint: {
		presence: fieldFormatMigrateLint,
		value:    func(c Config) string { return c.Format.Migrate.Lint },
	},
	StringFormatMigrateStatus: {
		presence: fieldFormatMigrateStatus,
		value:    func(c Config) string { return c.Format.Migrate.Status },
	},
	StringFormatSchemaApply: {
		presence: fieldFormatSchemaApply,
		value:    func(c Config) string { return c.Format.Schema.Apply },
	},
	StringFormatSchemaClean: {
		presence: fieldFormatSchemaClean,
		value:    func(c Config) string { return c.Format.Schema.Clean },
	},
	StringFormatSchemaDiff: {
		presence: fieldFormatSchemaDiff,
		value:    func(c Config) string { return c.Format.Schema.Diff },
	},
	StringFormatSchemaInspect: {
		presence: fieldFormatSchemaInspect,
		value:    func(c Config) string { return c.Format.Schema.Inspect },
	},
	StringExternalSchemaFormat: {
		presence: fieldExternalSchemaFormat,
		value:    func(c Config) string { return c.ExternalSchema.Format },
	},
	StringExternalSchemaWorkingDir: {
		presence: fieldExternalSchemaWorkingDir,
		value:    func(c Config) string { return c.ExternalSchema.WorkingDir },
	},
}

// StringValue returns a string-valued field together with its source presence.
// Non-empty programmatic values count as present even without loader metadata.
// Unknown fields return an absent value.
func (c Config) StringValue(field StringField) Value[string] {
	if field >= stringFieldCount {
		return Value[string]{}
	}
	descriptor := stringFieldDescriptors[field]
	value := descriptor.value(c)
	return Value[string]{
		Value:   value,
		Present: c.presence.has(descriptor.presence) || value != "",
	}
}

// SchemaSourcesValue returns the desired schema sources with presence.
func (c Config) SchemaSourcesValue() Value[[]string] {
	return Value[[]string]{
		Value:   slices.Clone(c.SchemaSources),
		Present: c.presence.has(fieldSchemaSources) || len(c.SchemaSources) > 0,
	}
}

// SchemasValue returns the configured introspection schemas with presence.
func (c Config) SchemasValue() Value[[]string] {
	return Value[[]string]{
		Value:   slices.Clone(c.Schemas),
		Present: c.presence.has(fieldSchemas) || len(c.Schemas) > 0,
	}
}

// LintLatestValue returns lint.latest with presence. A present nil pointer is
// retained for completeness even though supported loaders materialize an int.
func (c Config) LintLatestValue() Value[int] {
	value := 0
	if c.Lint.Latest != nil {
		value = *c.Lint.Latest
	}
	return Value[int]{
		Value:   value,
		Present: c.presence.has(fieldLintLatest) || c.Lint.Latest != nil,
	}
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
	// Origin names the config file that supplied Program: PtahFileName for a
	// ptah.yaml external_schema block or AtlasFileName for an atlas.hcl
	// data.external_schema source. Safety-gate errors use it to point the user
	// at the file that configured the program.
	Origin string
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
	Repo SchemaRepoConfig
}

// SchemaRepoConfig holds Atlas env.schema.repo settings: the name of the schema
// repository a hosted registry would store the desired state under.
//
// The name is decoded and type-checked because the pinned community binary
// decodes it -- `repo { name = 1 }` is refused with `value of attr "name"
// cannot be read as string` on a command as cheap as `schema inspect`. Nothing
// reads the value afterwards, and that is also measured: with the community
// binary, `schema inspect`, `schema apply --dry-run`, and `schema apply
// --auto-approve` (both on an empty database and incrementally) produce
// byte-identical output with and without the block, and an `atlas://` URL is
// refused identically either way ("atlas remote state is not supported by the
// community version of Atlas"). Ptah likewise has no hosted registry --
// `schema plan --repo` already says so -- so accepting the name and acting on
// nothing is exact parity, not silent non-enforcement.
type SchemaRepoConfig struct {
	Name string
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
	// Baseline is the migration version `migrate apply` marks as already
	// applied before running the pending ones, the config spelling of its
	// --baseline flag.
	Baseline string
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
	// DropSchema mirrors Atlas diff.skip.drop_schema. It is decoded and
	// type-checked because the pinned community binary decodes it eagerly --
	// `drop_schema = "x"` is refused with `value of attr "drop_schema" cannot be
	// read as bool` even on `schema inspect`.
	//
	// It has no entry in SkipChangeKinds because Ptah's planner has no
	// schema-removal change kind to omit: migration/schemadiff/types.SchemaDiff
	// carries no removed-schema list and no code path renders DROP SCHEMA into a
	// plan. Measured on the community binary, `schema apply --dry-run` against a
	// realm URL holding a schema the desired state omits prints
	// `DROP SCHEMA "extra" CASCADE;` by default and `Schema is synced, no changes
	// to be made` with drop_schema = true; Ptah prints the synced line either way,
	// so honoring the suppression costs nothing and can never make Ptah emit a
	// drop the community binary would have withheld. Cleanup is unaffected on
	// both sides: `schema clean` on the community binary drops every schema with
	// the setting on -- measured -- and no Ptah code path reads this field at
	// all.
	DropSchema ConfigBool
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

// ConcurrentIndexDrop reports whether the policy requests
// DROP INDEX CONCURRENTLY for removed indexes.
func (c DiffConfig) ConcurrentIndexDrop() bool {
	return c.ConcurrentIndex.Drop.Value
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

// Merge returns base overridden by explicitly present loader values and
// non-zero programmatic values from override.
func Merge(base, override Config) Config {
	result := base
	result.migrationDirectories = cloneMigrationDirectories(base.migrationDirectories)
	if len(override.migrationDirectories) > 0 {
		if result.migrationDirectories == nil {
			result.migrationDirectories = make(map[string]MigrationDirectorySource)
		}
		maps.Copy(result.migrationDirectories, override.migrationDirectories)
	}
	result.IgnoredConstructs = slices.Concat(base.IgnoredConstructs, override.IgnoredConstructs)
	result.presence = base.presence.clone()
	result.EnvName = mergeStringValue(
		base.EnvName,
		override.EnvName,
		fieldEnvName,
		override.presence,
		&result.presence,
	)
	result.DatabaseURL = mergeStringValue(
		base.DatabaseURL,
		override.DatabaseURL,
		fieldDatabaseURL,
		override.presence,
		&result.presence,
	)
	result.DevURL = mergeStringValue(
		base.DevURL,
		override.DevURL,
		fieldDevURL,
		override.presence,
		&result.presence,
	)
	result.SchemaSources = mergeStringSliceValue(
		base.SchemaSources,
		override.SchemaSources,
		fieldSchemaSources,
		override.presence,
		&result.presence,
	)
	// The scope map travels with the list it describes. Keeping the base map
	// after the override replaced the list would attach one file's variables to
	// a source the surviving list never names.
	result.schemaSourceVars = base.schemaSourceVars
	if override.presence.has(fieldSchemaSources) || len(override.SchemaSources) > 0 {
		result.schemaSourceVars = override.schemaSourceVars
	}
	result.Schemas = mergeStringSliceValue(
		base.Schemas,
		override.Schemas,
		fieldSchemas,
		override.presence,
		&result.presence,
	)
	result.Exclude = mergeStringSliceValue(
		base.Exclude,
		override.Exclude,
		fieldExclude,
		override.presence,
		&result.presence,
	)
	result.Schema = mergeSchema(result.Schema, override.Schema, override.presence, &result.presence)
	result.Migration = mergeMigration(
		result.Migration,
		override.Migration,
		override.presence,
		&result.presence,
	)
	result.OnlineDDL = mergeOnlineDDL(
		result.OnlineDDL,
		override.OnlineDDL,
		override.presence,
		&result.presence,
	)
	result.Lint = mergeLint(
		result.Lint,
		override.Lint,
		override.presence,
		&result.presence,
	)
	result.Format = mergeFormat(
		result.Format,
		override.Format,
		override.presence,
		&result.presence,
	)
	result.Diff = mergeDiff(result.Diff, override.Diff)
	result.ExternalSchema = mergeExternalSchema(
		result.ExternalSchema,
		override.ExternalSchema,
		override.presence,
		&result.presence,
	)
	return result
}

func mergeStringValue(
	base,
	override string,
	field configField,
	overridePresence configPresence,
	resultPresence *configPresence,
) string {
	if !overridePresence.has(field) && override == "" {
		return base
	}
	resultPresence.mark(field)
	return override
}

func mergeStringSliceValue(
	base,
	override []string,
	field configField,
	overridePresence configPresence,
	resultPresence *configPresence,
) []string {
	if !overridePresence.has(field) && len(override) == 0 {
		return slices.Clone(base)
	}
	resultPresence.mark(field)
	return slices.Clone(override)
}

func mergeInt64Value(
	base,
	override int64,
	field configField,
	overridePresence configPresence,
	resultPresence *configPresence,
) int64 {
	if !overridePresence.has(field) && override == 0 {
		return base
	}
	resultPresence.mark(field)
	return override
}

func mergeOnlineDDL(
	base,
	override OnlineDDLConfig,
	overridePresence configPresence,
	resultPresence *configPresence,
) OnlineDDLConfig {
	result := base
	result.Args = slices.Clone(base.Args)
	toolSet := overridePresence.has(fieldOnlineDDLTool) || override.Tool != ""
	thresholdSet := overridePresence.has(fieldOnlineDDLThresholdRows) || override.ThresholdRows != 0
	if toolSet {
		result.Tool = override.Tool
		resultPresence.mark(fieldOnlineDDLTool)
		if override.Tool == "" && !thresholdSet {
			result.ThresholdRows = 0
			resultPresence.mark(fieldOnlineDDLThresholdRows)
		}
	}
	if thresholdSet {
		result.ThresholdRows = mergeInt64Value(
			result.ThresholdRows,
			override.ThresholdRows,
			fieldOnlineDDLThresholdRows,
			overridePresence,
			resultPresence,
		)
	}
	result.Args = mergeStringSliceValue(
		result.Args,
		override.Args,
		fieldOnlineDDLArgs,
		overridePresence,
		resultPresence,
	)
	result.Fallback = mergeStringValue(
		result.Fallback,
		override.Fallback,
		fieldOnlineDDLFallback,
		overridePresence,
		resultPresence,
	)
	return result
}

func mergeExternalSchema(
	base,
	override ExternalSchemaConfig,
	overridePresence configPresence,
	resultPresence *configPresence,
) ExternalSchemaConfig {
	result := base
	result.Program = mergeStringSliceValue(
		base.Program,
		override.Program,
		fieldExternalSchemaProgram,
		overridePresence,
		resultPresence,
	)
	// Origin follows Program: whichever source supplied the program names the
	// file the safety gate should point at.
	if overridePresence.has(fieldExternalSchemaProgram) || len(override.Program) > 0 {
		result.Origin = override.Origin
	}
	result.Format = mergeStringValue(
		base.Format,
		override.Format,
		fieldExternalSchemaFormat,
		overridePresence,
		resultPresence,
	)
	result.WorkingDir = mergeStringValue(
		base.WorkingDir,
		override.WorkingDir,
		fieldExternalSchemaWorkingDir,
		overridePresence,
		resultPresence,
	)
	result.Env = mergeStringSliceValue(
		base.Env,
		override.Env,
		fieldExternalSchemaEnv,
		overridePresence,
		resultPresence,
	)
	return result
}

func mergeSchema(
	base, override SchemaConfig,
	overridePresence configPresence,
	resultPresence *configPresence,
) SchemaConfig {
	result := base
	result.Repo.Name = mergeStringValue(
		base.Repo.Name,
		override.Repo.Name,
		fieldSchemaRepoName,
		overridePresence,
		resultPresence,
	)
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

func mergeMigration(
	base,
	override MigrationConfig,
	overridePresence configPresence,
	resultPresence *configPresence,
) MigrationConfig {
	result := base
	result.Dir = mergeStringValue(base.Dir, override.Dir, fieldMigrationDir, overridePresence, resultPresence)
	result.Format = mergeStringValue(
		base.Format,
		override.Format,
		fieldMigrationFormat,
		overridePresence,
		resultPresence,
	)
	result.RevisionsSchema = mergeStringValue(
		base.RevisionsSchema,
		override.RevisionsSchema,
		fieldMigrationRevisionsSchema,
		overridePresence,
		resultPresence,
	)
	result.RevisionsTable = mergeStringValue(
		base.RevisionsTable,
		override.RevisionsTable,
		fieldMigrationRevisionsTable,
		overridePresence,
		resultPresence,
	)
	result.Baseline = mergeStringValue(
		base.Baseline,
		override.Baseline,
		fieldMigrationBaseline,
		overridePresence,
		resultPresence,
	)
	result.RevisionFormat = mergeStringValue(
		base.RevisionFormat,
		override.RevisionFormat,
		fieldMigrationRevisionFormat,
		overridePresence,
		resultPresence,
	)
	result.LockTimeout = mergeStringValue(
		base.LockTimeout,
		override.LockTimeout,
		fieldMigrationLockTimeout,
		overridePresence,
		resultPresence,
	)
	result.StatementTimeout = mergeStringValue(
		base.StatementTimeout,
		override.StatementTimeout,
		fieldMigrationStatementTimeout,
		overridePresence,
		resultPresence,
	)
	result.ConnectTimeout = mergeStringValue(
		base.ConnectTimeout,
		override.ConnectTimeout,
		fieldMigrationConnectTimeout,
		overridePresence,
		resultPresence,
	)
	result.MigrationLockTimeout = mergeStringValue(
		base.MigrationLockTimeout,
		override.MigrationLockTimeout,
		fieldMigrationMigrationLock,
		overridePresence,
		resultPresence,
	)
	result.ExecOrder = mergeStringValue(
		base.ExecOrder,
		override.ExecOrder,
		fieldMigrationExecOrder,
		overridePresence,
		resultPresence,
	)
	result.TxMode = mergeStringValue(
		base.TxMode,
		override.TxMode,
		fieldMigrationTxMode,
		overridePresence,
		resultPresence,
	)
	result.PreUpHook = mergeStringValue(
		base.PreUpHook,
		override.PreUpHook,
		fieldMigrationPreUpHook,
		overridePresence,
		resultPresence,
	)
	result.PreDownHook = mergeStringValue(
		base.PreDownHook,
		override.PreDownHook,
		fieldMigrationPreDownHook,
		overridePresence,
		resultPresence,
	)
	result.PostgresDumpTo = mergeStringValue(
		base.PostgresDumpTo,
		override.PostgresDumpTo,
		fieldMigrationPostgresDumpTo,
		overridePresence,
		resultPresence,
	)
	result.MySQLDumpTo = mergeStringValue(
		base.MySQLDumpTo,
		override.MySQLDumpTo,
		fieldMigrationMySQLDumpTo,
		overridePresence,
		resultPresence,
	)
	result.Webhook = mergeStringValue(
		base.Webhook,
		override.Webhook,
		fieldMigrationWebhook,
		overridePresence,
		resultPresence,
	)
	return result
}

func mergeLint(
	base,
	override LintConfig,
	overridePresence configPresence,
	resultPresence *configPresence,
) LintConfig {
	result := base
	result.Latest = clonePointer(base.Latest)
	result.Dialect = mergeStringValue(
		base.Dialect,
		override.Dialect,
		fieldLintDialect,
		overridePresence,
		resultPresence,
	)
	result.DisabledRules = mergeStringSliceValue(
		base.DisabledRules,
		override.DisabledRules,
		fieldLintDisabledRules,
		overridePresence,
		resultPresence,
	)
	result.RuleConfigs = mergeLintRuleConfigs(
		base.RuleConfigs,
		override.RuleConfigs,
		overridePresence,
		resultPresence,
	)
	if overridePresence.has(fieldLintLatest) || override.Latest != nil {
		result.Latest = clonePointer(override.Latest)
		result.GitBase = ""
		result.GitDir = ""
		resultPresence.mark(fieldLintLatest)
		resultPresence.unmark(fieldLintGitBase)
		resultPresence.unmark(fieldLintGitDir)
	}
	if overridePresence.has(fieldLintGitBase) || override.GitBase != "" {
		result.GitBase = override.GitBase
		resultPresence.mark(fieldLintGitBase)
		if override.GitBase != "" {
			result.Latest = nil
			resultPresence.unmark(fieldLintLatest)
		}
	}
	result.GitDir = mergeStringValue(
		result.GitDir,
		override.GitDir,
		fieldLintGitDir,
		overridePresence,
		resultPresence,
	)
	return result
}

func mergeLintRuleConfigs(
	base,
	override map[string]LintRuleConfig,
	overridePresence configPresence,
	resultPresence *configPresence,
) map[string]LintRuleConfig {
	result := cloneLintRuleConfigs(base)
	if !overridePresence.has(fieldLintRuleConfigs) && len(override) == 0 {
		return result
	}
	resultPresence.mark(fieldLintRuleConfigs)
	if len(override) == 0 {
		resultPresence.removePrefix(lintRuleConfigFieldPrefix)
		return make(map[string]LintRuleConfig)
	}
	if result == nil {
		result = make(map[string]LintRuleConfig, len(override))
	}
	for code, config := range override {
		baseConfig := result[code]
		baseConfig.Severity = mergeStringValue(
			baseConfig.Severity,
			config.Severity,
			lintRuleSeverityField(code),
			overridePresence,
			resultPresence,
		)
		baseConfig.Exclude = mergeStringSliceValue(
			baseConfig.Exclude,
			config.Exclude,
			lintRuleExcludeField(code),
			overridePresence,
			resultPresence,
		)
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

func mergeFormat(
	base,
	override FormatConfig,
	overridePresence configPresence,
	resultPresence *configPresence,
) FormatConfig {
	result := base
	result.Migrate.Apply = mergeStringValue(
		base.Migrate.Apply,
		override.Migrate.Apply,
		fieldFormatMigrateApply,
		overridePresence,
		resultPresence,
	)
	result.Migrate.Diff = mergeStringValue(
		base.Migrate.Diff,
		override.Migrate.Diff,
		fieldFormatMigrateDiff,
		overridePresence,
		resultPresence,
	)
	result.Migrate.Lint = mergeStringValue(
		base.Migrate.Lint,
		override.Migrate.Lint,
		fieldFormatMigrateLint,
		overridePresence,
		resultPresence,
	)
	result.Migrate.Status = mergeStringValue(
		base.Migrate.Status,
		override.Migrate.Status,
		fieldFormatMigrateStatus,
		overridePresence,
		resultPresence,
	)
	result.Schema.Apply = mergeStringValue(
		base.Schema.Apply,
		override.Schema.Apply,
		fieldFormatSchemaApply,
		overridePresence,
		resultPresence,
	)
	result.Schema.Clean = mergeStringValue(
		base.Schema.Clean,
		override.Schema.Clean,
		fieldFormatSchemaClean,
		overridePresence,
		resultPresence,
	)
	result.Schema.Diff = mergeStringValue(
		base.Schema.Diff,
		override.Schema.Diff,
		fieldFormatSchemaDiff,
		overridePresence,
		resultPresence,
	)
	result.Schema.Inspect = mergeStringValue(
		base.Schema.Inspect,
		override.Schema.Inspect,
		fieldFormatSchemaInspect,
		overridePresence,
		resultPresence,
	)
	return result
}

func clonePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	return new(*value)
}

func mergeDiff(base, override DiffConfig) DiffConfig {
	result := base
	result.Skip.DropTable = mergeBool(result.Skip.DropTable, override.Skip.DropTable)
	result.Skip.DropColumn = mergeBool(result.Skip.DropColumn, override.Skip.DropColumn)
	result.Skip.DropIndex = mergeBool(result.Skip.DropIndex, override.Skip.DropIndex)
	result.Skip.DropEnum = mergeBool(result.Skip.DropEnum, override.Skip.DropEnum)
	result.Skip.DropSchema = mergeBool(result.Skip.DropSchema, override.Skip.DropSchema)
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
