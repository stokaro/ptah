// Package projectconfig loads Ptah project-level configuration from Ptah and
// Atlas project config files.
package projectconfig

import "slices"

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
	// Migration holds migration-directory and runtime settings.
	Migration MigrationConfig
	// Lint holds migration-lint settings.
	Lint LintConfig

	presence configPresence
}

type configPresence struct {
	schemaSources     bool
	schemas           bool
	exclude           bool
	lintDisabledRules bool
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
	Latest        *int
}

// Merge returns base overridden by non-zero values from override.
func Merge(base, override Config) Config {
	result := base
	if override.EnvName != "" {
		result.EnvName = override.EnvName
	}
	if override.DatabaseURL != "" {
		result.DatabaseURL = override.DatabaseURL
	}
	if override.DevURL != "" {
		result.DevURL = override.DevURL
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
	result.Migration = mergeMigration(result.Migration, override.Migration)
	result.Lint = mergeLint(result.Lint, override.Lint, override.presence)
	if override.presence.lintDisabledRules || len(override.Lint.DisabledRules) > 0 {
		result.presence.lintDisabledRules = true
	}
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
	if override.Latest != nil {
		latest := *override.Latest
		result.Latest = &latest
	}
	return result
}
