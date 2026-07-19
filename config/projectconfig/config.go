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
	// EnvName is the selected Atlas env name, when the source had one.
	EnvName string
	// DatabaseURL is the target database URL used by migration commands.
	DatabaseURL string
	// DevURL is the disposable dev/shadow database URL.
	DevURL string
	// Exclude lists schema patterns excluded by project config.
	Exclude []string
	// Migration holds migration-directory and runtime settings.
	Migration MigrationConfig
	// Lint holds migration-lint settings.
	Lint LintConfig
}

// MigrationConfig is the migration section of the project config IR.
type MigrationConfig struct {
	Dir             string
	Format          string
	RevisionsSchema string
	RevisionFormat  string
	LockTimeout     string
	ExecOrder       string
}

// LintConfig is the lint section of the project config IR.
type LintConfig struct {
	Latest *int
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
	if len(override.Exclude) > 0 {
		result.Exclude = slices.Clone(override.Exclude)
	}
	result.Migration = mergeMigration(result.Migration, override.Migration)
	result.Lint = mergeLint(result.Lint, override.Lint)
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
	if override.RevisionFormat != "" {
		result.RevisionFormat = override.RevisionFormat
	}
	if override.LockTimeout != "" {
		result.LockTimeout = override.LockTimeout
	}
	if override.ExecOrder != "" {
		result.ExecOrder = override.ExecOrder
	}
	return result
}

func mergeLint(base, override LintConfig) LintConfig {
	result := base
	if override.Latest != nil {
		latest := *override.Latest
		result.Latest = &latest
	}
	return result
}
