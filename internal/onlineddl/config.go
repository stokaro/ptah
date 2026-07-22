// Package onlineddl routes ALTER TABLE migration statements through external
// online-DDL tools — gh-ost or pt-online-schema-change — so large MySQL and
// MariaDB tables can be altered without holding a long table lock (issue
// #173).
//
// Routing happens two ways:
//
//   - a per-migration directive `-- +ptah online_ddl_tool=ghost` (or pt-osc,
//     or none) forces the choice for every ALTER TABLE in that migration;
//   - a ptah.yaml config with online_ddl.tool and online_ddl.threshold_rows
//     routes any ALTER TABLE automatically when the target table's estimated
//     row count reaches the threshold.
//
// Fallback behavior depends on intent: explicit tool directives fail closed by
// default, while automatic threshold routing preserves the original warning +
// plain ALTER fallback unless online_ddl.fallback overrides it.
package onlineddl

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"go.yaml.in/yaml/v3"
)

// ConfigFileName is the conventional ptah configuration file looked up in the
// working directory when no explicit path is given.
const ConfigFileName = "ptah.yaml"

// Canonical tool names accepted in configuration and directives.
const (
	// ToolGhost routes ALTERs through GitHub's gh-ost.
	ToolGhost = "ghost"
	// ToolPTOSC routes ALTERs through Percona's pt-online-schema-change.
	ToolPTOSC = "pt-osc"
)

// Fallback policies accepted in configuration and directives.
const (
	// FallbackError aborts instead of letting a routed statement degrade to a
	// plain ALTER TABLE.
	FallbackError = "error"
	// FallbackPlain falls through so the migrator executes the plain ALTER
	// TABLE on its own connection.
	FallbackPlain = "plain"
)

// Config is the online_ddl section of ptah.yaml.
type Config struct {
	// Tool selects the online-DDL tool used for automatic routing: "ghost"
	// or "pt-osc". Empty disables automatic routing (per-migration
	// directives keep working).
	Tool string `yaml:"tool"`
	// ThresholdRows enables automatic routing: any ALTER TABLE on a table
	// whose estimated row count (information_schema) is at or above this
	// value goes through Tool. Zero disables automatic routing.
	ThresholdRows int64 `yaml:"threshold_rows"`
	// Args are extra arguments appended to every tool invocation — e.g.
	// gh-ost's --allow-on-master or --max-load, pt-osc's --max-lag.
	Args []string `yaml:"args"`
	// Fallback controls what happens when a selected online-DDL path cannot be
	// used: "error" aborts, "plain" runs the plain ALTER. Empty uses the
	// source default.
	Fallback string `yaml:"fallback"`
}

// ptahConfig is the top-level ptah.yaml envelope.
type ptahConfig struct {
	ptahSettings `yaml:",inline"`
	Env          map[string]ptahSettings `yaml:"env"`
}

type ptahSettings struct {
	URL       string        `yaml:"url"`
	Dev       string        `yaml:"dev"`
	Schemas   []string      `yaml:"schemas"`
	Exclude   []string      `yaml:"exclude"`
	Migration yamlMigration `yaml:"migration"`
	Lint      yamlLint      `yaml:"lint"`
	Migrate   yamlMigrate   `yaml:"migrate"`
	OnlineDDL yamlOnlineDDL `yaml:"online_ddl"`
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
	Dialect       string   `yaml:"dialect"`
	DisabledRules []string `yaml:"disabled-rules"`
	Latest        *int     `yaml:"latest"`
}

type yamlMigrate struct {
	Generate yamlMigrateGenerate `yaml:"generate"`
}

type yamlMigrateGenerate struct {
	ShadowDatabaseURL string `yaml:"shadow_db"`
}

type yamlOnlineDDL struct {
	Tool          *string   `yaml:"tool"`
	ThresholdRows *int64    `yaml:"threshold_rows"`
	Args          *[]string `yaml:"args"`
	Fallback      *string   `yaml:"fallback"`
}

// Enabled reports whether automatic threshold routing is configured.
// Directive-based routing works regardless.
func (c Config) Enabled() bool {
	return c.Tool != "" && c.ThresholdRows > 0
}

// Validate checks the configuration values.
func (c Config) Validate() error {
	switch c.Tool {
	case "", ToolGhost, ToolPTOSC:
	default:
		return fmt.Errorf("unknown online_ddl tool %q: expected %s or %s", c.Tool, ToolGhost, ToolPTOSC)
	}
	if c.ThresholdRows < 0 {
		return fmt.Errorf("online_ddl threshold_rows must not be negative, got %d", c.ThresholdRows)
	}
	if c.ThresholdRows > 0 && c.Tool == "" {
		return fmt.Errorf("online_ddl threshold_rows is set but no tool is configured")
	}
	if c.Fallback != "" {
		if err := validateConfigFallback(c.Fallback); err != nil {
			return err
		}
	}
	return nil
}

// LoadConfig reads the top-level online_ddl section of a ptah.yaml file. A
// missing file at the conventional location is not an error — it yields a zero
// Config (automatic routing disabled); an unreadable, malformed or invalid
// file is.
func LoadConfig(path string) (*Config, error) {
	return LoadConfigForEnv(path, "")
}

// LoadConfigForEnv reads the online_ddl section of a ptah.yaml file after
// applying the selected project environment, when present.
func LoadConfigForEnv(path, envName string) (*Config, error) {
	raw, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return &Config{}, nil
	case err != nil:
		return nil, fmt.Errorf("failed to read ptah config %s: %w", path, err)
	}

	var cfg ptahConfig
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to parse ptah config %s: %w", path, err)
	}
	onlineDDL := selectOnlineDDLConfig(cfg, envName)
	if err := onlineDDL.Validate(); err != nil {
		return nil, fmt.Errorf("invalid online_ddl config in %s: %w", path, err)
	}
	return &onlineDDL, nil
}

func selectOnlineDDLConfig(cfg ptahConfig, envName string) Config {
	base := cfg.OnlineDDL.config()
	if envName != "" {
		if env, ok := cfg.Env[envName]; ok {
			return mergeOnlineDDL(base, env.OnlineDDL)
		}
		return base
	}
	if len(cfg.Env) != 1 {
		return base
	}
	for _, env := range cfg.Env {
		return mergeOnlineDDL(base, env.OnlineDDL)
	}
	return base
}

func (c yamlOnlineDDL) config() Config {
	var cfg Config
	if c.Tool != nil {
		cfg.Tool = *c.Tool
	}
	if c.ThresholdRows != nil {
		cfg.ThresholdRows = *c.ThresholdRows
	}
	if c.Args != nil {
		cfg.Args = append([]string{}, (*c.Args)...)
	}
	if c.Fallback != nil {
		cfg.Fallback = *c.Fallback
	}
	return cfg
}

func mergeOnlineDDL(base Config, override yamlOnlineDDL) Config {
	result := base
	if override.Tool != nil {
		result.Tool = *override.Tool
		if *override.Tool == "" && override.ThresholdRows == nil {
			result.ThresholdRows = 0
		}
	}
	if override.ThresholdRows != nil {
		result.ThresholdRows = *override.ThresholdRows
	}
	if override.Args != nil {
		result.Args = append([]string{}, (*override.Args)...)
	}
	if override.Fallback != nil {
		result.Fallback = *override.Fallback
	}
	return result
}
