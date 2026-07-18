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

// ptahConfig is the ptah.yaml envelope.
type ptahConfig struct {
	OnlineDDL Config `yaml:"online_ddl"`
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

// LoadConfig reads the online_ddl section of a ptah.yaml file. A missing file
// at the conventional location is not an error — it yields a zero Config
// (automatic routing disabled); an unreadable, malformed or invalid file is.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return &Config{}, nil
	case err != nil:
		return nil, fmt.Errorf("failed to read ptah config %s: %w", path, err)
	}

	var cfg ptahConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse ptah config %s: %w", path, err)
	}
	if err := cfg.OnlineDDL.Validate(); err != nil {
		return nil, fmt.Errorf("invalid online_ddl config in %s: %w", path, err)
	}
	return &cfg.OnlineDDL, nil
}
