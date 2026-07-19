package lint

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	yaml "go.yaml.in/yaml/v3"
)

// ConfigFileName is the conventional per-project lint configuration file,
// looked up inside the linted migrations directory when no explicit config
// path is given.
const ConfigFileName = ".ptah-lint.yaml"

// Config is the on-disk lint configuration.
//
// Example .ptah-lint.yaml:
//
//	dialect: postgres
//	disabled-rules:
//	  - MF103
//	  - MY
//	rules:
//	  DS103:
//	    severity: warning
//	    exclude:
//	      - legacy/**
type Config struct {
	// Dialect is the target dialect used to gate dialect-specific rules;
	// the --dialect flag overrides it.
	Dialect string `yaml:"dialect"`
	// DisabledRules lists rule codes or family prefixes to skip; merged
	// with --disable flags.
	DisabledRules []string `yaml:"disabled-rules"`
	// Rules carries per-rule severity and path-scope overrides.
	Rules map[string]RuleConfig `yaml:"rules"`
}

// LoadConfig reads a lint configuration file. A missing file at the
// conventional location is not an error — callers get an empty config — but
// an unreadable or malformed file is.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return &Config{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read lint config %s: %w", path, err)
	}
	cfg, err := parseConfig(raw, path)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadConfigFS reads a lint configuration from fsys. A missing file is not an
// error, matching LoadConfig's conventional-file behavior.
func LoadConfigFS(fsys fs.FS, name string) (*Config, error) {
	raw, err := fs.ReadFile(fsys, name)
	if errors.Is(err, fs.ErrNotExist) {
		return &Config{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read lint config %s: %w", name, err)
	}
	return parseConfig(raw, name)
}

func parseConfig(raw []byte, name string) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse lint config %s: %w", name, err)
	}
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse lint config %s: %w", name, err)
	}
	return &cfg, nil
}

func validateConfig(cfg Config) error {
	for code, rule := range cfg.Rules {
		switch rule.Severity {
		case "", SeverityWarning, SeverityError:
		default:
			return fmt.Errorf("rule %s has unsupported severity %q", code, rule.Severity)
		}
	}
	return nil
}
