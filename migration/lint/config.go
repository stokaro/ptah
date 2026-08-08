package lint

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path"
	"slices"
	"strings"

	yaml "go.yaml.in/yaml/v3"

	"go.5x5.cz/ptah/internal/lintdialect"
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

// LoadConfig reads an explicit lint configuration file. Missing, unreadable,
// malformed, and unknown configuration fields are errors, as are unsupported
// dialects and invalid rule exclusion globs.
func LoadConfig(configPath string) (*Config, error) {
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read lint config %s: %w", configPath, err)
	}
	cfg, err := parseConfig(raw, configPath)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadConfigFS reads a conventional lint configuration from fsys. A missing
// file is not an error; the same strict validation as [LoadConfig] applies to
// a present file.
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
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	err := decoder.Decode(&cfg)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to parse lint config %s: %w", name, err)
	}
	if err == nil {
		var trailing any
		err = decoder.Decode(&trailing)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("failed to parse lint config %s: %w", name, err)
		}
		if err == nil {
			return nil, fmt.Errorf("failed to parse lint config %s: multiple YAML documents are not supported", name)
		}
	}
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse lint config %s: %w", name, err)
	}
	return &cfg, nil
}

func validateConfig(cfg Config) error {
	if !lintdialect.Valid(cfg.Dialect) {
		return fmt.Errorf("unsupported lint dialect %q: expected %s", cfg.Dialect, lintdialect.Expected)
	}
	if err := validateRuleSelectors(cfg.DisabledRules); err != nil {
		return err
	}
	return validateRuleConfigs(cfg.Rules)
}

func validateRuleConfigs(configs map[string]RuleConfig) error {
	for _, code := range slices.Sorted(maps.Keys(configs)) {
		rule := configs[code]
		if !isCanonicalRuleCode(code) {
			return invalidRuleSelectorError(code)
		}
		switch rule.Severity {
		case "", SeverityWarning, SeverityError:
		default:
			return fmt.Errorf("rule %s has unsupported severity %q", code, rule.Severity)
		}
		for _, pattern := range rule.Exclude {
			if err := validateExcludePattern(pattern); err != nil {
				return fmt.Errorf("rule %s has invalid exclude pattern %q: %w", code, pattern, err)
			}
		}
	}
	return nil
}

func validateExcludePattern(pattern string) error {
	if strings.TrimSpace(pattern) == "" {
		return errors.New("pattern must not be empty")
	}
	if strings.TrimSpace(pattern) != pattern {
		return errors.New("pattern must not contain surrounding whitespace")
	}
	if path.Clean(pattern) != pattern {
		return errors.New("pattern must be a normalized slash-separated path")
	}
	segments := strings.Split(pattern, "/")
	if slices.Contains(segments, ".") || slices.Contains(segments, "..") {
		return errors.New("pattern must not contain . or .. path segments")
	}
	_, err := path.Match(pattern, "")
	return err
}

func validateConfiguredRuleSelectors(rules []Rule, opts Options) error {
	selectors := append(slices.Sorted(maps.Keys(opts.RuleConfigs)), opts.Disabled...)
	for _, selector := range selectors {
		if selector != "" && !selectorMatchesRule(selector, rules) {
			return fmt.Errorf("rule selector %q does not match any registered rule", selector)
		}
	}
	return nil
}

func selectorMatchesRule(selector string, rules []Rule) bool {
	for _, rule := range rules {
		if strings.HasPrefix(rule.Code, selector) {
			return true
		}
	}
	return false
}

func validateRuleSelectors(selectors []string) error {
	for _, selector := range selectors {
		if selector != "" && !isCanonicalRuleCode(selector) {
			return invalidRuleSelectorError(selector)
		}
	}
	return nil
}

func invalidRuleSelectorError(selector string) error {
	return fmt.Errorf("rule selector %q must start with an uppercase ASCII letter and contain only uppercase ASCII letters and digits", selector)
}
