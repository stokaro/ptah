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

	"ptah.run/internal/lintdialect"
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
//	server-version: "17"
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
	// ServerVersion is the server the migrations will run against, in the same
	// spelling --server-version takes on every other offline command: "17",
	// "8.4.6", "10.11.6-MariaDB". The --server-version flag overrides it, and
	// it overrides what a dev database reports about itself -- what an
	// operator wrote down is what they meant, and a dev database is often a
	// different release from the one a migration will meet. Empty with no flag
	// and no dev database leaves the run planning against the dialect default.
	//
	// It is validated against Dialect, so a version naming another product is
	// refused here rather than resolving to the default and quietly changing
	// which rules can fire.
	ServerVersion string `yaml:"server-version,omitempty"`
	// DisabledRules lists rule codes or family prefixes to skip; merged
	// with --disable flags.
	DisabledRules []string `yaml:"disabled-rules"`
	// Rules carries per-rule severity and path-scope overrides.
	Rules map[string]RuleConfig `yaml:"rules"`
	// Naming is the naming convention the six NM rules enforce; absent, they
	// stay silent. See [NamingConfig].
	Naming *NamingConfig `yaml:"naming,omitempty"`
	// Gate widens what the apply-time gate blocks on. See [GateConfig].
	Gate *GateConfig `yaml:"gate,omitempty"`
	// Online selects the online mode: every statement the analysis cannot
	// prove takes no lock conflicting with reads and writes is reported at
	// error severity, and `ptah migrations up` refuses the migration.
	//
	// The only accepted value is `require`. It is a word rather than a boolean
	// because the mode is a promise about the SQL and not a preference, and
	// because a second level -- report without refusing -- is the obvious next
	// value and would have no room in a boolean.
	Online string `yaml:"online,omitempty"`
}

// OnlineRequire is the one accepted value of the `online` key.
const OnlineRequire = "require"

// RequiresOnline reports whether the configuration selected the online mode.
func (c *Config) RequiresOnline() bool {
	return c != nil && c.Online == OnlineRequire
}

// GateConfig is the `gate` section of .ptah-lint.yaml: the rule families
// whose error-severity findings refuse `ptah migrations up`, beyond the DS
// family the gate always blocks on.
//
// Lint and apply are deliberately separate: `ptah migrations lint` reports
// every rule and exits at a threshold, while the apply gate stops only on
// data-safety findings, because a locking hazard or a naming convention is
// something to review, not something the migrator should refuse to run.
// A project that wants a finding to stop the deploy says so here, by
// family, and gives the rule an error severity under `rules`; the gate
// then runs that family and refuses on it. The DS family needs no entry.
type GateConfig struct {
	Families []string `yaml:"families"`
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
	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("failed to parse lint config %s: %w", name, err)
	}
	return &cfg, nil
}

// validateConfig rejects an unusable configuration and rewrites what it keeps
// into the spelling the rest of the pipeline compares against. Canonicalizing
// here rather than at each reader is what lets a policy name an engine by any
// documented alias: everything downstream matches Rule.Dialects and the lexer
// mode by exact string comparison, so an alias left in place would select no
// dialect-specific rule at all instead of failing.
func validateConfig(cfg *Config) error {
	canonical, ok := lintdialect.Canonical(cfg.Dialect)
	if !ok {
		return fmt.Errorf("unsupported lint dialect %q: expected %s", cfg.Dialect, lintdialect.Expected)
	}
	cfg.Dialect = canonical
	// Only where the configuration names its own dialect. A policy may leave
	// the dialect to the command line or to a dev database and still pin the
	// version here, and refusing that pair would refuse a configuration that
	// is complete by the time it is used. What wins is resolved once, against
	// the effective dialect, by the caller that knows it.
	if cfg.Dialect != "" {
		if _, err := ResolveTarget(cfg.Dialect, cfg.ServerVersion); err != nil {
			return err
		}
	}
	if err := validateRuleSelectors(cfg.DisabledRules); err != nil {
		return err
	}
	if _, err := compileNamingConfig(cfg.Naming); err != nil {
		return err
	}
	if err := validateGateConfig(cfg.Gate); err != nil {
		return err
	}
	if err := validateOnlineConfig(cfg); err != nil {
		return err
	}
	if err := validateOnlineSelectors(cfg); err != nil {
		return err
	}
	return validateRuleConfigs(cfg.Rules)
}

// validateOnlineConfig refuses a value the mode does not have and a dialect it
// cannot honestly cover.
//
// The dialect refusal is the load-bearing half. A mode that ran on SQLite and
// found nothing would be reporting that every change is online on an engine
// that rebuilds the table for most of them, and a guarantee that is wrong in
// silence is worse than no guarantee. The same applies to the engines whose
// online story is real but unmeasured here: CockroachDB, YugabyteDB and
// Spanner apply schema changes online by design, and saying so needs its own
// measurement rather than PostgreSQL's; SQL Server and Oracle answer
// differently by edition.
func validateOnlineConfig(cfg *Config) error {
	switch cfg.Online {
	case "", OnlineRequire:
	default:
		return fmt.Errorf("online: unsupported value %q (only %q is supported)", cfg.Online, OnlineRequire)
	}
	if !cfg.RequiresOnline() || cfg.Dialect == "" {
		return nil
	}
	if err := ValidateOnlineDialect(cfg.Dialect); err != nil {
		return fmt.Errorf("online: %w", err)
	}
	return nil
}

// validateOnlineSelectors refuses a policy that requires the mode and then
// takes its findings away.
//
// Adding ON to the gate is not enough on its own: the same file can disable
// the family, drop its severity below blocking, or exclude the paths it would
// have fired on, and each of those turns a stated guarantee into advice
// without saying so. A policy that wants the findings reported and not blocked
// is a policy that does not require the mode.
func validateOnlineSelectors(cfg *Config) error {
	if !cfg.RequiresOnline() {
		return nil
	}
	for _, selector := range cfg.DisabledRules {
		if selectorTouchesOnline(selector) {
			return fmt.Errorf(
				"online: disabled-rules names %q while online: require is set, which would report "+
					"nothing and refuse nothing", selector)
		}
	}
	for _, code := range slices.Sorted(maps.Keys(cfg.Rules)) {
		if !selectorTouchesOnline(code) {
			continue
		}
		rule := cfg.Rules[code]
		if rule.Severity != "" && Severity(rule.Severity) != SeverityError {
			return fmt.Errorf(
				"online: rules.%s sets severity %q while online: require is set; the mode's findings "+
					"refuse the apply, so a lower severity would make the guarantee advisory",
				code, rule.Severity)
		}
		if len(rule.Exclude) > 0 {
			return fmt.Errorf(
				"online: rules.%s excludes paths while online: require is set; a migration the mode "+
					"does not read is one it cannot prove", code)
		}
	}
	return nil
}

// selectorTouchesOnline reports whether a rule selector names the ON family or
// a rule in it.
func selectorTouchesOnline(selector string) bool {
	return strings.HasPrefix(selector, OnlineFamily)
}

// validateGateConfig refuses a gate that names no family and a family no
// registered rule belongs to, so a typo cannot read as a widened gate.
func validateGateConfig(gate *GateConfig) error {
	if gate == nil {
		return nil
	}
	if len(gate.Families) == 0 {
		return fmt.Errorf("gate: families lists nothing, so the section would widen nothing")
	}
	for _, family := range gate.Families {
		if !isCanonicalRuleCode(family) || !selectorMatchesRule(family, Rules()) {
			return fmt.Errorf("gate: family %q matches no registered rule", family)
		}
	}
	return nil
}

// GateFamilies returns the families a policy's gate section names, with the
// DS family the gate always blocks on first and no family twice.
func (c *Config) GateFamilies() []string {
	families := []string{"DS"}
	if c.RequiresOnline() {
		// The mode is a promise about what the apply does, so its findings
		// refuse the apply. A policy selecting it and then having to name the
		// family under `gate` as well would be two ways to say one thing, and
		// the one that was left out would be a mode that reported and ran.
		families = append(families, OnlineFamily)
	}
	if c == nil || c.Gate == nil {
		return families
	}
	for _, family := range c.Gate.Families {
		if !slices.Contains(families, family) {
			families = append(families, family)
		}
	}
	return families
}

func validateRuleConfigs(configs map[string]RuleConfig) error {
	for _, code := range slices.Sorted(maps.Keys(configs)) {
		rule := configs[code]
		if !isCanonicalRuleCode(code) {
			if rule.Declares() {
				// A declared rule's code is not decoration: it is what reports
				// print, what `--disable` selects, and what a `nolint`
				// directive names. A second naming shape would make a family
				// prefix such as `DS` ambiguous against a rule that merely
				// starts with those letters.
				return fmt.Errorf(
					"lint rule %q: a declared rule needs a code in the same form as every other "+
						"rule -- uppercase ASCII letters and digits, such as %q -- because the code "+
						"is what findings print and what --disable and nolint select; "+
						"put the readable name in `title`",
					code, suggestedRuleCode(code))
			}
			return invalidRuleSelectorError(code)
		}
		switch rule.Severity {
		case "", SeverityInfo, SeverityWarning, SeverityError:
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

// selectorMatchesRule reports whether a configured selector reaches a rule.
//
// The Atlas spelling counts. A user reading the Atlas documentation writes
// PG301, and refusing it as unknown told them their code does not exist when
// what it does not have is a rule of its own (stokaro/ptah#1631).
func selectorMatchesRule(selector string, rules []Rule) bool {
	for _, candidate := range expandAtlasCodeSelectors([]string{selector}) {
		for _, rule := range rules {
			if strings.HasPrefix(rule.Code, candidate) {
				return true
			}
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
