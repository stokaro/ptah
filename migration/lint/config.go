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

	"ptah.run/core/lintext"
	"ptah.run/core/schemaext"
	"ptah.run/internal/lintdialect"
	"ptah.run/internal/lintnames"
	"ptah.run/internal/servertarget"
	"ptah.run/internal/sqllint"
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
//	  - file-form/file-name
//	  - MY
//	rules:
//	  data-dependent/alter-column-type:
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

// GateConfig is the `gate` section of .ptah-lint.yaml: the rules, by name or
// category, whose error-severity findings refuse `ptah migrations up`, beyond
// the data-safety rules the gate always blocks on.
//
// Lint and apply are deliberately separate: `ptah migrations lint` reports
// every rule and exits at a threshold, while the apply gate stops only on
// data-safety findings, because a locking hazard or a naming convention is
// something to review, not something the migrator should refuse to run.
// A project that wants a finding to stop the deploy says so here, by rule
// name or category, and gives the rule an error severity under `rules`; the
// gate then runs those rules and refuses on them. The data-safety rules need
// no entry.
type GateConfig struct {
	// Rules are the rule names and categories the gate blocks on.
	Rules []string `yaml:"rules"`
}

// LoadConfig reads an explicit lint configuration file. Missing, unreadable,
// malformed, and unknown configuration fields are errors, as are unsupported
// dialects, invalid rule exclusion globs and a gate family no rule of runtime
// belongs to. A nil runtime is an error.
func LoadConfig(runtime lintext.Runtime, configPath string) (*Config, error) {
	rules, err := Rules(runtime)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read lint config %s: %w", configPath, err)
	}
	cfg, err := parseConfig(raw, configPath, rules, servedBy(runtime))
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadConfigFS reads a conventional lint configuration from fsys. A missing
// file is not an error; the same strict validation as [LoadConfig] applies to
// a present file.
func LoadConfigFS(runtime lintext.Runtime, fsys fs.FS, name string) (*Config, error) {
	rules, err := Rules(runtime)
	if err != nil {
		return nil, err
	}
	raw, err := fs.ReadFile(fsys, name)
	if errors.Is(err, fs.ErrNotExist) {
		return &Config{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read lint config %s: %w", name, err)
	}
	return parseConfig(raw, name, rules, servedBy(runtime))
}

func parseConfig(raw []byte, name string, rules []Rule, served schemaext.TargetResolver) (*Config, error) {
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
	if err := validateConfig(&cfg, rules, served); err != nil {
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
//
// A dialect the runtime serves through an executable provider is accepted
// under the target name it selects. A server version beside it must name a
// release line its provider declares facts for, and is refused rather than
// dropped where it names none.
func validateConfig(cfg *Config, rules []Rule, served schemaext.TargetResolver) error {
	canonical, ok := lintdialect.CanonicalIn(cfg.Dialect, served)
	if !ok {
		return fmt.Errorf("unsupported lint dialect %q: expected %s", cfg.Dialect, lintdialect.Expected)
	}
	_, builtIn := lintdialect.Canonical(canonical)
	if !builtIn && cfg.ServerVersion != "" {
		if _, err := servertarget.Resolve(canonical, cfg.ServerVersion); err != nil {
			return fmt.Errorf("lint server-version: %w", err)
		}
	}
	cfg.Dialect = canonical
	// Only where the configuration names its own dialect. A policy may leave
	// the dialect to the command line or to a dev database and still pin the
	// version here, and refusing that pair would refuse a configuration that
	// is complete by the time it is used. What wins is resolved once, against
	// the effective dialect, by the caller that knows it.
	if cfg.Dialect != "" && builtIn {
		if _, err := ResolveTarget(cfg.Dialect, cfg.ServerVersion); err != nil {
			return err
		}
	}
	if err := validateRuleSelectors(cfg.DisabledRules); err != nil {
		return err
	}
	if err := validateConfigNames(cfg, rules); err != nil {
		return err
	}
	if _, err := compileNamingConfig(cfg.Naming); err != nil {
		return err
	}
	if err := validateGateConfig(cfg.Gate, rules); err != nil {
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

// validateConfigNames refuses a `disabled-rules` entry that selects no rule
// the runtime registers or the file declares, and a `rules` key that selects no
// rule of either linter: `ptah sql lint` reads the same section, so a name it
// reports is a name the file may carry.
func validateConfigNames(cfg *Config, rules []Rule) error {
	declared := make([]string, 0, len(cfg.Rules))
	for name, rule := range cfg.Rules {
		if rule.Declares() {
			declared = append(declared, name)
		}
	}
	migration := knownNames(rules)
	disabled := lintnames.NewSet("migration lint", append(migration.Names(), declared...))
	for _, selector := range cfg.DisabledRules {
		if _, err := disabled.Resolve(selector); err != nil && selector != "" {
			return fmt.Errorf("rule selector %w", err)
		}
	}
	configured := lintnames.NewSet("lint", append(disabled.Names(), sqllint.CatalogIDs()...))
	for _, key := range slices.Sorted(maps.Keys(cfg.Rules)) {
		if cfg.Rules[key].Declares() {
			continue
		}
		if _, err := configured.Resolve(key); err != nil {
			return fmt.Errorf("lint rules key %w", err)
		}
	}
	return nil
}

// validateOnlineConfig refuses a value the mode does not have and a dialect it
// cannot honestly cover.
//
// The dialect refusal is the load-bearing half. A mode that ran on SQLite and
// found nothing would be reporting that every change is online on an engine
// that rebuilds the table for most of them, and a guarantee that is wrong in
// silence is worse than no guarantee. The same applies to the engines whose
// online story is real but unmeasured here: CockroachDB and YugabyteDB apply
// schema changes online by design, and saying so needs its own measurement
// rather than PostgreSQL's; SQL Server and Oracle answer
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

// selectorTouchesOnline reports whether a rule selector names the online
// category or a rule in it.
func selectorTouchesOnline(selector string) bool {
	return strings.TrimSuffix(selector, "/") == OnlineCategory || lintnames.CategoryOf(selector) == OnlineCategory
}

// validateGateConfig refuses a gate that names no rule and a rule name or
// category none of rules holds, so a typo cannot read as a widened gate.
func validateGateConfig(gate *GateConfig, rules []Rule) error {
	if gate == nil {
		return nil
	}
	if len(gate.Rules) == 0 {
		return fmt.Errorf("gate: rules lists nothing, so the section would widen nothing")
	}
	known := knownNames(rules)
	for _, selector := range gate.Rules {
		if err := known.ValidateSelector(selector); err != nil {
			return fmt.Errorf("gate: rule %w", err)
		}
	}
	return nil
}

// GateRules returns the rules and categories a policy's gate section names,
// preceded by the data-safety rules the gate always blocks on, with the online
// category after them when the policy requires the online mode, and nothing
// twice.
func (c *Config) GateRules() []string {
	rules := slices.Clone(DefaultGateRules)
	if c.RequiresOnline() {
		// The mode is a promise about what the apply does, so its findings
		// refuse the apply. A policy selecting it and then having to name the
		// category under `gate` as well would be two ways to say one thing, and
		// the one that was left out would be a mode that reported and ran.
		rules = append(rules, OnlineCategory)
	}
	if c == nil || c.Gate == nil {
		return rules
	}
	for _, selector := range c.Gate.Rules {
		if !slices.Contains(rules, selector) {
			rules = append(rules, selector)
		}
	}
	return rules
}

// DefaultGateRules are the rules the apply gate always blocks on: the
// data-safety rules, which destroy or weaken what a database holds.
//
// They are listed by name because no category holds exactly these. The
// destructive category also holds rules the gate leaves to a lint pass, such
// as a dropped YDB resource pool, and a category that also held the
// advisory rules would widen the stop.
var DefaultGateRules = []string{
	"destructive/drop-table",
	"destructive/drop-column",
	"dependency/drop-column-in-use",
	"data-dependent/alter-column-type",
	"integrity/drop-not-null",
	"integrity/drop-untyped-constraint",
	"destructive/remove-enum-value",
	"destructive/drop-object",
	"destructive/truncate-table",
	"security/disable-row-level-security",
	"security/no-force-row-level-security",
}

func validateRuleConfigs(configs map[string]RuleConfig) error {
	for _, code := range slices.Sorted(maps.Keys(configs)) {
		rule := configs[code]
		if err := validateRuleConfigKey(code, rule, configs); err != nil {
			return err
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

// validateRuleConfigKey refuses a `rules:` key that is not a rule name or
// category, a key spelled as a rule code, a declared rule whose name is not
// well formed, and a category written in both of its spellings.
func validateRuleConfigKey(key string, rule RuleConfig, configs map[string]RuleConfig) error {
	if rule.Declares() {
		// A declared rule's name is not decoration: it is what reports print,
		// what `--disable` selects, and what a `nolint` directive names, so it
		// has the same form as every other rule's.
		if lintnames.IsCode(key) {
			return lintnames.RefuseCode(key)
		}
		if !lintnames.WellFormed(key) {
			return fmt.Errorf(
				"lint rule %q: a declared rule needs a name in the same form as every other rule -- "+
					"a category and a rule joined by a slash, such as %q -- because the name "+
					"is what findings print and what --disable and nolint select; "+
					"put a longer description in `title`",
				key, exampleDeclaredRuleName)
		}
		return validateDeclaredCategory(key)
	}
	if err := lintnames.ValidateGrammar(key); err != nil {
		return fmt.Errorf("lint rules key %w", err)
	}
	if category, found := strings.CutSuffix(key, "/"); found {
		if _, twice := configs[category]; twice {
			return fmt.Errorf("lint rules %q and %q name one category; keep one of them", category, key)
		}
	}
	return nil
}

// exampleDeclaredRuleName is the name refusals show for a declared rule.
const exampleDeclaredRuleName = "design/no-select-star"

// validateDeclaredCategory refuses a declared rule under a category Ptah
// does not declare.
func validateDeclaredCategory(name string) error {
	if _, declared := lintnames.CategoryFor(lintnames.CategoryOf(name)); !declared {
		return fmt.Errorf("lint rule %q: %q is not a rule category; the categories are %s",
			name, lintnames.CategoryOf(name), strings.Join(lintnames.CategoryNames(), ", "))
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

// validateConfiguredRuleSelectors refuses a configured selector that reaches
// no rule the run registers: a name or category the run has no rule for, such
// as a provider's rule linted without the provider, is refused rather than
// read as a rule turned off.
func validateConfiguredRuleSelectors(rules []Rule, opts Options) error {
	known := knownNames(rules)
	for _, selector := range opts.Disabled {
		if selector == "" {
			continue
		}
		if _, err := known.Resolve(selector); err != nil {
			return fmt.Errorf("rule selector %w", err)
		}
	}
	configured := lintnames.NewSet("lint", append(known.Names(), sqllint.CatalogIDs()...))
	for _, key := range slices.Sorted(maps.Keys(opts.RuleConfigs)) {
		if _, err := configured.Resolve(key); err != nil {
			return fmt.Errorf("rule selector %w", err)
		}
	}
	return nil
}

// validateRuleSelectors refuses a selector that is not a rule name or
// category, judged by form alone, and one spelled as a rule code. A
// misspelled name is refused here or against the rules of the run, rather
// than matched against nothing, which would read as a rule turned off.
func validateRuleSelectors(selectors []string) error {
	for _, selector := range selectors {
		if err := lintnames.ValidateGrammar(selector); err != nil {
			return fmt.Errorf("rule selector %w", err)
		}
	}
	return nil
}

// servedBy is the target resolver of a lint runtime that serves targets, such
// as an engine runtime with an executable provider's engines, and nil for one
// that does not.
func servedBy(runtime lintext.Runtime) schemaext.TargetResolver {
	served, _ := runtime.(schemaext.TargetResolver)
	return served
}
