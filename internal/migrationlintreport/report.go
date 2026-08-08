// Package migrationlintreport builds and renders migration lint reports.
package migrationlintreport

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/lintdialect"
	"go.5x5.cz/ptah/internal/migrationreplay"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/internal/schemaselection"
	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/risk"
)

const (
	// FormatText renders a human-readable lint report.
	FormatText = "text"
	// FormatJSON renders a structured JSON lint report.
	FormatJSON = "json"
	// FormatGitHubActions renders GitHub Actions workflow annotations.
	FormatGitHubActions = "github-actions"
	// FormatSARIF renders a SARIF 2.1.0 report.
	FormatSARIF = "sarif"

	// FailOnError fails when any error-severity finding is present.
	FailOnError = "error"
	// FailOnAny fails when any lint finding is present.
	FailOnAny = "any"
	// FailOnNone never fails because of lint findings.
	FailOnNone = "none"
)

// Report is the structured result produced by the migration linter.
type Report struct {
	Failed           bool           `json:"failed"`
	FailureThreshold string         `json:"failure_threshold"`
	Dialect          string         `json:"dialect,omitempty"`
	Dir              string         `json:"dir,omitempty"`
	DisabledRules    []string       `json:"disabled_rules,omitempty"`
	Findings         []lint.Finding `json:"findings"`
	Error            string         `json:"error,omitempty"`
	Versions         []int64        `json:"-"`
	Analysis         lint.Analysis  `json:"-"`
}

// Options are the migration lint inputs shared by native and Atlas-compatible
// commands.
type Options struct {
	Dir        string
	FS         fs.FS
	DirFormat  string
	Dialect    string
	ConfigPath string
	AtlasEnv   string
	DevURL     string
	GitBase    string
	GitDir     string
	Disabled   []string
	FailOn     string
	Latest     uint
	Positional []string
	Changed    ChangedOptions
	// Compatibility selects native or command-surface-specific lint semantics.
	Compatibility lint.CompatibilityProfile
}

// ChangedOptions records which CLI values were explicitly provided. This lets
// command packages preserve their flag precedence without making report
// construction depend on Cobra.
type ChangedOptions struct {
	Dir       bool
	DirFormat bool
	Dialect   bool
	AtlasEnv  bool
	DevURL    bool
	GitBase   bool
	GitDir    bool
	Latest    bool
}

type sarifReport struct {
	Version string     `json:"version"`
	Schema  string     `json:"$schema"`
	Runs    []sarifRun `json:"runs"`
}

type sarifRun struct {
	Tool               sarifTool                        `json:"tool"`
	OriginalURIBaseIDs map[string]sarifArtifactLocation `json:"originalUriBaseIds,omitempty"`
	Results            []sarifResult                    `json:"results"`
}

type sarifTool struct {
	Driver sarifDriver `json:"driver"`
}

type sarifDriver struct {
	Name           string      `json:"name"`
	InformationURI string      `json:"informationUri,omitempty"`
	Rules          []sarifRule `json:"rules,omitempty"`
}

type sarifRule struct {
	ID               string             `json:"id"`
	Name             string             `json:"name,omitempty"`
	ShortDescription sarifMessage       `json:"shortDescription,omitzero"`
	DefaultConfig    sarifDefaultConfig `json:"defaultConfiguration"`
}

type sarifDefaultConfig struct {
	Level string `json:"level"`
}

type sarifResult struct {
	RuleID              string            `json:"ruleId"`
	RuleIndex           int               `json:"ruleIndex"`
	Level               string            `json:"level"`
	Message             sarifMessage      `json:"message"`
	Locations           []sarifLocation   `json:"locations,omitempty"`
	PartialFingerprints map[string]string `json:"partialFingerprints,omitempty"`
}

type sarifMessage struct {
	Text string `json:"text"`
}

type sarifLocation struct {
	PhysicalLocation sarifPhysicalLocation `json:"physicalLocation"`
}

type sarifPhysicalLocation struct {
	ArtifactLocation sarifArtifactLocation `json:"artifactLocation"`
	Region           *sarifRegion          `json:"region,omitempty"`
}

type sarifArtifactLocation struct {
	URI       string `json:"uri"`
	URIBaseID string `json:"uriBaseId,omitempty"`
}

type sarifRegion struct {
	StartLine int `json:"startLine"`
}

// Build returns the migration lint report without rendering it.
func Build(ctx context.Context, opts Options, projectCfg projectconfig.Config) (Report, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	prepared, err := prepareSourceOptions(opts, projectCfg)
	if err != nil {
		return Report{}, err
	}
	opts = prepared.options
	sourceFS := opts.FS
	if sourceFS == nil {
		sourceFS = os.DirFS(opts.Dir)
	}
	snapshot, err := migrationsnapshot.Capture(sourceFS)
	if err != nil {
		return Report{}, err
	}
	versions, restrictVersions, err := lintVersions(ctx, opts, projectCfg, snapshot)
	if err != nil {
		return Report{}, err
	}
	cfg, err := loadEffectiveConfig(opts, projectCfg, snapshot)
	if err != nil {
		return Report{}, err
	}
	dialect, err := effectiveDialect(opts, cfg.Dialect, prepared.devDialect)
	if err != nil {
		return Report{}, err
	}
	disabled := append(append([]string{}, cfg.DisabledRules...), opts.Disabled...)
	analysis, err := lintDirectory(ctx, opts, snapshot, prepared.dirFormat, lint.Options{
		Compatibility: opts.Compatibility,
		Dialect:       dialect,
		Disabled:      disabled,
		PathPrefix:    filepath.ToSlash(opts.Dir),
		// Scoped on the Atlas-compatible surface ONLY, where the pinned
		// community binary is the contract and it reviews just what the dev URL
		// covers.
		//
		// The native surface keeps reviewing everything, deliberately. The
		// argument for scoping — "an object outside the dev URL's reach is never
		// in the before-state the run compares against" — describes a diff-based
		// analyzer. Ptah's is a SQL-text analyzer, which is why it reports
		// TRUNCATE and DROP SCHEMA, neither of which produces a diff. Applying
		// the boundary there turned `DROP TABLE app."Users", app.audit_log;`
		// from two error-grade DS101 findings and exit 1 into "No lint findings"
		// and exit 0, on a surface with no comparison binary to justify it.
		SchemaScope: compatSchemaScope(opts.Compatibility, opts.DevURL),
		Selection: lint.VersionSelection{
			Versions:   versions,
			Restricted: restrictVersions,
		},
		DirFormat:         prepared.dirFormat,
		AtlasTemplateData: migrator.AtlasTemplateData{Env: opts.AtlasEnv},
		RuleConfigs:       cfg.Rules,
	})
	findings := analysis.Findings()
	report := Report{
		Failed:           shouldFail(findings, opts.FailOn),
		FailureThreshold: opts.FailOn,
		Dialect:          dialect,
		Dir:              opts.Dir,
		DisabledRules:    disabled,
		Findings:         findings,
		Versions:         versions,
		Analysis:         analysis,
	}
	if err != nil {
		report.Error = err.Error()
		return report, err
	}
	return report, nil
}

type preparedSourceOptions struct {
	options    Options
	dirFormat  migrator.MigrationDirFormat
	devDialect string
}

// PrepareSourceOptions resolves project-backed values and validates every
// option whose error precedes migration source capture in Build.
func PrepareSourceOptions(opts Options, projectCfg projectconfig.Config) (Options, error) {
	prepared, err := prepareSourceOptions(opts, projectCfg)
	if err != nil {
		return Options{}, err
	}
	return prepared.options, nil
}

func prepareSourceOptions(
	opts Options,
	projectCfg projectconfig.Config,
) (preparedSourceOptions, error) {
	opts, err := normalizeOptions(opts, projectCfg)
	if err != nil {
		return preparedSourceOptions{}, err
	}
	dirFormat, err := migrator.ParseMigrationDirFormat(opts.DirFormat)
	if err != nil {
		return preparedSourceOptions{}, err
	}
	devDialect, err := atlasurl.DialectFromURL(opts.DevURL)
	if err != nil {
		return preparedSourceOptions{}, err
	}
	return preparedSourceOptions{
		options:    opts,
		dirFormat:  dirFormat,
		devDialect: devDialect,
	}, nil
}

func normalizeOptions(opts Options, projectCfg projectconfig.Config) (Options, error) {
	if opts.FailOn == "" {
		opts.FailOn = FailOnError
	}
	if err := ValidateFailOn(opts.FailOn); err != nil {
		return Options{}, err
	}
	if err := validateDialect(opts.Dialect); err != nil {
		return Options{}, err
	}
	if len(opts.Positional) > 0 {
		msg := fmt.Sprintf("unexpected positional arguments %q: pass the migrations directory via --dir", opts.Positional)
		return Options{}, errors.New(msg)
	}
	opts.Dir = opts.effectiveDir(projectCfg)
	opts.DirFormat = opts.effectiveDirFormat(projectCfg)
	opts.AtlasEnv = opts.effectiveAtlasEnv(projectCfg)
	opts.DevURL = opts.effectiveDevURL(projectCfg)
	if opts.FS == nil {
		if err := validateDir(opts.Dir); err != nil {
			return Options{}, err
		}
	}
	return opts, nil
}

func (opts Options) effectiveDir(projectCfg projectconfig.Config) string {
	return effectiveProjectString(
		projectconfig.Value[string]{Value: opts.Dir, Present: opts.Changed.Dir},
		projectCfg.StringValue(projectconfig.StringMigrationDir),
	)
}

func (opts Options) effectiveDirFormat(projectCfg projectconfig.Config) string {
	return effectiveProjectString(
		projectconfig.Value[string]{Value: opts.DirFormat, Present: opts.Changed.DirFormat},
		projectCfg.StringValue(projectconfig.StringMigrationFormat),
	)
}

func (opts Options) effectiveAtlasEnv(projectCfg projectconfig.Config) string {
	return effectiveProjectString(
		projectconfig.Value[string]{Value: opts.AtlasEnv, Present: opts.Changed.AtlasEnv},
		projectCfg.StringValue(projectconfig.StringEnvName),
	)
}

func (opts Options) effectiveDevURL(projectCfg projectconfig.Config) string {
	return effectiveProjectString(
		projectconfig.Value[string]{Value: opts.DevURL, Present: opts.Changed.DevURL},
		projectCfg.StringValue(projectconfig.StringDevURL),
	)
}

func effectiveProjectString(
	optionValue projectconfig.Value[string],
	configValue projectconfig.Value[string],
) string {
	if optionValue.Present || !configValue.Present {
		return optionValue.Value
	}
	return configValue.Value
}

func loadEffectiveConfig(
	opts Options,
	projectCfg projectconfig.Config,
	fsys fs.FS,
) (*lint.Config, error) {
	cfg, err := loadConfig(opts, fsys)
	if err != nil {
		return nil, err
	}
	if cfg.Dialect == "" {
		cfg.Dialect = projectCfg.Lint.Dialect
	}
	if len(cfg.DisabledRules) == 0 {
		cfg.DisabledRules = append([]string{}, projectCfg.Lint.DisabledRules...)
	}
	cfg.Rules = effectiveLintRuleConfigs(projectCfg.Lint.RuleConfigs, cfg.Rules)
	if !lintdialect.Valid(cfg.Dialect) {
		msg := fmt.Sprintf("invalid dialect %q in lint config: expected %s", cfg.Dialect, lintdialect.Expected)
		return nil, errors.New(msg)
	}
	return cfg, nil
}

func effectiveDialect(opts Options, configDialect, devDialect string) (string, error) {
	dialect := configDialect
	if opts.Changed.Dialect {
		dialect = opts.Dialect
	}
	if err := validateDevURLDialect(dialect, devDialect); err != nil {
		return "", err
	}
	if dialect == "" {
		dialect = devDialect
	}
	return dialect, nil
}

// lintDirectory analyzes the captured migration directory and replays it on the
// dev database.
//
// The replay does double duty. It has always validated that the SQL runs, and it
// now also reads the schema state the versions the analysis asked about start
// from, because some diagnostics cannot be reached from SQL text alone -- see
// [lint.BaselineColumn]. When it read anything, the analysis runs a second time
// with those facts in hand.
//
// The analysis stays first so error precedence does not move: a directory that
// fails to analyze reports that, not a replay error it would also have hit. The
// second pass is skipped whenever the first one asked for nothing, which is
// every run with no renames to resolve and every run without a dev database.
func lintDirectory(
	ctx context.Context,
	opts Options,
	fsys fs.FS,
	dirFormat migrator.MigrationDirFormat,
	lintOptions lint.Options,
) (lint.Analysis, error) {
	analysis, err := lint.AnalyzeFS(fsys, lintOptions)
	if err != nil {
		return lint.Analysis{}, err
	}
	baseline := newBaselineCollector(analysis.BaselineVersions(), opts.DevURL)
	if err := migrationreplay.Replay(ctx, migrationreplay.Options{
		Dir:               opts.Dir,
		DirFormat:         dirFormat,
		DevURL:            opts.DevURL,
		FS:                analysis.SnapshotFS(),
		AtlasTemplateData: migrator.AtlasTemplateData{Env: opts.AtlasEnv},
		ObserveVersion:    baseline.observer(),
	}); err != nil {
		return analysis, fmt.Errorf("error validating migration SQL on dev database: %w", err)
	}
	if len(baseline.columns) == 0 {
		return analysis, nil
	}
	lintOptions.Baseline = baseline.columns
	return lint.AnalyzeFS(fsys, lintOptions)
}

// baselineCollector reads the dev database once per version the analysis asked
// about, while the replay is standing on that version's starting state.
//
// A nil observer is handed to the replay when nothing was asked for, so a
// directory with no renames pays no introspection at all.
type baselineCollector struct {
	wanted  map[int64]bool
	schemas []string
	columns []lint.BaselineColumn
}

func newBaselineCollector(versions []int64, devURL string) *baselineCollector {
	collector := &baselineCollector{wanted: make(map[int64]bool, len(versions))}
	for _, version := range versions {
		collector.wanted[version] = true
	}
	if scope := schemaselection.FromURL(devURL).Scope; scope != "" {
		collector.schemas = []string{scope}
	}
	return collector
}

// observer returns the replay hook, or nil when the analysis asked for nothing
// so the replay runs exactly as it did before this existed.
func (c *baselineCollector) observer() func(context.Context, int64, *dbschema.DatabaseConnection) error {
	if len(c.wanted) == 0 {
		return nil
	}
	return c.observe
}

func (c *baselineCollector) observe(
	_ context.Context,
	version int64,
	conn *dbschema.DatabaseConnection,
) error {
	if c == nil || !c.wanted[version] {
		return nil
	}
	columns, err := readBaselineColumns(conn, version, c.schemas)
	if err != nil {
		return err
	}
	c.columns = append(c.columns, columns...)
	return nil
}

func validateDevURLDialect(dialect, devDialect string) error {
	if dialect == "" || devDialect == "" {
		return nil
	}
	if dialect != devDialect {
		return fmt.Errorf("lint dialect %q does not match --dev-url dialect %q", dialect, devDialect)
	}
	return nil
}

// loadConfig reads the explicit --config file, or the conventional
// .ptah-lint.yaml inside the linted directory when present.
func loadConfig(opts Options, fsys fs.FS) (*lint.Config, error) {
	if opts.ConfigPath != "" {
		return lint.LoadConfig(opts.ConfigPath)
	}
	return lint.LoadConfigFS(fsys, lint.ConfigFileName)
}

func effectiveLintRuleConfigs(
	projectRules map[string]projectconfig.LintRuleConfig,
	configRules map[string]lint.RuleConfig,
) map[string]lint.RuleConfig {
	if len(projectRules) == 0 {
		return cloneLintRuleConfigs(configRules)
	}
	merged := make(map[string]lint.RuleConfig, len(projectRules)+len(configRules))
	for code, rule := range projectRules {
		converted := lint.RuleConfig{
			Severity: lint.Severity(rule.Severity),
			Exclude:  slices.Clone(rule.Exclude),
		}
		merged[code] = converted
	}
	for code, rule := range configRules {
		rule.Exclude = slices.Clone(rule.Exclude)
		merged[code] = rule
	}
	return merged
}

func cloneLintRuleConfigs(values map[string]lint.RuleConfig) map[string]lint.RuleConfig {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]lint.RuleConfig, len(values))
	for code, rule := range values {
		rule.Exclude = slices.Clone(rule.Exclude)
		cloned[code] = rule
	}
	return cloned
}

func lintVersions(
	ctx context.Context,
	opts Options,
	cfg projectconfig.Config,
	fsys fs.FS,
) ([]int64, bool, error) {
	projectSelectors := projectLintSelectorUse{
		latest: !opts.Changed.GitBase,
		git:    !opts.Changed.Latest,
	}
	latest, latestSet := effectiveLatest(opts, cfg, projectSelectors)
	git, err := effectiveGit(opts, cfg, projectSelectors)
	if err != nil {
		return nil, false, err
	}
	if !git.ok && gitDirConfigured(opts, cfg, projectSelectors) {
		return nil, false, fmt.Errorf("--git-dir requires --git-base")
	}
	if latestSet && git.ok {
		return nil, false, fmt.Errorf("--latest and --git-base are mutually exclusive")
	}
	if latestSet {
		if latest <= 0 {
			return nil, false, fmt.Errorf("--latest must be greater than zero")
		}
		dirFormat, err := migrator.ParseMigrationDirFormat(opts.DirFormat)
		if err != nil {
			return nil, false, err
		}
		versions, err := latestMigrationVersions(fsys, uint(latest), dirFormat)
		return versions, true, err
	}
	if git.ok {
		dirFormat, err := migrator.ParseMigrationDirFormat(opts.DirFormat)
		if err != nil {
			return nil, false, err
		}
		versions, err := gitChangedMigrationVersions(ctx, opts.Dir, git.base, git.dir, dirFormat)
		return versions, true, err
	}
	return nil, false, nil
}

type projectLintSelectorUse struct {
	latest bool
	git    bool
}

func effectiveLatest(
	opts Options,
	cfg projectconfig.Config,
	projectSelectors projectLintSelectorUse,
) (int, bool) {
	if opts.Changed.Latest {
		return int(opts.Latest), true
	}
	if !projectSelectors.latest {
		return 0, false
	}
	value := cfg.LintLatestValue()
	if value.Present {
		return value.Value, true
	}
	return 0, false
}

type effectiveGitOptions struct {
	base string
	dir  string
	ok   bool
}

func effectiveGit(
	opts Options,
	cfg projectconfig.Config,
	projectSelectors projectLintSelectorUse,
) (effectiveGitOptions, error) {
	gitBase := opts.GitBase
	configGitBase := cfg.StringValue(projectconfig.StringLintGitBase)
	if !opts.Changed.GitBase && projectSelectors.git && configGitBase.Present {
		gitBase = configGitBase.Value
	}
	gitDir := opts.GitDir
	configGitDir := cfg.StringValue(projectconfig.StringLintGitDir)
	if !opts.Changed.GitDir && projectSelectors.git && configGitDir.Present {
		gitDir = configGitDir.Value
	}
	if strings.TrimSpace(gitBase) == "" {
		return effectiveGitOptions{dir: gitDir}, nil
	}
	if err := validateGitBaseRef(gitBase); err != nil {
		return effectiveGitOptions{}, err
	}
	if strings.TrimSpace(gitDir) == "" {
		gitDir = "."
	}
	return effectiveGitOptions{base: gitBase, dir: gitDir, ok: true}, nil
}

func validateGitBaseRef(ref string) error {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return fmt.Errorf("--git-base requires a non-empty ref")
	}
	if strings.HasPrefix(ref, "-") || strings.ContainsAny(ref, "\x00\r\n") {
		return fmt.Errorf("--git-base %q is not a safe Git ref", ref)
	}
	return nil
}

func gitDirConfigured(
	opts Options,
	cfg projectconfig.Config,
	projectSelectors projectLintSelectorUse,
) bool {
	if opts.Changed.GitDir {
		return true
	}
	if !projectSelectors.git {
		return false
	}
	configGitDir := cfg.StringValue(projectconfig.StringLintGitDir)
	return configGitDir.Present && configGitDir.Value != ""
}

func gitChangedMigrationVersions(
	ctx context.Context,
	migrationsDir string,
	gitBase string,
	gitDir string,
	dirFormat migrator.MigrationDirFormat,
) ([]int64, error) {
	repoRoot, err := gitOutput(ctx, gitDir, "rev-parse", "--show-toplevel")
	if err != nil {
		return nil, fmt.Errorf("find git repository root: %w", err)
	}
	migrationsAbs, err := filepath.Abs(migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("resolve migrations directory: %w", err)
	}
	relDir, err := filepath.Rel(repoRoot, migrationsAbs)
	if err != nil {
		return nil, fmt.Errorf("resolve migrations directory relative to git repository: %w", err)
	}
	if strings.HasPrefix(relDir, ".."+string(filepath.Separator)) || relDir == ".." || filepath.IsAbs(relDir) {
		return nil, fmt.Errorf("migrations directory %s is outside git repository %s", migrationsAbs, repoRoot)
	}
	changed, err := gitOutput(ctx, repoRoot,
		"diff",
		"--name-only",
		"--diff-filter=ACMR",
		"--end-of-options",
		gitBase+"...HEAD",
		"--",
		filepath.ToSlash(relDir),
	)
	if err != nil {
		return nil, fmt.Errorf("detect git changeset against %q: %w", gitBase, err)
	}
	versions, err := migrationVersionsFromChangedPaths(changed, dirFormat)
	if err != nil {
		return nil, err
	}
	return versions, nil
}

func gitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}

func migrationVersionsFromChangedPaths(changed string, dirFormat migrator.MigrationDirFormat) ([]int64, error) {
	seen := map[int64]struct{}{}
	var unversioned []string
	for name := range strings.Lines(changed) {
		name = strings.TrimSpace(name)
		if !strings.EqualFold(path.Ext(name), ".sql") {
			continue
		}
		parsed, err := parseChangedMigrationName(path.Base(name), dirFormat)
		if err != nil {
			unversioned = append(unversioned, name)
			continue
		}
		if parsed.Repeatable || parsed.Version <= 0 {
			continue
		}
		seen[parsed.Version] = struct{}{}
	}
	if len(unversioned) > 0 {
		return nil, fmt.Errorf("--git-base requires versioned migration files; unversioned SQL files found: %s", strings.Join(unversioned, ", "))
	}
	versions := make([]int64, 0, len(seen))
	for version := range seen {
		versions = append(versions, version)
	}
	slices.Sort(versions)
	return versions, nil
}

func parseChangedMigrationName(name string, dirFormat migrator.MigrationDirFormat) (*migrator.MigrationFile, error) {
	switch dirFormat {
	case migrator.MigrationDirFormatPtah:
		return migrator.ParseMigrationFileName(name)
	case migrator.MigrationDirFormatAtlas:
		return migrator.ParseAtlasMigrationFileName(name)
	}
	if parsed, err := migrator.ParseMigrationFileName(name); err == nil {
		return parsed, nil
	}
	return migrator.ParseAtlasMigrationFileNameForAutoDetection(name)
}

func latestMigrationVersions(fsys fs.FS, latest uint, dirFormat migrator.MigrationDirFormat) ([]int64, error) {
	unversioned, err := unversionedSQLFiles(fsys, dirFormat)
	if err != nil {
		return nil, err
	}
	if len(unversioned) > 0 {
		return nil, fmt.Errorf("--latest requires versioned migration files; unversioned SQL files found: %s", strings.Join(unversioned, ", "))
	}
	files, err := migrator.DiscoverMigrationFiles(fsys, dirFormat)
	if err != nil {
		return nil, err
	}
	seen := make(map[int64]struct{})
	for _, file := range files {
		if file.Repeatable || file.Version <= 0 {
			continue
		}
		seen[file.Version] = struct{}{}
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("no versioned migration files found for --latest")
	}
	versions := make([]int64, 0, len(seen))
	for version := range seen {
		versions = append(versions, version)
	}
	slices.SortFunc(versions, func(a, b int64) int {
		return cmp.Compare(b, a)
	})
	if latest < uint(len(versions)) {
		versions = versions[:int(latest)]
	}
	slices.Sort(versions)
	return versions, nil
}

func unversionedSQLFiles(fsys fs.FS, dirFormat migrator.MigrationDirFormat) ([]string, error) {
	var names []string
	hasAtlasSum := false
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if path.Base(p) == "atlas.sum" {
			hasAtlasSum = true
			return nil
		}
		if strings.EqualFold(path.Ext(p), ".sql") {
			names = append(names, p)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan migration files for --latest: %w", err)
	}

	var unversioned []string
	parseAtlasName := migrator.ParseAtlasMigrationFileNameForAutoDetection
	if hasAtlasSum || dirFormat == migrator.MigrationDirFormatAtlas {
		parseAtlasName = migrator.ParseAtlasMigrationFileName
	}
	for _, name := range names {
		known, err := knownVersionedMigration(fsys, name, dirFormat, parseAtlasName)
		if err != nil {
			return nil, err
		}
		if !known {
			unversioned = append(unversioned, name)
		}
	}
	slices.Sort(unversioned)
	return unversioned, nil
}

func knownVersionedMigration(
	fsys fs.FS,
	name string,
	dirFormat migrator.MigrationDirFormat,
	parseAtlasName func(string) (*migrator.MigrationFile, error),
) (bool, error) {
	base := path.Base(name)
	if dirFormat != migrator.MigrationDirFormatAtlas {
		if _, err := migrator.ParseMigrationFileName(base); err == nil {
			return true, nil
		}
	}
	if dirFormat == migrator.MigrationDirFormatPtah {
		return false, nil
	}
	if _, err := parseAtlasName(base); err == nil {
		return true, nil
	}

	raw, err := fs.ReadFile(fsys, name)
	if err != nil {
		return false, fmt.Errorf("failed to read %s: %w", name, err)
	}
	sql := string(raw)
	return migrator.LooksAtlasTemplateSQL(sql) && strings.Contains(sql, "define "), nil
}

func shouldFail(findings []lint.Finding, failOn string) bool {
	switch failOn {
	case FailOnNone:
		return false
	case FailOnAny:
		return len(findings) > 0
	default:
		for _, finding := range findings {
			if finding.Severity == lint.SeverityError {
				return true
			}
		}
		return false
	}
}

// Write renders a migration lint report using a native Ptah output format.
func Write(w io.Writer, format string, report Report) error {
	switch format {
	case FormatJSON:
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	case FormatGitHubActions:
		writeGitHubActions(w, report)
		return nil
	case FormatSARIF:
		return writeSARIF(w, report)
	default:
		writeText(w, report)
		return nil
	}
}

func writeSARIF(w io.Writer, report Report) error {
	rules, ruleIndexes := sarifRules(report.Findings)
	results := make([]sarifResult, 0, len(report.Findings))
	for _, finding := range report.Findings {
		artifactURI := sarifArtifactURI(finding.File)
		artifactLocation := sarifArtifactLocation{URI: artifactURI}
		if isRelativeURI(artifactURI) {
			artifactLocation.URIBaseID = "%SRCROOT%"
		}
		location := sarifLocation{
			PhysicalLocation: sarifPhysicalLocation{
				ArtifactLocation: artifactLocation,
				Region:           &sarifRegion{StartLine: sarifStartLine(finding.Line)},
			},
		}
		results = append(results, sarifResult{
			RuleID:              finding.Rule,
			RuleIndex:           ruleIndexes[finding.Rule],
			Level:               sarifLevel(finding.Severity),
			Message:             sarifMessage{Text: fmt.Sprintf("%s: %s", finding.Title, finding.Message)},
			Locations:           []sarifLocation{location},
			PartialFingerprints: sarifPartialFingerprints(finding, artifactURI),
		})
	}

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(sarifReport{
		Version: "2.1.0",
		Schema:  "https://json.schemastore.org/sarif-2.1.0.json",
		Runs: []sarifRun{{
			Tool: sarifTool{Driver: sarifDriver{
				Name:           "ptah migrations lint",
				InformationURI: "https://github.com/stokaro/ptah",
				Rules:          rules,
			}},
			OriginalURIBaseIDs: map[string]sarifArtifactLocation{
				"%SRCROOT%": {URI: "file:///"},
			},
			Results: results,
		}},
	})
}

func sarifRules(findings []lint.Finding) ([]sarifRule, map[string]int) {
	byCode := make(map[string]lint.Finding)
	for _, finding := range findings {
		if _, ok := byCode[finding.Rule]; !ok {
			byCode[finding.Rule] = finding
		}
	}
	codes := make([]string, 0, len(byCode))
	for code := range byCode {
		codes = append(codes, code)
	}
	slices.Sort(codes)
	rules := make([]sarifRule, 0, len(codes))
	indexes := make(map[string]int, len(codes))
	for i, code := range codes {
		indexes[code] = i
		finding := byCode[code]
		rules = append(rules, sarifRule{
			ID:               code,
			Name:             finding.Title,
			ShortDescription: sarifMessage{Text: finding.Title},
			DefaultConfig:    sarifDefaultConfig{Level: sarifLevel(finding.Severity)},
		})
	}
	return rules, indexes
}

func sarifLevel(severity lint.Severity) string {
	return risk.SARIFLevel(severity)
}

func sarifArtifactURI(file string) string {
	if file == "" {
		return ""
	}
	cleaned := filepath.Clean(file)
	if filepath.IsAbs(cleaned) {
		if wd, err := os.Getwd(); err == nil {
			if rel, err := filepath.Rel(wd, cleaned); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return filepath.ToSlash(rel)
			}
		}
		return (&url.URL{Scheme: "file", Path: filepath.ToSlash(cleaned)}).String()
	}
	return path.Clean(filepath.ToSlash(file))
}

func isRelativeURI(uri string) bool {
	parsed, err := url.Parse(uri)
	return err == nil && parsed.Scheme == "" && !strings.HasPrefix(uri, "/")
}

func sarifStartLine(line int) int {
	if line > 0 {
		return line
	}
	return 1
}

func sarifPartialFingerprints(finding lint.Finding, artifactURI string) map[string]string {
	hash := sha256.New()
	fmt.Fprintf(hash, "%s\x00%s\x00%d\x00%s", finding.Rule, artifactURI, sarifStartLine(finding.Line), finding.Message)
	return map[string]string{
		"primaryLocationLineHash": hex.EncodeToString(hash.Sum(nil))[:32],
	}
}

func writeText(w io.Writer, report Report) {
	if report.Error != "" {
		fmt.Fprintf(w, "error: %s\n", report.Error)
		return
	}
	if len(report.Findings) == 0 {
		fmt.Fprintln(w, "No lint findings.")
		return
	}
	for _, finding := range report.Findings {
		fmt.Fprintln(w, lint.Describe(finding))
	}
	fmt.Fprintf(w, "\n%d finding(s).\n", len(report.Findings))
}

func writeGitHubActions(w io.Writer, report Report) {
	if report.Error != "" {
		fmt.Fprintf(w, "::error::%s\n", escapeGHData(report.Error))
		return
	}
	for _, finding := range report.Findings {
		level := "warning"
		if finding.Severity == lint.SeverityError {
			level = "error"
		}
		file := escapeGHProperty(finding.File)
		message := escapeGHData(fmt.Sprintf("%s: %s", finding.Rule, finding.Message))
		if finding.Line > 0 {
			fmt.Fprintf(w, "::%s file=%s,line=%d::%s\n", level, file, finding.Line, message)
			continue
		}
		fmt.Fprintf(w, "::%s file=%s::%s\n", level, file, message)
	}
}

func escapeGHData(s string) string {
	s = strings.ReplaceAll(s, "%", "%25")
	s = strings.ReplaceAll(s, "\r", "%0D")
	s = strings.ReplaceAll(s, "\n", "%0A")
	return s
}

func escapeGHProperty(s string) string {
	s = escapeGHData(s)
	s = strings.ReplaceAll(s, ":", "%3A")
	s = strings.ReplaceAll(s, ",", "%2C")
	return s
}

// ErrorReport returns the report shape used for validation and usage errors.
func ErrorReport(failOn, msg string) Report {
	return Report{
		Failed:           true,
		FailureThreshold: failOn,
		Findings:         []lint.Finding{},
		Error:            msg,
	}
}

// ValidateFormat rejects unknown native report formats.
func ValidateFormat(format string) error {
	switch format {
	case FormatText, FormatJSON, FormatGitHubActions, FormatSARIF:
		return nil
	default:
		return fmt.Errorf("invalid --format value %q: expected text, json, github-actions, or sarif", format)
	}
}

// ValidateFailOn rejects unknown failure thresholds.
func ValidateFailOn(failOn string) error {
	switch failOn {
	case FailOnError, FailOnAny, FailOnNone:
		return nil
	default:
		return fmt.Errorf("invalid --fail-on value %q: expected error, any, or none", failOn)
	}
}

func validateDialect(dialect string) error {
	if lintdialect.Valid(dialect) {
		return nil
	}
	return fmt.Errorf("invalid --dialect value %q: expected %s", dialect, lintdialect.Expected)
}

func validateDir(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("migrations directory %s: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("migrations directory %s: not a directory", dir)
	}
	return nil
}

// compatSchemaScope returns the schema boundary the run analyzes within, which
// is empty — everything under review — on every surface but the
// Atlas-compatible one.
//
// The split is the point. On ptah-compat the pinned community binary is the
// contract and it reviews only what the dev URL covers, so matching it means
// scoping. The native surface has no such contract and its linter reads SQL
// text rather than a diff, so an object the dev database does not carry is
// still an object the migration destroys.
func compatSchemaScope(profile lint.CompatibilityProfile, devURL string) string {
	if profile != lint.CompatibilityProfileAtlas {
		return ""
	}
	return schemaselection.FromURL(devURL).Scope
}
