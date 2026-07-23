// Package lint implements the migration lint command: a sqlcheck-style linter
// for migration directories with rule-coded findings (issue #151).
package lint

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

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/internal/atlasurl"
	"github.com/stokaro/ptah/internal/migrationreplay"
	"github.com/stokaro/ptah/migration/lint"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/risk"
)

const (
	formatText          = "text"
	formatJSON          = "json"
	formatGitHubActions = "github-actions"
	formatSARIF         = "sarif"

	failOnError = "error"
	failOnAny   = "any"
	failOnNone  = "none"

	latestFlag  = "latest"
	gitBaseFlag = "git-base"
	gitDirFlag  = "git-dir"
)

var errLintFindings = errors.New("lint findings exceed the failure threshold")

// NewLintCommand returns the migration-linter command.
func NewLintCommand() *cobra.Command {
	var dir string
	var dialect string
	var format string
	var configPath string
	var atlasEnv string
	var envName string
	var devURL string
	var gitBase string
	var gitDir string
	var disabled []string
	var failOn string
	var latest uint

	cmd := &cobra.Command{
		Use:   "lint",
		Short: "Lint migration files for production-unsafe patterns",
		Long: `Lint inspects every *.sql file in a migrations directory and reports
rule-coded findings, sqlcheck-style:

  DS  data safety (dropped tables/columns, lossy type changes)
  MF  migration form (missing down file, empty migration, naming)
  BC  breaking-change safety (renames breaking deployed code)
  PG  PostgreSQL-specific hazards (CREATE INDEX without CONCURRENTLY, ...)
  MY  MySQL/MariaDB-specific hazards (lock-heavy ALTER TABLE forms)

Statement rules run against up migrations; file-form rules cover every file.
Rules can be disabled per code or family via --disable or .ptah-lint.yaml.`,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLint(cmd, runOptions{
				dir:        dir,
				dialect:    dialect,
				format:     format,
				configPath: configPath,
				atlasEnv:   atlasEnv,
				devURL:     devURL,
				gitBase:    gitBase,
				gitDir:     gitDir,
				disabled:   disabled,
				failOn:     failOn,
				latest:     latest,
				positional: args,
			})
		},
	}

	cmd.Flags().StringVar(&dir, "dir", "./migrations", "Directory containing migration files")
	cmd.Flags().StringVar(&dialect, "dialect", "", "Target dialect gating dialect-specific rules: postgres, mysql, mariadb, sqlite, clickhouse, cockroachdb, yugabytedb, or spanner (empty runs every rule)")
	cmd.Flags().StringVar(&format, "format", formatText, "Output format: text, json, github-actions, sarif")
	cmd.Flags().StringVar(&configPath, "config", "", "Path to a lint config file (default: <dir>/"+lint.ConfigFileName+" when present)")
	cmd.Flags().StringVar(&atlasEnv, "atlas-env", "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	cmd.Flags().StringVar(&envName, dbcli.EnvFlagName, "", "Project env name to read from ptah.yaml or atlas.hcl")
	dbcli.RegisterAtlasProjectInternalFlags(cmd.Flags())
	cmd.Flags().StringVar(&devURL, "dev-url", "", "Dev database URL used to clean and replay migrations and infer the lint dialect")
	cmd.Flags().StringVar(&gitBase, gitBaseFlag, "", "Run analysis against the base Git branch")
	cmd.Flags().StringVar(&gitDir, gitDirFlag, ".", "Repository working directory for --git-base")
	cmd.Flags().StringArrayVar(&disabled, "disable", nil, "Disable a rule code or family, for example DS101 or MY (repeatable)")
	cmd.Flags().StringVar(&failOn, "fail-on", failOnError, "Failure threshold controlling the exit code: error, any or none")
	cmd.Flags().UintVar(&latest, latestFlag, 0, "Lint only the latest N migration versions")

	cmdutil.ConfigureCommand(cmd)
	return cmd
}

type runOptions struct {
	dir        string
	dialect    string
	format     string
	configPath string
	atlasEnv   string
	devURL     string
	gitBase    string
	gitDir     string
	disabled   []string
	failOn     string
	latest     uint
	positional []string
}

type lintReport struct {
	Failed           bool           `json:"failed"`
	FailureThreshold string         `json:"failure_threshold"`
	Dialect          string         `json:"dialect,omitempty"`
	Dir              string         `json:"dir,omitempty"`
	DisabledRules    []string       `json:"disabled_rules,omitempty"`
	Findings         []lint.Finding `json:"findings"`
	Error            string         `json:"error,omitempty"`
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

func runLint(cmd *cobra.Command, opts runOptions) error {
	if err := validateFormat(opts.format); err != nil {
		return writeError(cmd.ErrOrStderr(), formatText, opts.failOn, err.Error())
	}
	if err := validateFailOn(opts.failOn); err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, failOnError, err.Error())
	}
	if err := validateDialect(opts.dialect); err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, opts.failOn, err.Error())
	}
	if len(opts.positional) > 0 {
		// Silently linting the default --dir while the user pointed at
		// another directory would be a silent false negative in CI.
		msg := fmt.Sprintf("unexpected positional arguments %q: pass the migrations directory via --dir", opts.positional)
		return writeError(cmd.ErrOrStderr(), opts.format, opts.failOn, msg)
	}
	projectCfg, err := dbcli.LoadProjectConfig(cmd, "")
	if err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, opts.failOn, err.Error())
	}
	opts.dir = dbcli.EffectiveString(cmd, "dir", opts.dir, projectCfg.Migration.Dir)
	opts.atlasEnv = dbcli.EffectiveString(cmd, "atlas-env", opts.atlasEnv, projectCfg.EnvName)
	opts.devURL = dbcli.EffectiveString(cmd, "dev-url", opts.devURL, projectCfg.DevURL)
	if err := validateDir(opts.dir); err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, opts.failOn, err.Error())
	}
	devDialect, err := atlasurl.DialectFromURL(opts.devURL)
	if err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, opts.failOn, err.Error())
	}
	versions, restrictVersions, err := lintVersions(cmd, opts, projectCfg)
	if err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, opts.failOn, err.Error())
	}

	cfg, err := loadConfig(opts)
	if err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, opts.failOn, err.Error())
	}
	if cfg.Dialect == "" {
		cfg.Dialect = projectCfg.Lint.Dialect
	}
	if len(cfg.DisabledRules) == 0 {
		cfg.DisabledRules = append([]string{}, projectCfg.Lint.DisabledRules...)
	}
	cfg.Rules = effectiveLintRuleConfigs(projectCfg.Lint.RuleConfigs, cfg.Rules)
	if !isValidDialect(cfg.Dialect) {
		msg := fmt.Sprintf("invalid dialect %q in lint config: expected postgres, mysql, mariadb, sqlite, clickhouse, cockroachdb, yugabytedb, or spanner", cfg.Dialect)
		return writeError(cmd.ErrOrStderr(), opts.format, opts.failOn, msg)
	}
	// An explicitly passed --dialect wins even when set to "" (run every
	// rule); an untouched flag defers to the config.
	dialect := cfg.Dialect
	if cmd.Flags().Changed("dialect") {
		dialect = opts.dialect
	}
	if err := validateDevURLDialect(dialect, devDialect); err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, opts.failOn, err.Error())
	}
	if dialect == "" {
		dialect = devDialect
	}
	disabled := append(append([]string{}, cfg.DisabledRules...), opts.disabled...)

	if err := migrationreplay.Replay(cmd.Context(), migrationreplay.Options{
		Dir:       opts.dir,
		DirFormat: migrator.MigrationDirFormatAuto,
		DevURL:    opts.devURL,
	}); err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, opts.failOn,
			fmt.Sprintf("error validating migration SQL on dev database: %v", err))
	}

	findings := []lint.Finding{}
	if !restrictVersions || len(versions) > 0 {
		findings, err = lint.LintFS(os.DirFS(opts.dir), lint.Options{
			Dialect:           dialect,
			Disabled:          disabled,
			PathPrefix:        filepath.ToSlash(opts.dir),
			Versions:          versions,
			AtlasTemplateData: migrator.AtlasTemplateData{Env: opts.atlasEnv},
			RuleConfigs:       cfg.Rules,
		})
		if err != nil {
			return writeError(cmd.ErrOrStderr(), opts.format, opts.failOn, err.Error())
		}
		if findings == nil {
			findings = []lint.Finding{}
		}
	}

	failed := shouldFail(findings, opts.failOn)
	report := lintReport{
		Failed:           failed,
		FailureThreshold: opts.failOn,
		Dialect:          dialect,
		Dir:              opts.dir,
		DisabledRules:    disabled,
		Findings:         findings,
	}

	writer := cmd.OutOrStdout()
	if failed {
		writer = cmd.ErrOrStderr()
	}
	if err := writeReport(writer, opts.format, report); err != nil {
		return writeError(cmd.ErrOrStderr(), formatText, opts.failOn, err.Error())
	}
	if failed {
		return exitcode.New(1, errLintFindings)
	}
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
func loadConfig(opts runOptions) (*lint.Config, error) {
	if opts.configPath != "" {
		if _, err := os.Stat(opts.configPath); err != nil {
			return nil, fmt.Errorf("lint config %s: %w", opts.configPath, err)
		}
		return lint.LoadConfig(opts.configPath)
	}
	return lint.LoadConfig(filepath.Join(opts.dir, lint.ConfigFileName))
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

func lintVersions(cmd *cobra.Command, opts runOptions, cfg projectconfig.Config) ([]int64, bool, error) {
	latest, latestSet := effectiveLatest(cmd, opts, cfg)
	gitBase, gitDir, gitSet := effectiveGit(cmd, opts, cfg)
	if !gitSet && gitDirConfigured(cmd, cfg) {
		return nil, false, fmt.Errorf("--git-dir requires --git-base")
	}
	if latestSet && gitSet {
		return nil, false, fmt.Errorf("--latest and --git-base are mutually exclusive")
	}
	if latestSet {
		if latest <= 0 {
			return nil, false, fmt.Errorf("--latest must be greater than zero")
		}
		versions, err := latestMigrationVersions(os.DirFS(opts.dir), uint(latest))
		return versions, true, err
	}
	if gitSet {
		versions, err := gitChangedMigrationVersions(cmd.Context(), opts.dir, gitBase, gitDir)
		return versions, true, err
	}
	return nil, false, nil
}

func effectiveLatest(cmd *cobra.Command, opts runOptions, cfg projectconfig.Config) (int, bool) {
	if cmd.Flags().Changed(latestFlag) {
		return int(opts.latest), true
	}
	if cfg.Lint.Latest != nil {
		return *cfg.Lint.Latest, true
	}
	return 0, false
}

func effectiveGit(cmd *cobra.Command, opts runOptions, cfg projectconfig.Config) (gitBase, gitDir string, ok bool) {
	gitBase = opts.gitBase
	if !cmd.Flags().Changed(gitBaseFlag) {
		gitBase = cfg.Lint.GitBase
	}
	gitDir = opts.gitDir
	if !cmd.Flags().Changed(gitDirFlag) && cfg.Lint.GitDir != "" {
		gitDir = cfg.Lint.GitDir
	}
	if strings.TrimSpace(gitBase) == "" {
		return "", gitDir, false
	}
	if strings.TrimSpace(gitDir) == "" {
		gitDir = "."
	}
	return gitBase, gitDir, true
}

func gitDirConfigured(cmd *cobra.Command, cfg projectconfig.Config) bool {
	return cmd.Flags().Changed(gitDirFlag) || cfg.Lint.GitDir != ""
}

func gitChangedMigrationVersions(ctx context.Context, migrationsDir, gitBase, gitDir string) ([]int64, error) {
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
	changed, err := gitOutput(ctx, repoRoot, "diff", "--name-only", "--diff-filter=ACMR", gitBase+"...HEAD", "--", filepath.ToSlash(relDir))
	if err != nil {
		return nil, fmt.Errorf("detect git changeset against %q: %w", gitBase, err)
	}
	versions, err := migrationVersionsFromChangedPaths(changed)
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

func migrationVersionsFromChangedPaths(changed string) ([]int64, error) {
	seen := map[int64]struct{}{}
	var unversioned []string
	for name := range strings.Lines(changed) {
		name = strings.TrimSpace(name)
		if !strings.EqualFold(path.Ext(name), ".sql") {
			continue
		}
		parsed, err := parseChangedMigrationName(path.Base(name))
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

func parseChangedMigrationName(name string) (*migrator.MigrationFile, error) {
	if parsed, err := migrator.ParseMigrationFileName(name); err == nil {
		return parsed, nil
	}
	return migrator.ParseAtlasMigrationFileNameForAutoDetection(name)
}

func latestMigrationVersions(fsys fs.FS, latest uint) ([]int64, error) {
	unversioned, err := unversionedSQLFiles(fsys)
	if err != nil {
		return nil, err
	}
	if len(unversioned) > 0 {
		return nil, fmt.Errorf("--latest requires versioned migration files; unversioned SQL files found: %s", strings.Join(unversioned, ", "))
	}
	files, err := migrator.DiscoverMigrationFiles(fsys, migrator.MigrationDirFormatAuto)
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

func unversionedSQLFiles(fsys fs.FS) ([]string, error) {
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
	if hasAtlasSum {
		parseAtlasName = migrator.ParseAtlasMigrationFileName
	}
	for _, name := range names {
		known, err := knownVersionedMigration(fsys, name, parseAtlasName)
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
	parseAtlasName func(string) (*migrator.MigrationFile, error),
) (bool, error) {
	base := path.Base(name)
	if _, err := migrator.ParseMigrationFileName(base); err == nil {
		return true, nil
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

// shouldFail applies the --fail-on threshold to the findings.
func shouldFail(findings []lint.Finding, failOn string) bool {
	switch failOn {
	case failOnNone:
		return false
	case failOnAny:
		return len(findings) > 0
	default: // failOnError
		for _, f := range findings {
			if f.Severity == lint.SeverityError {
				return true
			}
		}
		return false
	}
}

func writeReport(w io.Writer, format string, report lintReport) error {
	switch format {
	case formatJSON:
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	case formatGitHubActions:
		writeGitHubActions(w, report)
		return nil
	case formatSARIF:
		return writeSARIF(w, report)
	default:
		writeText(w, report)
		return nil
	}
}

func writeSARIF(w io.Writer, report lintReport) error {
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

func writeText(w io.Writer, report lintReport) {
	if report.Error != "" {
		fmt.Fprintf(w, "error: %s\n", report.Error)
		return
	}
	if len(report.Findings) == 0 {
		fmt.Fprintln(w, "No lint findings.")
		return
	}
	for _, f := range report.Findings {
		fmt.Fprintln(w, lint.Describe(f))
	}
	fmt.Fprintf(w, "\n%d finding(s).\n", len(report.Findings))
}

func writeGitHubActions(w io.Writer, report lintReport) {
	if report.Error != "" {
		fmt.Fprintf(w, "::error::%s\n", escapeGHData(report.Error))
		return
	}
	for _, f := range report.Findings {
		level := "warning"
		if f.Severity == lint.SeverityError {
			level = "error"
		}
		file := escapeGHProperty(f.File)
		message := escapeGHData(fmt.Sprintf("%s: %s", f.Rule, f.Message))
		if f.Line > 0 {
			fmt.Fprintf(w, "::%s file=%s,line=%d::%s\n", level, file, f.Line, message)
			continue
		}
		fmt.Fprintf(w, "::%s file=%s::%s\n", level, file, message)
	}
}

// escapeGHData escapes message text for GitHub Actions workflow commands.
func escapeGHData(s string) string {
	s = strings.ReplaceAll(s, "%", "%25")
	s = strings.ReplaceAll(s, "\r", "%0D")
	s = strings.ReplaceAll(s, "\n", "%0A")
	return s
}

// escapeGHProperty escapes property values (file=...) for workflow commands,
// which additionally reserve the separator characters.
func escapeGHProperty(s string) string {
	s = escapeGHData(s)
	s = strings.ReplaceAll(s, ":", "%3A")
	s = strings.ReplaceAll(s, ",", "%2C")
	return s
}

func writeError(w io.Writer, format, failOn, msg string) error {
	report := lintReport{
		Failed:           true,
		FailureThreshold: failOn,
		Findings:         []lint.Finding{},
		Error:            msg,
	}
	_ = writeReport(w, format, report)
	return exitcode.New(2, errors.New(msg))
}

func validateFormat(format string) error {
	switch format {
	case formatText, formatJSON, formatGitHubActions, formatSARIF:
		return nil
	default:
		return fmt.Errorf("invalid --format value %q: expected text, json, github-actions, or sarif", format)
	}
}

func validateFailOn(failOn string) error {
	switch failOn {
	case failOnError, failOnAny, failOnNone:
		return nil
	default:
		return fmt.Errorf("invalid --fail-on value %q: expected error, any, or none", failOn)
	}
}

func validateDialect(dialect string) error {
	if isValidDialect(dialect) {
		return nil
	}
	return fmt.Errorf("invalid --dialect value %q: expected postgres, mysql, mariadb, sqlite, clickhouse, cockroachdb, yugabytedb, or spanner", dialect)
}

func isValidDialect(dialect string) bool {
	switch dialect {
	case "", "postgres", "mysql", "mariadb", "sqlite", "clickhouse", "cockroachdb", "yugabytedb", "spanner":
		return true
	default:
		return false
	}
}

// validateDir reports a usable error when the migrations directory itself is
// missing, instead of the misleading "no *.sql migration files found".
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
