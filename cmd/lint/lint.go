// Package lint implements the migration lint command: a sqlcheck-style linter
// for migration directories with rule-coded findings (issue #151).
package lint

import (
	"errors"
	"io"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/internal/migrationlintreport"
	migrationlint "github.com/stokaro/ptah/migration/lint"
	"github.com/stokaro/ptah/migration/migrator"
)

const (
	formatText          = migrationlintreport.FormatText
	formatJSON          = migrationlintreport.FormatJSON
	formatGitHubActions = migrationlintreport.FormatGitHubActions
	formatSARIF         = migrationlintreport.FormatSARIF

	failOnError = migrationlintreport.FailOnError

	latestFlag  = "latest"
	gitBaseFlag = "git-base"
	gitDirFlag  = "git-dir"
)

var errLintFindings = errors.New("lint findings exceed the failure threshold")

// NewLintCommand returns the migration-linter command.
func NewLintCommand() *cobra.Command {
	var dir string
	var dirFormat string
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
				dirFormat:  dirFormat,
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
	cmd.Flags().StringVar(&dirFormat, "dir-format", string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	cmd.Flags().StringVar(&dialect, "dialect", "", "Target dialect gating dialect-specific rules: postgres, mysql, mariadb, sqlite, clickhouse, cockroachdb, yugabytedb, or spanner (empty runs every rule)")
	cmd.Flags().StringVar(&format, "format", formatText, "Output format: text, json, github-actions, sarif")
	cmd.Flags().StringVar(&configPath, "config", "", "Path to a lint config file (default: <dir>/"+migrationlint.ConfigFileName+" when present)")
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
	dirFormat  string
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

func runLint(cmd *cobra.Command, opts runOptions) error {
	if err := migrationlintreport.ValidateFormat(opts.format); err != nil {
		return writeError(cmd.ErrOrStderr(), formatText, opts.failOn, err.Error())
	}
	if err := migrationlintreport.ValidateFailOn(opts.failOn); err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, failOnError, err.Error())
	}
	projectCfg, err := dbcli.LoadProjectConfig(cmd, "")
	if err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, opts.failOn, err.Error())
	}
	report, err := migrationlintreport.Build(cmd.Context(), reportOptions(cmd, opts), projectCfg)
	if err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, opts.failOn, err.Error())
	}

	writer := lintReportWriter(cmd.OutOrStdout(), cmd.ErrOrStderr(), report)
	if err := migrationlintreport.Write(writer, opts.format, report); err != nil {
		return writeError(cmd.ErrOrStderr(), formatText, opts.failOn, err.Error())
	}
	if report.Failed {
		return exitcode.New(1, errLintFindings)
	}
	return nil
}

func reportOptions(cmd *cobra.Command, opts runOptions) migrationlintreport.Options {
	return migrationlintreport.Options{
		Dir:        opts.dir,
		DirFormat:  opts.dirFormat,
		Dialect:    opts.dialect,
		ConfigPath: opts.configPath,
		AtlasEnv:   opts.atlasEnv,
		DevURL:     opts.devURL,
		GitBase:    opts.gitBase,
		GitDir:     opts.gitDir,
		Disabled:   opts.disabled,
		FailOn:     opts.failOn,
		Latest:     opts.latest,
		Positional: opts.positional,
		Changed: migrationlintreport.ChangedOptions{
			Dir:       cmd.Flags().Changed("dir"),
			DirFormat: cmd.Flags().Changed("dir-format"),
			Dialect:   cmd.Flags().Changed("dialect"),
			AtlasEnv:  cmd.Flags().Changed("atlas-env"),
			DevURL:    cmd.Flags().Changed("dev-url"),
			GitBase:   cmd.Flags().Changed(gitBaseFlag),
			GitDir:    cmd.Flags().Changed(gitDirFlag),
			Latest:    cmd.Flags().Changed(latestFlag),
		},
	}
}

func lintReportWriter(stdout, stderr io.Writer, report migrationlintreport.Report) io.Writer {
	if report.Failed {
		return stderr
	}
	return stdout
}

func writeError(w io.Writer, format, failOn, msg string) error {
	_ = migrationlintreport.Write(w, format, migrationlintreport.ErrorReport(failOn, msg))
	return exitcode.New(2, errors.New(msg))
}
