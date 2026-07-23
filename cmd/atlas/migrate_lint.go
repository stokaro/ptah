package atlas

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	lintcmd "github.com/stokaro/ptah/cmd/lint"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/internal/atlasargs"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/atlasurl"
	"github.com/stokaro/ptah/internal/pathguard"
)

const atlasMigrateLintFindingError = "lint findings exceed the failure threshold"

type atlasMigrateLintOptions struct {
	devURL    string
	dir       string
	dirFormat string
	format    string
	latest    uint
	gitBase   string
	gitDir    string
	atlasEnv  string
}

func newAtlasMigrateLintCommand() *cobra.Command {
	opts := atlasMigrateLintOptions{
		dir:       "file://migrations",
		dirFormat: atlasDirFormatDefault,
		gitDir:    ".",
	}
	cmd := &cobra.Command{
		Use:   "lint",
		Short: "Lint migration files",
		Long: `Run Atlas-compatible migration lint checks over a local migration directory.

Native Ptah equivalent: ptah migrations lint.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasMigrateLint(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL")
	flags.StringVar(&opts.dir, "dir", opts.dir, "Migration directory URL")
	flags.StringVar(&opts.dirFormat, "dir-format", opts.dirFormat, "Migration directory format")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	flags.UintVar(&opts.latest, "latest", 0, "Number of latest migrations to lint")
	flags.StringVar(&opts.gitBase, "git-base", "", "Base Git branch for changeset linting")
	flags.StringVar(&opts.gitDir, "git-dir", opts.gitDir, "Repository working directory for --git-base")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAtlasMigrateLint(cmd *cobra.Command, opts atlasMigrateLintOptions) error {
	formatOutput := cmd.Flags().Changed("format")
	projectCfg, loaded, err := loadOptionalAtlasProjectConfigForCommand(cmd)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if loaded {
		opts.devURL = dbcli.EffectiveString(cmd, "dev-url", opts.devURL, projectCfg.DevURL)
		opts.dir = dbcli.EffectiveString(cmd, "dir", opts.dir, projectCfg.Migration.Dir)
		opts.dirFormat = dbcli.EffectiveString(cmd, "dir-format", opts.dirFormat, projectCfg.Migration.Format)
		opts.atlasEnv = dbcli.EffectiveString(cmd, dbcli.EnvFlagName, opts.atlasEnv, projectCfg.EnvName)
		opts.gitBase = dbcli.EffectiveString(cmd, "git-base", opts.gitBase, projectCfg.Lint.GitBase)
		opts.gitDir = dbcli.EffectiveString(cmd, "git-dir", opts.gitDir, projectCfg.Lint.GitDir)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, projectCfg.Format.Migrate.Lint)
		formatOutput = formatOutput || projectCfg.Format.Migrate.Lint != ""
	}
	if loaded && !cmd.Flags().Changed("dir") && projectCfg.Migration.Dir != "" {
		opts.dir, err = atlasProjectConfigLocalDir(cmd, opts.dir)
		if err != nil {
			return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate lint --dir: %w", err))
		}
	}
	if err := validateAtlasMigrateLintOptions(opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	dirFormat, err := atlasMigrateDirFormatValue(opts.dirFormat)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate lint --dir-format: %w", err))
	}
	if formatOutput {
		if err := validateAtlasMigrateLintFormat(opts.format); err != nil {
			return cmdutil.Fail(cmd, err)
		}
		if err := atlasreport.ValidateMigrateLintTemplate(opts.format); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	dir, err := atlasargs.LocalDirValue(opts.dir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate lint --dir: %w", err))
	}
	dir, err = pathguard.ResolveCLIPath(dir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("resolve migration directory: %w", err))
	}
	fsys := os.DirFS(dir)
	integrity, err := atlasreport.InspectMigrateLintIntegrity(fsys)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if integrity.Failed() {
		if formatOutput {
			driver, err := atlasurl.DialectFromURL(opts.devURL)
			if err != nil {
				return cmdutil.Fail(cmd, err)
			}
			if err := atlasreport.WriteMigrateLintFormat(cmd.OutOrStdout(), opts.format, atlasreport.MigrateLintOptions{
				Driver:    driver,
				URL:       opts.devURL,
				Dir:       dir,
				FS:        fsys,
				Integrity: integrity,
			}); err != nil {
				return cmdutil.Fail(cmd, err)
			}
		}
		if !formatOutput {
			fmt.Fprintln(cmd.ErrOrStderr(), integrity.Error)
		}
		return exitcode.New(1, errors.New(integrity.Error))
	}

	report, err := lintcmd.BuildReportWithConfig(cmd, lintcmd.ReportOptions{
		Dir:       dir,
		DirFormat: dirFormat,
		AtlasEnv:  opts.atlasEnv,
		DevURL:    opts.devURL,
		GitBase:   opts.gitBase,
		GitDir:    opts.gitDir,
		FailOn:    "error",
		Latest:    opts.latest,
	}, atlasMigrateLintReportConfig(projectCfg))
	if err != nil {
		if formatOutput {
			if err := writeAtlasMigrateLintReplayError(cmd, opts, dir, fsys, integrity, err); err != nil {
				return cmdutil.Fail(cmd, err)
			}
			return exitcode.New(1, err)
		}
		return cmdutil.Fail(cmd, err)
	}
	if formatOutput {
		if err := atlasreport.WriteMigrateLintFormat(cmd.OutOrStdout(), opts.format, atlasreport.MigrateLintOptions{
			Driver:    report.Dialect,
			URL:       opts.devURL,
			Dir:       dir,
			FS:        fsys,
			Findings:  report.Findings,
			Versions:  report.Versions,
			Integrity: integrity,
			Error:     report.Error,
		}); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	} else if err := lintcmd.WriteReport(lintReportWriter(cmd, report), "text", report); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if report.Failed {
		return exitcode.New(1, errors.New(atlasMigrateLintFindingError))
	}
	return nil
}

func atlasMigrateLintReportConfig(projectCfg projectconfig.Config) projectconfig.Config {
	projectCfg.Migration.Dir = ""
	return projectCfg
}

func writeAtlasMigrateLintReplayError(
	cmd *cobra.Command,
	opts atlasMigrateLintOptions,
	dir string,
	fsys fs.FS,
	integrity atlasreport.MigrateLintIntegrity,
	replayErr error,
) error {
	driver, err := atlasurl.DialectFromURL(opts.devURL)
	if err != nil {
		return err
	}
	return atlasreport.WriteMigrateLintFormat(cmd.OutOrStdout(), opts.format, atlasreport.MigrateLintOptions{
		Driver:    driver,
		URL:       opts.devURL,
		Dir:       dir,
		FS:        fsys,
		Integrity: integrity,
		Error:     replayErr.Error(),
	})
}

func lintReportWriter(cmd *cobra.Command, report lintcmd.Report) io.Writer {
	if report.Failed {
		return cmd.ErrOrStderr()
	}
	return cmd.OutOrStdout()
}

func validateAtlasMigrateLintOptions(opts atlasMigrateLintOptions) error {
	if opts.dir == "" {
		return fmt.Errorf("migrations directory is required")
	}
	return nil
}

func validateAtlasMigrateLintFormat(format string) error {
	if strings.TrimSpace(format) == "" {
		return fmt.Errorf("--format must not be empty")
	}
	return nil
}
