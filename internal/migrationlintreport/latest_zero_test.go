package migrationlintreport_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/migrationlintreport"
	migrationlint "go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestBuild_AtlasExplicitZeroAllowsExplicitGitSelector(t *testing.T) {
	c := qt.New(t)
	gitDir := t.TempDir()

	_, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:           t.TempDir(),
		FS:            fstest.MapFS{"1_init.sql": {Data: []byte("CREATE TABLE users (id int);\n")}},
		DirFormat:     string(migrator.MigrationDirFormatAtlas),
		Dialect:       "sqlite",
		GitBase:       "HEAD",
		GitDir:        gitDir,
		FailOn:        migrationlintreport.FailOnNone,
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		Changed: migrationlintreport.ChangedOptions{
			Dialect: true,
			GitBase: true,
			Latest:  true,
		},
	}, projectconfig.Config{})

	c.Assert(err, qt.ErrorMatches, `find git repository root: .*`)
}

func TestBuild_AtlasExplicitZeroSuppressesProjectLatestAndAllowsProjectGit(t *testing.T) {
	c := qt.New(t)
	latest := 1
	cfg := projectconfig.Config{
		Lint: projectconfig.LintConfig{
			Latest:  &latest,
			GitBase: "-unsafe",
			GitDir:  "/not/a/repository",
		},
	}

	_, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:           t.TempDir(),
		FS:            fstest.MapFS{"1_init.sql": {Data: []byte("CREATE TABLE users (id int);\n")}},
		DirFormat:     string(migrator.MigrationDirFormatAtlas),
		Dialect:       "sqlite",
		FailOn:        migrationlintreport.FailOnNone,
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		Changed: migrationlintreport.ChangedOptions{
			Dialect: true,
			Latest:  true,
		},
	}, cfg)

	c.Assert(err, qt.ErrorMatches, `--git-base "-unsafe" is not a safe Git ref`)
}

func TestBuild_NativeExplicitZeroKeepsPreciseRefusal(t *testing.T) {
	c := qt.New(t)

	_, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		Dir:       t.TempDir(),
		FS:        fstest.MapFS{"1_init.sql": {Data: []byte("CREATE TABLE users (id int);\n")}},
		DirFormat: string(migrator.MigrationDirFormatAtlas),
		Dialect:   "sqlite",
		FailOn:    migrationlintreport.FailOnNone,
		Changed: migrationlintreport.ChangedOptions{
			Dialect: true,
			Latest:  true,
		},
	}, projectconfig.Config{})

	c.Assert(err, qt.ErrorMatches, `--latest must be greater than zero`)
}
