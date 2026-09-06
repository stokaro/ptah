package migrationlintreport_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/migrationlintreport"
	migrationlint "ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

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

	_, err := migrationlintreport.Build(t.Context(), migrationlintreport.Options{
		Dir:           "unused",
		FS:            fstest.MapFS{"1_init.sql": {Data: []byte("CREATE TABLE users (id int);\n")}},
		DirFormat:     string(migrationfile.DirFormatAtlas),
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

	_, err := migrationlintreport.Build(t.Context(), migrationlintreport.Options{
		Dir:       "unused",
		FS:        fstest.MapFS{"1_init.sql": {Data: []byte("CREATE TABLE users (id int);\n")}},
		DirFormat: string(migrationfile.DirFormatAtlas),
		Dialect:   "sqlite",
		FailOn:    migrationlintreport.FailOnNone,
		Changed: migrationlintreport.ChangedOptions{
			Dialect: true,
			Latest:  true,
		},
	}, projectconfig.Config{})

	c.Assert(err, qt.ErrorMatches, `--latest must be greater than zero`)
}
