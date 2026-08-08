package migrationlintgate_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migrationlintgate"
	"go.5x5.cz/ptah/migration/lint"
)

func TestAnalyze_HappyPath_AppliesPolicy(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		lint.ConfigFileName: {
			Data: []byte("dialect: sqlite\ndisabled-rules:\n  - DS102\n"),
		},
		"0000000001_drop_column.up.sql": {
			Data: []byte("ALTER TABLE users DROP COLUMN legacy;\n"),
		},
		"0000000001_drop_column.down.sql": {
			Data: []byte("ALTER TABLE users ADD COLUMN legacy TEXT;\n"),
		},
	}

	findings, err := migrationlintgate.Analyze(fsys, []int64{1}, "sqlite", "")

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0)
}

func TestLoadPolicy_FailurePath_DialectMismatch(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		lint.ConfigFileName: {
			Data: []byte("dialect: postgres\n"),
		},
	}

	_, err := migrationlintgate.LoadPolicy(fsys, "sqlite")

	c.Assert(err, qt.ErrorMatches, `lint dialect "postgres" does not match database dialect "sqlite"`)
}

func TestAnalyze_FailurePath_InvalidPolicy(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		lint.ConfigFileName: {
			Data: []byte("rules:\n  DS101:\n    exclude:\n      - '[legacy/**'\n"),
		},
		"0000000001_drop.up.sql": {
			Data: []byte("DROP TABLE users;\n"),
		},
		"0000000001_drop.down.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER);\n"),
		},
	}

	findings, err := migrationlintgate.Analyze(fsys, []int64{1}, "sqlite", "")

	c.Assert(err, qt.ErrorMatches, `.*rule DS101 has invalid exclude pattern "\[legacy/\*\*": syntax error in pattern`)
	c.Assert(findings, qt.IsNil)
}

func TestLoadPolicy_FailurePath_UnregisteredRuleSelectors(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name   string
		config string
	}{
		{
			name:   "disabled rule selector",
			config: "disabled-rules:\n  - ZZ404\n",
		},
		{
			name:   "configured rule selector",
			config: "rules:\n  ZZ404:\n    severity: error\n",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			fsys := fstest.MapFS{
				lint.ConfigFileName: {Data: []byte(test.config)},
			}

			_, err := migrationlintgate.LoadPolicy(fsys, "sqlite")

			c.Assert(err, qt.ErrorMatches, `rule selector "ZZ404" does not match any registered rule`)
		})
	}
}

func TestLoadPolicy_FailurePath_NonNormalizedExclusionGlob(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		lint.ConfigFileName: {
			Data: []byte("rules:\n  DS101:\n    exclude:\n      - '**/../**'\n"),
		},
	}

	_, err := migrationlintgate.LoadPolicy(fsys, "sqlite")

	c.Assert(err, qt.ErrorMatches, `.*rule DS101 has invalid exclude pattern "\*\*/\.\./\*\*": pattern must be a normalized slash-separated path`)
}
