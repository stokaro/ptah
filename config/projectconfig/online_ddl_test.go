package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

func TestParsePtahOnlineDDLBase(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParsePtah([]byte(`online_ddl:
  tool: ghost
  threshold_rows: 1000000
  args: ["--allow-on-master", "--max-load=Threads_running=25"]
  fallback: error
`), "base.yaml", "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.OnlineDDL, qt.DeepEquals, projectconfig.OnlineDDLConfig{
		Tool:          projectconfig.OnlineDDLToolGhost,
		ThresholdRows: 1000000,
		Args:          []string{"--allow-on-master", "--max-load=Threads_running=25"},
		Fallback:      projectconfig.OnlineDDLFallbackError,
	})
	c.Assert(cfg.OnlineDDL.Enabled(), qt.IsTrue)
}

func TestParsePtahOnlineDDLNamedEnvInheritsAndOverrides(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParsePtah([]byte(`online_ddl:
  tool: ghost
  threshold_rows: 1000000
  args: ["--base"]
  fallback: plain
env:
  prod:
    online_ddl:
      threshold_rows: 2000000
      args: ["--prod"]
      fallback: error
`), "ptah.yaml", "prod")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.EnvName, qt.Equals, "prod")
	c.Assert(cfg.OnlineDDL, qt.DeepEquals, projectconfig.OnlineDDLConfig{
		Tool:          projectconfig.OnlineDDLToolGhost,
		ThresholdRows: 2000000,
		Args:          []string{"--prod"},
		Fallback:      projectconfig.OnlineDDLFallbackError,
	})
}

func TestParsePtahOnlineDDLEnvFragmentIsValidatedAfterMerge(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParsePtah([]byte(`online_ddl:
  tool: ghost
env:
  prod:
    online_ddl:
      threshold_rows: 250000
  broken:
    online_ddl:
      tool: liquibase
`), "ptah.yaml", "prod")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.OnlineDDL.Tool, qt.Equals, projectconfig.OnlineDDLToolGhost)
	c.Assert(cfg.OnlineDDL.ThresholdRows, qt.Equals, int64(250000))
	c.Assert(cfg.OnlineDDL.Enabled(), qt.IsTrue)
}

func TestParsePtahOnlineDDLExplicitEmptyToolClearsThreshold(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParsePtah([]byte(`online_ddl:
  tool: ghost
  threshold_rows: 1000000
  args: ["--base"]
  fallback: error
env:
  prod:
    online_ddl:
      tool: ""
      args: []
      fallback: ""
`), "ptah.yaml", "prod")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.OnlineDDL, qt.DeepEquals, projectconfig.OnlineDDLConfig{
		Args: make([]string, 0),
	})
	c.Assert(cfg.OnlineDDL.Enabled(), qt.IsFalse)
}

func TestParsePtahOnlineDDLExplicitZeroThresholdOverridesBase(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParsePtah([]byte(`online_ddl:
  tool: ghost
  threshold_rows: 1000000
env:
  prod:
    online_ddl:
      threshold_rows: 0
`), "ptah.yaml", "prod")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.OnlineDDL.Tool, qt.Equals, projectconfig.OnlineDDLToolGhost)
	c.Assert(cfg.OnlineDDL.ThresholdRows, qt.Equals, int64(0))
	c.Assert(cfg.OnlineDDL.Enabled(), qt.IsFalse)
}

func TestParsePtahOnlineDDLSelectsImplicitSingleEnv(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParsePtah([]byte(`online_ddl:
  tool: ghost
  threshold_rows: 100
env:
  local:
    online_ddl:
      tool: pt-osc
      threshold_rows: 200
`), "ptah.yaml", "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.EnvName, qt.Equals, "local")
	c.Assert(cfg.OnlineDDL.Tool, qt.Equals, projectconfig.OnlineDDLToolPTOSC)
	c.Assert(cfg.OnlineDDL.ThresholdRows, qt.Equals, int64(200))
}

func TestParsePtahOnlineDDLValidationFailures(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		filename string
		envName  string
		wantErr  string
	}{
		{
			name:     "unknown base tool",
			raw:      "online_ddl:\n  tool: liquibase\n",
			filename: "base.yaml",
			wantErr:  `invalid online_ddl config in base\.yaml: unknown online_ddl tool "liquibase": expected ghost or pt-osc`,
		},
		{
			name: "negative selected env threshold",
			raw: `online_ddl:
  tool: ghost
env:
  prod:
    online_ddl:
      threshold_rows: -5
`,
			filename: "custom.yaml",
			envName:  "prod",
			wantErr: `invalid online_ddl config in custom\.yaml for env "prod": ` +
				`online_ddl threshold_rows must not be negative, got -5`,
		},
		{
			name:     "threshold without tool",
			raw:      "online_ddl:\n  threshold_rows: 100\n",
			filename: "base.yaml",
			wantErr: `invalid online_ddl config in base\.yaml: ` +
				`online_ddl threshold_rows is set but no tool is configured`,
		},
		{
			name:     "unknown fallback",
			raw:      "online_ddl:\n  tool: ghost\n  fallback: warn\n",
			filename: "base.yaml",
			wantErr: `invalid online_ddl config in base\.yaml: ` +
				`unknown online_ddl fallback "warn": expected error or plain`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := projectconfig.ParsePtah(
				[]byte(test.raw),
				test.filename,
				test.envName,
			)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestOnlineDDLConfigConstants(t *testing.T) {
	c := qt.New(t)

	c.Assert(projectconfig.OnlineDDLToolGhost, qt.Equals, "ghost")
	c.Assert(projectconfig.OnlineDDLToolPTOSC, qt.Equals, "pt-osc")
	c.Assert(projectconfig.OnlineDDLFallbackError, qt.Equals, "error")
	c.Assert(projectconfig.OnlineDDLFallbackPlain, qt.Equals, "plain")
}

func TestOnlineDDLConfigEnabled(t *testing.T) {
	tests := []struct {
		name string
		cfg  projectconfig.OnlineDDLConfig
		want bool
	}{
		{
			name: "empty config",
		},
		{
			name: "tool without threshold",
			cfg: projectconfig.OnlineDDLConfig{
				Tool: projectconfig.OnlineDDLToolGhost,
			},
		},
		{
			name: "tool with threshold",
			cfg: projectconfig.OnlineDDLConfig{
				Tool:          projectconfig.OnlineDDLToolGhost,
				ThresholdRows: 1,
			},
			want: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.cfg.Enabled(), qt.Equals, test.want)
		})
	}
}

func TestMergeOnlineDDLClonesArgs(t *testing.T) {
	c := qt.New(t)
	base := projectconfig.Config{
		OnlineDDL: projectconfig.OnlineDDLConfig{
			Args: []string{"--base"},
		},
	}
	override := projectconfig.Config{
		OnlineDDL: projectconfig.OnlineDDLConfig{
			Args: []string{"--override"},
		},
	}

	fromBase := projectconfig.Merge(base, projectconfig.Config{})
	fromOverride := projectconfig.Merge(projectconfig.Config{}, override)
	base.OnlineDDL.Args[0] = "--mutated-base"
	override.OnlineDDL.Args[0] = "--mutated-override"

	c.Assert(fromBase.OnlineDDL.Args, qt.DeepEquals, []string{"--base"})
	c.Assert(fromOverride.OnlineDDL.Args, qt.DeepEquals, []string{"--override"})
}
