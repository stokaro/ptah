package onlineddl_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/onlineddl"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ptah.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfig(t *testing.T) {
	c := qt.New(t)

	path := writeConfig(t, "online_ddl:\n  tool: ghost\n  threshold_rows: 1000000\n  fallback: error\n  args:\n    - --allow-on-master\n    - --max-load=Threads_running=25\n")
	cfg, err := onlineddl.LoadConfig(path)
	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Tool, qt.Equals, onlineddl.ToolGhost)
	c.Assert(cfg.ThresholdRows, qt.Equals, int64(1000000))
	c.Assert(cfg.Fallback, qt.Equals, onlineddl.FallbackError)
	c.Assert(cfg.Args, qt.DeepEquals, []string{"--allow-on-master", "--max-load=Threads_running=25"})
	c.Assert(cfg.Enabled(), qt.IsTrue)
}

func TestLoadConfig_AllowsProjectConfigEnvelope(t *testing.T) {
	c := qt.New(t)

	path := writeConfig(t, `url: postgres://app/db
schemas: [public]
migration:
  dir: ./migrations
lint:
  dialect: postgres
env:
  prod:
    url: postgres://prod/db
online_ddl:
  tool: ghost
  threshold_rows: 1000000
`)
	cfg, err := onlineddl.LoadConfig(path)

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Tool, qt.Equals, onlineddl.ToolGhost)
	c.Assert(cfg.Enabled(), qt.IsTrue)
}

func TestLoadConfigForEnv_MergesNamedEnvironment(t *testing.T) {
	c := qt.New(t)

	path := writeConfig(t, `online_ddl:
  tool: ghost
  threshold_rows: 1000000
env:
  prod:
    online_ddl:
      threshold_rows: 2000000
      fallback: error
      args: ["--allow-on-master"]
`)
	cfg, err := onlineddl.LoadConfigForEnv(path, "prod")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Tool, qt.Equals, onlineddl.ToolGhost)
	c.Assert(cfg.ThresholdRows, qt.Equals, int64(2000000))
	c.Assert(cfg.Fallback, qt.Equals, onlineddl.FallbackError)
	c.Assert(cfg.Args, qt.DeepEquals, []string{"--allow-on-master"})
}

func TestLoadConfigForEnv_ClearsInheritedEnvironmentFields(t *testing.T) {
	c := qt.New(t)

	path := writeConfig(t, `online_ddl:
  tool: ghost
  threshold_rows: 1000000
  fallback: error
  args: ["--allow-on-master"]
env:
  prod:
    online_ddl:
      tool: ""
      threshold_rows: 0
      fallback: ""
      args: []
`)
	cfg, err := onlineddl.LoadConfigForEnv(path, "prod")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Tool, qt.Equals, "")
	c.Assert(cfg.ThresholdRows, qt.Equals, int64(0))
	c.Assert(cfg.Fallback, qt.Equals, "")
	c.Assert(cfg.Args, qt.DeepEquals, []string{})
	c.Assert(cfg.Enabled(), qt.IsFalse)
}

func TestLoadConfig_MissingFileYieldsDisabledConfig(t *testing.T) {
	c := qt.New(t)

	cfg, err := onlineddl.LoadConfig(filepath.Join(t.TempDir(), "nope.yaml"))
	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Tool, qt.Equals, "")
	c.Assert(cfg.Enabled(), qt.IsFalse)
}

func TestLoadConfig_Invalid(t *testing.T) {
	c := qt.New(t)

	_, err := onlineddl.LoadConfig(writeConfig(t, "online_ddl: [broken"))
	c.Assert(err, qt.ErrorMatches, "failed to parse ptah config .*")

	_, err = onlineddl.LoadConfig(writeConfig(t, "online_ddl:\n  tool: liquibase\n"))
	c.Assert(err, qt.ErrorMatches, `invalid online_ddl config .*unknown online_ddl tool "liquibase".*`)

	_, err = onlineddl.LoadConfig(writeConfig(t, "online_ddl:\n  tool: ghost\n  threshold_rows: -5\n"))
	c.Assert(err, qt.ErrorMatches, ".*threshold_rows must not be negative.*")

	_, err = onlineddl.LoadConfig(writeConfig(t, "online_ddl:\n  threshold_rows: 100\n"))
	c.Assert(err, qt.ErrorMatches, ".*threshold_rows is set but no tool is configured.*")

	_, err = onlineddl.LoadConfig(writeConfig(t, "online_ddl:\n  tool: ghost\n  fallback: warn\n"))
	c.Assert(err, qt.ErrorMatches, `invalid online_ddl config .*unknown online_ddl fallback "warn".*`)

	_, err = onlineddl.LoadConfig(writeConfig(t, "urll: postgres://typo/db\n"))
	c.Assert(err, qt.ErrorMatches, `(?s)failed to parse ptah config .*field urll not found.*`)

	_, err = onlineddl.LoadConfig(writeConfig(t, "migration:\n  dri: ./typo\n"))
	c.Assert(err, qt.ErrorMatches, `(?s)failed to parse ptah config .*field dri not found.*`)

	_, err = onlineddl.LoadConfig(writeConfig(t, "online_ddl:\n  threshhold_rows: 100\n"))
	c.Assert(err, qt.ErrorMatches, `(?s)failed to parse ptah config .*field threshhold_rows not found.*`)
}

func TestConfig_Enabled(t *testing.T) {
	c := qt.New(t)

	c.Assert(onlineddl.Config{}.Enabled(), qt.IsFalse)
	c.Assert(onlineddl.Config{Tool: onlineddl.ToolGhost}.Enabled(), qt.IsFalse,
		qt.Commentf("a tool without a threshold cannot auto-route"))
	c.Assert(onlineddl.Config{Tool: onlineddl.ToolGhost, ThresholdRows: 1}.Enabled(), qt.IsTrue)
}
