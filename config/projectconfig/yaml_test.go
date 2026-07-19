package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/config/projectconfig"
)

func TestParsePtahProjectConfigNamedEnv(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`url: postgres://base/db
dev: postgres://base/dev
schemas: [base]
migration:
  dir: ./base-migrations
  format: ptah
  revisions_schema: base_schema
  revisions_table: base_revisions
  revision_format: ptah
  lock_timeout: 1s
  statement_timeout: 2s
  connect_timeout: 3s
  migration_lock_timeout: 4s
  exec_order: linear
lint:
  dialect: mysql
  disabled-rules: [MF103]
env:
  prod:
    url: postgres://prod/db
    dev: postgres://prod/dev
    schemas: [public, tenant]
    migration:
      dir: ./prod-migrations
      revisions_schema: prod_schema
      revisions_table: prod_revisions
      lock_timeout: 5s
      statement_timeout: 6s
      connect_timeout: 7s
      migration_lock_timeout: 8s
      exec_order: non-linear
    lint:
      dialect: postgres
      disabled-rules: [DS103]
`)

	cfg, err := projectconfig.ParsePtah(raw, "ptah.yaml", "prod")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.EnvName, qt.Equals, "prod")
	c.Assert(cfg.DatabaseURL, qt.Equals, "postgres://prod/db")
	c.Assert(cfg.DevURL, qt.Equals, "postgres://prod/dev")
	c.Assert(cfg.Schemas, qt.DeepEquals, []string{"public", "tenant"})
	c.Assert(cfg.Migration.Dir, qt.Equals, "./prod-migrations")
	c.Assert(cfg.Migration.Format, qt.Equals, "ptah")
	c.Assert(cfg.Migration.RevisionsSchema, qt.Equals, "prod_schema")
	c.Assert(cfg.Migration.RevisionsTable, qt.Equals, "prod_revisions")
	c.Assert(cfg.Migration.RevisionFormat, qt.Equals, "ptah")
	c.Assert(cfg.Migration.LockTimeout, qt.Equals, "5s")
	c.Assert(cfg.Migration.StatementTimeout, qt.Equals, "6s")
	c.Assert(cfg.Migration.ConnectTimeout, qt.Equals, "7s")
	c.Assert(cfg.Migration.MigrationLockTimeout, qt.Equals, "8s")
	c.Assert(cfg.Migration.ExecOrder, qt.Equals, "non-linear")
	c.Assert(cfg.Lint.Dialect, qt.Equals, "postgres")
	c.Assert(cfg.Lint.DisabledRules, qt.DeepEquals, []string{"DS103"})
}

func TestParsePtahProjectConfigRejectsUnknownKeys(t *testing.T) {
	c := qt.New(t)

	_, err := projectconfig.ParsePtah([]byte(`url: postgres://app/db
urll: postgres://typo/db
`), "ptah.yaml", "")

	c.Assert(err, qt.ErrorMatches, `failed to parse ptah config ptah\.yaml: yaml: unmarshal errors:\n  line 2: field urll not found in type projectconfig\.yamlDocument`)
}

func TestParsePtahProjectConfigRejectsUnknownOnlineDDLKeys(t *testing.T) {
	c := qt.New(t)

	_, err := projectconfig.ParsePtah([]byte(`online_ddl:
  threshhold_rows: 100
`), "ptah.yaml", "")
	c.Assert(err, qt.ErrorMatches, `(?s)failed to parse ptah config ptah\.yaml: .*field threshhold_rows not found.*`)

	_, err = projectconfig.ParsePtah([]byte(`env:
  prod:
    online_ddl:
      tooll: ghost
`), "ptah.yaml", "prod")
	c.Assert(err, qt.ErrorMatches, `(?s)failed to parse ptah config ptah\.yaml: .*field tooll not found.*`)
}

func TestParsePtahProjectConfigAllowsOnlineDDLSection(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`online_ddl:
  tool: ghost
  threshold_rows: 1000000
env:
  prod:
    url: postgres://prod/db
    online_ddl:
      fallback: error
`)

	cfg, err := projectconfig.ParsePtah(raw, "ptah.yaml", "prod")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.EnvName, qt.Equals, "prod")
	c.Assert(cfg.DatabaseURL, qt.Equals, "postgres://prod/db")
}

func TestParsePtahProjectConfigEnvCanClearInheritedLists(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`schemas: [public]
exclude: [tmp_*]
lint:
  disabled-rules: [DS]
env:
  prod:
    schemas: []
    exclude: []
    lint:
      disabled-rules: []
`)

	cfg, err := projectconfig.ParsePtah(raw, "ptah.yaml", "prod")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Schemas, qt.DeepEquals, []string{})
	c.Assert(cfg.Exclude, qt.DeepEquals, []string{})
	c.Assert(cfg.Lint.DisabledRules, qt.DeepEquals, []string{})
}

func TestParsePtahProjectConfigSelectsSingleEnv(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env:
  local:
    url: postgres://local/db
`)

	cfg, err := projectconfig.ParsePtah(raw, "ptah.yaml", "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.EnvName, qt.Equals, "local")
	c.Assert(cfg.DatabaseURL, qt.Equals, "postgres://local/db")
}

func TestParsePtahProjectConfigRequiresEnvWhenMultipleEnvsExist(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env:
  dev:
    url: postgres://dev/db
  prod:
    url: postgres://prod/db
`)

	_, err := projectconfig.ParsePtah(raw, "ptah.yaml", "")

	c.Assert(err, qt.ErrorMatches, `ptah\.yaml contains multiple env blocks; pass --env`)
}

func TestParsePtahProjectConfigMissingEnv(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env:
  dev:
    url: postgres://dev/db
`)

	_, err := projectconfig.ParsePtah(raw, "ptah.yaml", "prod")

	c.Assert(err, qt.ErrorMatches, `ptah env "prod" not found`)
}
