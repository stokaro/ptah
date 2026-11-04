package projectconfig_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp"

	"ptah.run/config/projectconfig"
	"ptah.run/core/planext"
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
  tx_mode: file
  pre_up_hook: base-backup
  pre_down_hook: base-rollback-backup
  pg_dump_to: ./base-pg-dumps
  mysqldump_to: ./base-mysql-dumps
  webhook: https://hooks.example/base
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
      tx_mode: all
      pre_up_hook: prod-backup
      pre_down_hook: prod-rollback-backup
      pg_dump_to: ./prod-pg-dumps
      mysqldump_to: ./prod-mysql-dumps
      webhook: https://hooks.example/prod
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
	c.Assert(cfg.Migration.TxMode, qt.Equals, "all")
	c.Assert(cfg.Migration.PreUpHook, qt.Equals, "prod-backup")
	c.Assert(cfg.Migration.PreDownHook, qt.Equals, "prod-rollback-backup")
	c.Assert(cfg.Migration.PostgresDumpTo, qt.Equals, "./prod-pg-dumps")
	c.Assert(cfg.Migration.MySQLDumpTo, qt.Equals, "./prod-mysql-dumps")
	c.Assert(cfg.Migration.Webhook, qt.Equals, "https://hooks.example/prod")
	c.Assert(cfg.Lint.Dialect, qt.Equals, "postgres")
	c.Assert(cfg.Lint.DisabledRules, qt.DeepEquals, []string{"DS103"})
}

func TestParsePtahProjectConfigRejectsUnknownKeys(t *testing.T) {
	c := qt.New(t)

	_, err := projectconfig.ParsePtah([]byte(`url: postgres://app/db
urll: postgres://typo/db
`), "ptah.yaml", "")

	c.Assert(err, qt.ErrorMatches, `failed to parse ptah config ptah\.yaml: line 2: unknown ptah\.yaml key "urll"; supported keys are url, dev, schemas, exclude, ignore_extensions, migration, lint, migrate, online_ddl, diff, external_schema, env`)
	// The decoder reports rejected keys against the Go type it decodes into.
	// That name is not a thing the user can look up or act on, so no
	// diagnostic may carry it.
	c.Assert(err.Error(), qt.Not(qt.Contains), "projectconfig")
}

// TestParsePtahEmptyDocument_HappyPath holds every ptah.yaml that sets nothing
// to the configuration a missing file gives. A file holding no YAML document
// at all -- zero bytes, or only comments and blank lines -- reaches the decoder
// as an empty stream rather than as an empty document, and must not be refused
// where `---`, `null` and `{}` are accepted.
//
// The comparison reads unexported fields too: presence is what decides whether
// a merged-over value is kept, and a config that set nothing must record that
// it set nothing.
func TestParsePtahEmptyDocument_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		document string
	}{
		{name: "zero bytes", document: ""},
		{name: "comment only", document: "# settings go here\n"},
		{name: "blank lines", document: "  \n\n"},
		{name: "comments and blank lines without a trailing newline", document: "# one\n\n# two"},
		{name: "explicit empty document", document: "---\n"},
		{name: "null document", document: "null\n"},
		{name: "empty mapping", document: "{}\n"},
	}
	everyField := cmp.Exporter(func(reflect.Type) bool { return true })
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParsePtah([]byte(test.document), "ptah.yaml", "")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg, qt.CmpEquals(everyField), projectconfig.Config{})
		})
	}
}

// TestParsePtahLaterDocument_FailurePath holds ptah.yaml to one YAML
// document. The loader reads the first, so a later document that sets
// anything would be dropped: its unknown keys would pass unchecked and its
// settings would not apply. The refusal names the line the later document's
// settings start on.
func TestParsePtahLaterDocument_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{
			name:     "a second document repeats a key",
			document: "url: postgres://first/db\n---\nurl: postgres://second/db\n",
			wantErr:  `failed to parse ptah config ptah\.yaml: line 3: the file holds a second YAML document, and ptah\.yaml is read as one; move these settings into the first document`,
		},
		{
			name:     "a second document carries an unknown key",
			document: "url: postgres://first/db\n---\nurll: postgres://typo/db\n",
			wantErr:  `failed to parse ptah config ptah\.yaml: line 3: the file holds a second YAML document, and ptah\.yaml is read as one; move these settings into the first document`,
		},
		{
			name:     "every setting is in the second document",
			document: "---\n---\nmigration:\n  dir: elsewhere\n",
			wantErr:  `failed to parse ptah config ptah\.yaml: line 3: the file holds a second YAML document, and ptah\.yaml is read as one; move these settings into the first document`,
		},
		{
			name:     "a third document after an empty second",
			document: "url: postgres://first/db\n---\n---\nurl: postgres://third/db\n",
			wantErr:  `failed to parse ptah config ptah\.yaml: line 4: the file holds a second YAML document, and ptah\.yaml is read as one; move these settings into the first document`,
		},
		{
			name:     "a second document is not YAML",
			document: "url: postgres://first/db\n---\nurl: [unclosed\n",
			wantErr:  `(?s)failed to parse ptah config ptah\.yaml: yaml: .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParsePtah([]byte(test.document), "ptah.yaml", "")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(cfg, qt.CmpEquals(cmp.Exporter(func(reflect.Type) bool { return true })), projectconfig.Config{})
		})
	}
}

// TestParsePtahLaterDocument_HappyPath is the control: a later document that
// sets nothing drops nothing, so the first document is read as written.
func TestParsePtahLaterDocument_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		document string
	}{
		{name: "a trailing separator", document: "url: postgres://first/db\n---\n"},
		{name: "a trailing separator and a comment", document: "url: postgres://first/db\n---\n# nothing here\n"},
		{name: "a null second document", document: "url: postgres://first/db\n---\nnull\n"},
		{name: "an empty second mapping", document: "url: postgres://first/db\n---\n{}\n"},
		{name: "a document end marker", document: "url: postgres://first/db\n...\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParsePtah([]byte(test.document), "ptah.yaml", "")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.DatabaseURL, qt.Equals, "postgres://first/db")
		})
	}
}

func TestParsePtahProjectConfigRejectsUnknownOnlineDDLKeys(t *testing.T) {
	c := qt.New(t)

	_, err := projectconfig.ParsePtah([]byte(`online_ddl:
  threshhold_rows: 100
`), "ptah.yaml", "")
	c.Assert(err, qt.ErrorMatches, `failed to parse ptah config ptah\.yaml: line 2: unknown ptah\.yaml key "threshhold_rows" under online_ddl; supported keys are tool, threshold_rows, args, fallback`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "projectconfig")

	_, err = projectconfig.ParsePtah([]byte(`env:
  prod:
    online_ddl:
      tooll: ghost
`), "ptah.yaml", "prod")
	c.Assert(err, qt.ErrorMatches, `failed to parse ptah config ptah\.yaml: line 4: unknown ptah\.yaml key "tooll" under online_ddl; supported keys are tool, threshold_rows, args, fallback`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "projectconfig")
}

func TestParsePtahProjectConfigRejectsUnknownMigrationPreflightKeys(t *testing.T) {
	c := qt.New(t)

	_, err := projectconfig.ParsePtah([]byte(`migration:
  pg_dumpto: ./backups
`), "ptah.yaml", "")

	c.Assert(err, qt.ErrorMatches, `failed to parse ptah config ptah\.yaml: line 2: unknown ptah\.yaml key "pg_dumpto" under migration; supported keys are dir, format, revisions_schema, revisions_table, revision_format, lock_timeout, statement_timeout, connect_timeout, migration_lock_timeout, exec_order, tx_mode, pre_up_hook, pre_down_hook, pg_dump_to, mysqldump_to, webhook, log`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "projectconfig")
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
	c.Assert(cfg.OnlineDDL, qt.DeepEquals, projectconfig.OnlineDDLConfig{
		Tool:          projectconfig.OnlineDDLToolGhost,
		ThresholdRows: 1000000,
		Fallback:      projectconfig.OnlineDDLFallbackError,
	})
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
	c.Assert(cfg.Schemas, qt.DeepEquals, make([]string, 0))
	c.Assert(cfg.Exclude, qt.DeepEquals, make([]string, 0))
	c.Assert(cfg.Lint.DisabledRules, qt.DeepEquals, make([]string, 0))
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

func TestParsePtahDiffPolicy(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`url: postgres://base/db
diff:
  skip: [drop_table, drop_column, drop_index, drop_enum]
  concurrent_index: true
`)

	cfg, err := projectconfig.ParsePtah(raw, "ptah.yaml", "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Diff.Skip.DropTable.Value, qt.IsTrue)
	c.Assert(cfg.Diff.Skip.DropColumn.Value, qt.IsTrue)
	c.Assert(cfg.Diff.Skip.DropIndex.Value, qt.IsTrue)
	c.Assert(cfg.Diff.Skip.DropEnum.Value, qt.IsTrue)
	c.Assert(cfg.Diff.ConcurrentIndex.Create.Value, qt.IsTrue)
	c.Assert(cfg.Diff.ConcurrentIndex.Create.Set, qt.IsTrue)
	c.Assert(cfg.Diff.SkipChangeKinds(), qt.DeepEquals, []planext.ChangeKind{
		planext.DropTable, planext.DropColumn, planext.DropIndex, planext.DropEnum,
	})
	c.Assert(cfg.Diff.ConcurrentIndexCreate(), qt.IsTrue)
}

func TestParsePtahDiffPolicyConcurrentIndexExplicitFalse(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`url: postgres://base/db
diff:
  concurrent_index: false
`)

	cfg, err := projectconfig.ParsePtah(raw, "ptah.yaml", "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Diff.ConcurrentIndex.Create.Set, qt.IsTrue)
	c.Assert(cfg.Diff.ConcurrentIndex.Create.Value, qt.IsFalse)
	c.Assert(cfg.Diff.SkipChangeKinds(), qt.HasLen, 0)
}

func TestParsePtahDiffPolicyUnknownSkipKind(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`url: postgres://base/db
diff:
  skip: [drop_universe]
`)

	_, err := projectconfig.ParsePtah(raw, "ptah.yaml", "")

	c.Assert(err, qt.ErrorMatches, `.*diff.skip: unknown diff skip change kind "drop_universe".*`)
}

func TestParsePtahDiffPolicyEnvOverride(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`url: postgres://base/db
diff:
  skip: [drop_table]
env:
  prod:
    diff:
      skip: [drop_column]
`)

	cfg, err := projectconfig.ParsePtah(raw, "ptah.yaml", "prod")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Diff.Skip.DropTable, qt.DeepEquals, projectconfig.ConfigBool{Set: true})
	c.Assert(cfg.Diff.Skip.DropColumn.Value, qt.IsTrue)
}
