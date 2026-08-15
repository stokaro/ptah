package projectconfig_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

func TestParseAtlas_ResolvesComputedDataSourceIndex(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	atlasPath := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(atlasPath, []byte(`locals {
  source = "selected"
}
data "hcl_schema" "selected" {
  path = "schema.hcl"
}
env "local" {
  src = data.hcl_schema[local.source].url
}
`), 0o600), qt.IsNil)

	cfg, err := projectconfig.LoadAtlasFile(atlasPath, "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://schema.hcl"})
}

func TestParseAtlas_LazilyAcceptsRecognizedUnreferencedDataSources(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`
data "hcl_schema" "unused_hcl" {
  path = "missing.hcl"
}
data "external_schema" "unused_external_schema" {
  program = ["/missing/schema-loader"]
}
data "sql" "unused_sql" {
  url   = "sqlite:///missing/unused.db"
  query = "SELECT missing"
}
data "external" "unused_external" {
  program = ["/missing/program"]
}
data "runtimevar" "unused_runtimevar" {
  url = "file:///missing/secret"
}
data "template_dir" "unused_template" {
  path = "/missing/templates"
  vars = {}
}
data "remote_dir" "unused_remote" {
  name = "missing-cloud-directory"
}
data "remote_schema" "unused_remote_schema" {
  name = "missing-cloud-schema"
}
data "aws_rds_token" "unused_aws" {
  endpoint = "missing.invalid:5432"
  username = "nobody"
}
data "gcp_cloudsql_token" "unused_gcp" {}

env "local" {
  url = "sqlite://selected.db"
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://selected.db")
}

func TestParseAtlas_ValidatesUnreferencedHCLSchemaVarsWithoutResolvingPath(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`
locals {
  tenant = "acme"
}
data "hcl_schema" "unused" {
  path = "missing.hcl"
  vars = {
    tenant = local.tenant
  }
}
env "local" {
  url = "sqlite://selected.db"
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://selected.db")
}

func TestParseAtlas_RefusesMalformedUnreferencedHCLSchemaVars(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`
data "hcl_schema" "unused" {
  path = "missing.hcl"
  vars = ["not", "a", "map"]
}
env "local" {
  url = "sqlite://selected.db"
}
`)

	_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.ErrorMatches, `atlas.hcl "vars" at atlas.hcl:4 must be a map of values`)
}

func TestParseAtlas_DoesNotResolveDataUsedOnlyByUnselectedEnvironment(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`
data "external" "unused" {
  program = ["/missing/program"]
}
env "selected" {
  url = "sqlite://selected.db"
}
env "other" {
  url = data.external.unused
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "selected")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://selected.db")
}

func TestParseAtlas_ReferencedUnimplementedRemoteDataSourceStillRefuses(t *testing.T) {
	tests := []string{"remote_dir", "remote_schema"}
	for _, sourceType := range tests {
		t.Run(sourceType, func(t *testing.T) {
			c := qt.New(t)
			raw := fmt.Appendf(nil, `
data %q "selected" {
  name = "cloud-object"
}
env "local" {
  url = data.%s.selected.url
}
`, sourceType, sourceType)

			_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

			c.Assert(err, qt.ErrorMatches, `unsupported atlas\.hcl construct "data\.`+sourceType+`" at atlas\.hcl:2`)
		})
	}
}

func TestParseAtlas_ReportsDataSourceDependencyCycleBeforeExecution(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`
data "external" "first" {
  program = [data.external.second]
}
data "external" "second" {
  program = [data.external.first]
}
env "local" {
  url = data.external.first
}
`)

	_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.ErrorMatches, `atlas\.hcl evaluation cycle involving data\.external\.first`)
}
