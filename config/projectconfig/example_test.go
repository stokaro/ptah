package projectconfig_test

import (
	"fmt"
	"testing/fstest"

	"github.com/go-extras/go-kit/must"

	"go.5x5.cz/ptah/config/projectconfig"
)

// ExampleParsePtah parses a ptah.yaml document from bytes, selecting one env
// block. The env's values merge over the top-level settings, so the selected
// Config carries the env's URL and inherits everything the env does not
// set -- the shortest path from a config document to the typed IR.
func ExampleParsePtah() {
	document := []byte(`url: sqlite://file:app.db
migration:
  dir: ./migrations
env:
  prod:
    url: sqlite://file:prod.db
`)

	cfg := must.Must(projectconfig.ParsePtah(document, "ptah.yaml", "prod"))

	fmt.Println("env:", cfg.EnvName)
	fmt.Println("url:", cfg.DatabaseURL)
	fmt.Println("migrations:", cfg.Migration.Dir)

	// Output:
	// env: prod
	// url: sqlite://file:prod.db
	// migrations: ./migrations
}

// ExampleParseAtlasFSWithOptions evaluates an atlas.hcl against an fs.FS:
// file() and fileset() resolve only through that filesystem; data-source
// blocks (runtimevar, external, sql) still reach whatever their configuration
// names. This is the entry point for an embedder
// whose project files are already anchored, generated, or held in memory.
func ExampleParseAtlasFSWithOptions() {
	projectFS := fstest.MapFS{
		"database-url.txt": {Data: []byte("sqlite://file:app.db")},
		"schema/app.hcl":   {Data: []byte(`schema "main" {}`)},
	}
	raw := []byte(`data "hcl_schema" "app" {
  paths = fileset("schema/*.hcl")
}

env "local" {
  url = file("database-url.txt")
  src = data.hcl_schema.app.url
}
`)

	cfg := must.Must(projectconfig.ParseAtlasFSWithOptions(
		raw,
		"atlas.hcl",
		projectFS,
		projectconfig.AtlasLoadOptions{EnvName: "local"},
	))

	fmt.Println("url:", cfg.DatabaseURL)
	fmt.Println("sources:", cfg.SchemaSources)

	// Output:
	// url: sqlite://file:app.db
	// sources: [file://schema/app.hcl]
}

// ExampleParseAtlasCollectionWithOptions expands an env whose for_each selects
// several instances -- one Config per value, in source order for a tuple. The
// singular functions (ParseAtlas, ParseAtlasWithOptions, Load) refuse such an
// env rather than pick an instance silently, so a caller that can iterate
// reaches for a collection function.
func ExampleParseAtlasCollectionWithOptions() {
	raw := []byte(`env {
  for_each = ["sqlite://file:tenant-a.db", "sqlite://file:tenant-b.db"]
  name     = atlas.env
  url      = each.value
}
`)

	configs := must.Must(projectconfig.ParseAtlasCollectionWithOptions(
		raw,
		"atlas.hcl",
		projectconfig.AtlasLoadOptions{EnvName: "tenants"},
	))

	for _, cfg := range configs {
		fmt.Println(cfg.EnvName, cfg.DatabaseURL)
	}

	// Output:
	// tenants sqlite://file:tenant-a.db
	// tenants sqlite://file:tenant-b.db
}

// ExampleMerge combines two parsed configs with the package precedence: a
// value the override's loader marked present wins, everything else survives
// from the base. This is what Load does with ptah.yaml as the base and
// atlas.hcl as the override. A non-zero programmatic value needs no loader
// metadata to win, so a Config built by hand overrides too.
func ExampleMerge() {
	base := must.Must(projectconfig.ParsePtah([]byte(`url: sqlite://file:base.db
migration:
  dir: ./migrations
`), "ptah.yaml", ""))
	override := must.Must(projectconfig.ParseAtlas([]byte(`env "local" {
  url = "sqlite://file:local.db"
}
`), "atlas.hcl", "local"))

	merged := projectconfig.Merge(base, override)
	fmt.Println("url:", merged.DatabaseURL)
	fmt.Println("migrations:", merged.Migration.Dir)

	patched := projectconfig.Merge(merged, projectconfig.Config{DevURL: "sqlite://file:shadow.db"})
	fmt.Println("dev:", patched.DevURL)

	// Output:
	// url: sqlite://file:local.db
	// migrations: ./migrations
	// dev: sqlite://file:shadow.db
}

// ExampleConfig_StringValue resolves a field together with its presence, which
// is what separates "the file set this" from "the file never mentioned it".
// Command defaults must apply only when Present is false, so an embedder
// resolving flag-versus-config precedence reads through StringValue instead of
// comparing the field to the empty string.
func ExampleConfig_StringValue() {
	cfg := must.Must(projectconfig.ParsePtah([]byte("url: sqlite://file:app.db\n"), "ptah.yaml", ""))

	url := cfg.StringValue(projectconfig.StringDatabaseURL)
	fmt.Printf("database url %q present=%t\n", url.Value, url.Present)

	dev := cfg.StringValue(projectconfig.StringDevURL)
	fmt.Printf("dev url %q present=%t\n", dev.Value, dev.Present)
	if !dev.Present {
		fmt.Println("dev url: apply the command default")
	}

	// Output:
	// database url "sqlite://file:app.db" present=true
	// dev url "" present=false
	// dev url: apply the command default
}
