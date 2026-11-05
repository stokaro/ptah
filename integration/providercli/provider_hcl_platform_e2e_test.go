//go:build integration

package providercli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/testutils"
)

func hclPlatformProvider(c *qt.C, environment ...string) *engine.Runtime {
	c.Helper()
	_, runtime := yamlProviderEnvironment(c, "hcl-platform", environment)
	return runtime
}

// tableOverrides are the platform overrides of each table of db that holds
// some, by the table's name.
func tableOverrides(db *schemamodel.Database) map[string]map[string]map[string]string {
	overrides := make(map[string]map[string]map[string]string)
	for _, table := range db.Tables {
		if len(table.Overrides) != 0 {
			overrides[table.Name] = table.Overrides
		}
	}
	return overrides
}

// hclPlatformDocument gives one table the provider's platform group, in two
// blocks, beside a group of another target, and leaves a second table without
// one.
const hclPlatformDocument = `table "records" {
  column "id" {
    type = integer
  }
  platform "fixture" {
    override "seconds" {
      value = "30"
    }
  }
  platform "mysql" {
    override "engine" {
      value = "InnoDB"
    }
  }
}
table "events" {
  column "id" {
    type = integer
  }
}
`

// TestProviderHCLPlatform_HappyPath reads a table's platform group of the
// provider's engine as the provider's value of that table, and keeps a group
// of another target among the table's overrides. The document writes no
// header entry the engine reads, so it claims nothing of the provider's
// model: a table without the group keeps what it holds.
func TestProviderHCLPlatform_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := hclPlatformProvider(c)

	db, err := atlashcl.ParseWithOptions(c.Context(), []byte(hclPlatformDocument), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})

	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, *db), qt.DeepEquals, []string{`table records {"seconds":30}`})
	c.Assert(tableOverrides(db), qt.DeepEquals, map[string]map[string]map[string]string{"records": {"mysql": {"engine": "InnoDB"}}})
	c.Assert(db.FeatureCoverage.IsZero(), qt.IsTrue)
}

// TestProviderHCLPlatformWithAHeaderEntry_HappyPath reads a document that
// writes a retention header entry, which the host sends to the provider for
// its account. The provider claims the model complete, and the host states it
// for the table that writes the group alone: the kind is uninspected, since a
// table without the group states nothing about it.
func TestProviderHCLPlatformWithAHeaderEntry_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := hclPlatformProvider(c)

	db, err := atlashcl.ParseWithOptions(c.Context(), []byte("// ptah:not-described retention \"audits\"\n"+hclPlatformDocument), "schema.hcl",
		atlashcl.Options{HCL: runtime.HCL()})

	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, *db), qt.DeepEquals, []string{`table records {"seconds":30}`})
	var subjects []string
	for _, record := range db.FeatureCoverage.SubjectRecords() {
		subjects = append(subjects, string(record.Subject.Kind)+" "+record.Subject.Name.Source+" "+string(record.Knowledge.State))
	}
	c.Assert(subjects, qt.DeepEquals, []string{"table records complete"})
	kinds := db.FeatureCoverage.KindRecords()
	c.Assert(kinds, qt.HasLen, 1)
	c.Assert(kinds[0].Knowledge.Unscoped(), qt.DeepEquals, schemaext.Knowledge{State: schemaext.Uninspected,
		Reason: `HCL describes this model only for a table that writes a platform "fixture" block`})
}

// TestProviderHCLPlatform_FailurePath refuses, at its block, an override the
// provider's claim does not list, naming the ones it does, and one that
// differs from a listed one in case; and a key two blocks of the group write.
func TestProviderHCLPlatform_FailurePath(t *testing.T) {
	runtime := hclPlatformProvider(qt.New(t))
	for _, row := range []struct {
		name   string
		source string
		want   string
	}{
		{"an override the claim does not list", "table \"records\" {\n  platform \"fixture\" {\n    override \"minutes\" {\n      value = \"1\"\n    }\n  }\n}\n",
			`parse HCL schema at schema.hcl:3,5-13: unknown table platform "fixture" override "minutes": the group takes seconds`},
		{"an override in another case", "table \"records\" {\n  platform \"fixture\" {\n    override \"Seconds\" {\n      value = \"1\"\n    }\n  }\n}\n",
			`parse HCL schema at schema.hcl:3,5-13: unknown table platform "fixture" override "Seconds": override names are lower case, as "seconds"`},
		{"a key two blocks write", "table \"records\" {\n  platform \"fixture\" {\n    override \"seconds\" {\n      value = \"1\"\n    }\n  }\n" +
			"  platform \"fixture\" {\n    override \"seconds\" {\n      value = \"2\"\n    }\n  }\n}\n",
			`parse HCL schema at schema.hcl:8,5-13: table platform override "seconds" for dialect "fixture" is duplicated`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.ParseWithOptions(c.Context(), []byte(row.source), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})

			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(db, qt.IsNil)
		})
	}
}

// A run that does not select the provider keeps its platform group among the
// table's overrides, as a group of any other target.
func TestProviderHCLPlatformWithoutTheProvider_KeepsIt(t *testing.T) {
	c := qt.New(t)
	plain, err := engine.New()
	c.Assert(err, qt.IsNil)

	db, err := atlashcl.ParseWithOptions(c.Context(), []byte(hclPlatformDocument), "schema.hcl", atlashcl.Options{HCL: plain.HCL()})

	c.Assert(err, qt.IsNil)
	c.Assert(tableOverrides(db), qt.DeepEquals, map[string]map[string]map[string]string{"records": {"fixture": {"seconds": "30"}, "mysql": {"engine": "InnoDB"}}})
	c.Assert(scopedRetentions(c, plain, *db), qt.HasLen, 0)
}

// hclPlatformExportSource declares the retention of a table through the base
// fixture's Go claim.
const hclPlatformExportSource = `package models

//ptah:schema:table name="records"
//ptah:fixture:retention seconds=5
type Record struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`

// TestProviderHCLPlatformExport_HappyPath writes a table's value through
// EncodeSource as the table's platform group of the provider's engine, in the
// writer's layout, and the document reads back to the same value. The
// coverage header leaves the model out, so the document without the group
// claims nothing of it, as one that never wrote the group.
func TestProviderHCLPlatformExport_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newProviderCLI(c, fixtureReleaseSpec(c, "hcl-platform", testutils.FixtureRelease{}))
	models := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(models, "records.go"), []byte(hclPlatformExportSource), 0o600), qt.IsNil)
	out := filepath.Join(c.TempDir(), "schema.hcl")

	_, stderr, code := executeProviderCLI(c.Context(), []string{"schema", "export", "--provider-config", f.manifestPath, "--provider-engine", "fixture",
		"--to", "hcl", "--root-dir", models, "--out", out})

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	written, err := os.ReadFile(out)
	c.Assert(err, qt.IsNil)
	group := "  platform \"fixture\" {\n    override \"seconds\" {\n      value = \"5\"\n    }\n  }\n"
	c.Assert(string(written), qt.Contains, group)
	c.Assert(string(written), qt.Not(qt.Contains), schemaext.CoverageHeaderMarker)
	runtime := hclPlatformProvider(c)
	db, err := atlashcl.ParseWithOptions(c.Context(), written, "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, *db), qt.DeepEquals, []string{`table records {"seconds":5}`})
	without, err := atlashcl.ParseWithOptions(c.Context(), []byte(strings.Replace(string(written), group, "", 1)), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, *without), qt.HasLen, 0)
	c.Assert(without.FeatureCoverage.IsZero(), qt.IsTrue)
}

// TestProviderHCLPlatformPartialLoss writes a value its provider writes in
// part: the group is written among the table's own platform blocks, in the
// order the writer writes its own, and the part the group does not hold is a
// warning at the table.
func TestProviderHCLPlatformPartialLoss(t *testing.T) {
	c := qt.New(t)
	runtime := hclPlatformProvider(c, "FIXTURE_PLATFORM_PARTIAL=1")
	db, err := atlashcl.ParseWithOptions(c.Context(), []byte(hclPlatformDocument), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)

	result, err := atlashclrender.Render(db, atlashclrender.Options{HCL: runtime.HCL(), Target: "fixture"})

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains, "  platform \"fixture\" {\n    override \"seconds\" {\n      value = \"30\"\n    }\n  }\n"+
		"  platform \"mysql\" {\n    override \"engine\" {\n      value = \"InnoDB\"\n    }\n  }\n")
	c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{{Severity: atlashclrender.SeverityWarning, Path: "table.records",
		Message: `the retention tiers of table "records" are not represented in HCL`}})
}
