//go:build integration

package providercli_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/hclext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/testutils"
)

func hclClaimsProvider(c *qt.C) *engine.Runtime {
	c.Helper()
	_, runtime := yamlProviderCommand(c, "hcl-claims")
	return runtime
}

// hclClaimsDocument declares a retention in a table block, one in a
// top-level block that names another table, and one as an attribute of an
// index with its type. Its header leaves the retention of a third table
// undescribed, which the provider reads.
const hclClaimsDocument = `// ptah:not-described retention "audits"
schema "public" {}
table "records" {
  schema = schema.public
  column "id" {
    type = integer
    null = false
  }
  column "body" {
    type = text
  }
  index "by_body" {
    columns           = [column.body]
    type              = BTREE
    retention_seconds = "9"
  }
  retention {
    seconds = "5"
  }
}
table "events" {
  schema = schema.public
  column "id" {
    type = integer
    null = false
  }
}
retention "public" "events" {
  seconds = "30"
}
`

// TestProviderHCLClaims_HappyPath reads a provider's blocks and index
// attributes, attaches each value to what its block declares a setting of,
// and states the provider's account of the document.
func TestProviderHCLClaims_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := hclClaimsProvider(c)

	db, err := atlashcl.ParseWithOptions(c.Context(), []byte(hclClaimsDocument), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})

	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, *db), qt.DeepEquals, []string{
		`table events {"seconds":30}`,
		`table records {"seconds":5}`,
		`index by_body {"seconds":9}`,
	})
	var subjects []string
	for _, record := range db.FeatureCoverage.SubjectRecords() {
		subjects = append(subjects, string(record.Subject.Kind)+" "+record.Subject.Name.Source+" "+string(record.Knowledge.State))
	}
	c.Assert(subjects, qt.ContentEquals, []string{"table records complete", "table events complete", "index by_body complete"})
}

// TestProviderHCLClaims_WithoutAHeader reads the document of
// TestProviderHCLClaims_HappyPath without its header. It declares the same
// values and claims nothing of the provider's model: an HCL document whose
// header names none of the provider's header kinds describes none of the
// retentions it leaves out, so a plan of it removes none.
func TestProviderHCLClaims_WithoutAHeader(t *testing.T) {
	c := qt.New(t)
	runtime := hclClaimsProvider(c)
	source := strings.Replace(hclClaimsDocument, "// ptah:not-described retention \"audits\"\n", "", 1)
	c.Assert(source, qt.Not(qt.Equals), hclClaimsDocument)

	db, err := atlashcl.ParseWithOptions(c.Context(), []byte(source), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})

	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, *db), qt.DeepEquals, []string{
		`table events {"seconds":30}`,
		`table records {"seconds":5}`,
		`index by_body {"seconds":9}`,
	})
	c.Assert(db.FeatureCoverage.KindRecords(), qt.HasLen, 0)
	c.Assert(db.FeatureCoverage.SubjectRecords(), qt.HasLen, 0)
}

// TestProviderHCLClaims_FailurePath refuses an index whose inherited type the
// provider refuses a retention on, a document whose header leaves undescribed
// a table it declares a retention of, which shows the header reaches the
// provider, and, in a run that did not select the provider, the provider's
// block as an unknown name.
func TestProviderHCLClaims_FailurePath(t *testing.T) {
	runtime := hclClaimsProvider(qt.New(t))
	plain, err := engine.New()
	qt.New(t).Assert(err, qt.IsNil)
	for _, row := range []struct {
		name    string
		runtime *engine.Runtime
		source  string
		want    string
	}{
		{"an index whose inherited type the provider refuses", runtime,
			"table \"records\" {\n  column \"id\" {\n    type = integer\n  }\n  index \"by_id\" {\n    columns = [column.id]\n    type = HASH\n    retention_seconds = \"9\"\n  }\n}\n",
			`(?s).*provider decode source failed \(Unknown\).*`},
		{"a table the header leaves undescribed", runtime, "// ptah:not-described retention \"events\"\nschema \"public\" {}\ntable \"events\" {\n  schema = schema.public\n  column \"id\" {\n    type = integer\n  }\n}\n" +
			"retention \"public\" \"events\" {\n  seconds = \"30\"\n}\n", `(?s).*provider decode source failed \(Unknown\).*`},
		{"the provider's block without the provider", plain, "retention \"public\" \"events\" {\n  seconds = \"30\"\n}\n",
			`parse HCL schema at schema.hcl:1,1-10: unsupported top-level block "retention"`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.ParseWithOptions(c.Context(), []byte(row.source), "schema.hcl", atlashcl.Options{HCL: row.runtime.HCL()})

			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(db, qt.IsNil)
		})
	}
}

// hclExportSource declares the retention of a table, of an index and of a
// column through the provider's Go claims.
const hclExportSource = `package models

//ptah:schema:table name="records"
//ptah:fixture:retention seconds=5
type Record struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
	//ptah:schema:field name="body" type="TEXT" seconds=7
	Body string

	//ptah:schema:index name="by_body" fields="body" seconds=9
	_ int
}
`

// TestProviderHCLExport_HappyPath writes a provider's values through
// EncodeSource: the table's retention as the provider's block in the table,
// and the index's as the provider's attribute of the index block. The
// column's has no place in an HCL encoding and is reported as a loss.
// Reading the document back gives the table and index values again.
func TestProviderHCLExport_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newProviderCLI(c, fixtureReleaseSpec(c, "hcl-claims", testutils.FixtureRelease{}))
	models := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(models, "records.go"), []byte(hclExportSource), 0o600), qt.IsNil)
	out := filepath.Join(c.TempDir(), "schema.hcl")

	_, stderr, code := executeProviderCLI(c.Context(), []string{"schema", "export", "--provider-config", f.manifestPath, "--provider-engine", "fixture",
		"--to", "hcl", "--root-dir", models, "--out", out})

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	written, err := os.ReadFile(out)
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Contains, "  retention {\n    seconds = \"5\"\n  }\n")
	c.Assert(string(written), qt.Contains, "retention_seconds = \"9\"")
	c.Assert(stderr, qt.Contains, `warning: column.Record.body: example.org/fixture/retention of column "body" of table "records" has no HCL spelling a provider writes`)
	runtime := hclClaimsProvider(c)
	db, err := atlashcl.ParseWithOptions(c.Context(), written, "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, *db), qt.DeepEquals, []string{`table records {"seconds":5}`, `index by_body {"seconds":9}`})
}

// coverageHeader matches the captured feature coverage header line of an HCL
// document.
var coverageHeader = regexp.MustCompile(`(?m)^// (ptah:feature-coverage \{.*\})\n`)

// TestProviderHCLExport_WritesTheClaim writes the Go source of
// TestProviderHCLExport_HappyPath as HCL. The document states the source's
// claim of the provider's model as a captured feature coverage header, and
// writes no ptah:not-described entry; read back, it makes the same claim.
// Without the header the document claims nothing.
func TestProviderHCLExport_WritesTheClaim(t *testing.T) {
	c := qt.New(t)
	runtime := hclClaimsProvider(c)
	source, err := goschema.ParseSource(runtime.Annotations(), "records.go", hclExportSource)
	c.Assert(err, qt.IsNil)
	c.Assert(source.FeatureCoverage.KindRecords(), qt.HasLen, 1)

	result, err := atlashclrender.Render(&source, atlashclrender.Options{HCL: runtime.HCL(), Target: "fixture"})

	c.Assert(err, qt.IsNil)
	written := string(result.Data)
	c.Assert(coverageHeader.FindAllString(written, -1), qt.HasLen, 1, qt.Commentf("%s", written))
	c.Assert(written, qt.Not(qt.Contains), "ptah:not-described")
	read, err := atlashcl.ParseWithOptions(c.Context(), result.Data, "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(read.FeatureCoverage.KindRecords(), qt.DeepEquals, source.FeatureCoverage.KindRecords())
	c.Assert(read.FeatureCoverage.SubjectRecords(), qt.DeepEquals, source.FeatureCoverage.SubjectRecords())
	unclaimed, err := atlashcl.ParseWithOptions(c.Context(), []byte(coverageHeader.ReplaceAllString(written, "")), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(unclaimed.FeatureCoverage.KindRecords(), qt.HasLen, 0)
}

// TestProviderHCLExport_SplitKeepsTheClaim cuts the HCL export of
// TestProviderHCLExport_WritesTheClaim into files: each file carries the
// document's captured header again, so a file read on its own makes the claim
// the document made.
func TestProviderHCLExport_SplitKeepsTheClaim(t *testing.T) {
	c := qt.New(t)
	runtime := hclClaimsProvider(c)
	source, err := goschema.ParseSource(runtime.Annotations(), "records.go", hclExportSource)
	c.Assert(err, qt.IsNil)
	result, err := atlashclrender.Render(&source, atlashclrender.Options{HCL: runtime.HCL(), Target: "fixture"})
	c.Assert(err, qt.IsNil)
	written := coverageHeader.FindStringSubmatch(string(result.Data))
	c.Assert(written, qt.HasLen, 2, qt.Commentf("%s", result.Data))

	restated, err := runtime.HCL().SplitRestate(schemaext.CoverageHeaderBodies(string(result.Data)), nil)

	c.Assert(err, qt.IsNil)
	c.Assert(restated, qt.DeepEquals, []string{written[1]})
}

// TestProviderHCLExport_FailurePath refuses an HCL export whose provider
// answers with ptah:not-described entries: the host writes the document's
// account of the provider's models itself, and the document would hold two.
func TestProviderHCLExport_FailurePath(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "hcl-claims", []string{"FIXTURE_SOURCE_LIMITS=1", "FIXTURE_SOURCE_LIMITS_EXTRA=1"})
	source, err := goschema.ParseSource(runtime.Annotations(), "records.go", hclExportSource)
	c.Assert(err, qt.IsNil)

	result, err := atlashclrender.Render(&source, atlashclrender.Options{HCL: runtime.HCL(), Target: "fixture"})

	c.Assert(err, qt.ErrorMatches, `.*provider example\.org/fixture wrote ptah:not-described entries in an HCL document, whose account of the provider's models the host writes as its ptah:feature-coverage header`)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result.Data, qt.IsNil)
}

// TestProviderGoExport_HappyPath writes a provider's values as Go
// annotations through EncodeSource: the table's retention as the provider's
// own directive at the table, the column's and the index's as attributes of
// the frontend's field and index directives, and the INTEGER column as the
// int64 field the provider gives it. Reading the source back gives the same
// values.
func TestProviderGoExport_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := hclClaimsProvider(c)
	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", hclExportSource)
	c.Assert(err, qt.IsNil)
	want := scopedRetentions(c, runtime, db)

	files, err := goschematogo.Render(c.Context(), &db, goschematogo.Options{Annotations: runtime.Annotations(), Runtime: runtime, PackageName: "models",
		SingleFile: true, Dialect: "fixture"})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	source := string(files[0].Data)
	c.Assert(source, qt.Contains, "//ptah:fixture:retention seconds=\"5\"")
	c.Assert(source, qt.Contains, "ID *int64")
	again, err := goschema.ParseSource(runtime.Annotations(), files[0].Name, source)
	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, again), qt.DeepEquals, want)
	c.Assert(want, qt.DeepEquals, []string{`table records {"seconds":5}`, `column body {"seconds":7}`, `index by_body {"seconds":9}`})
}

// TestProviderSourceExport_RefusesAnotherTarget drives the runtime's public
// source encoders with an explicit target the selected provider does not own.
// Selecting a sole provider must not let its field types or blocks cross into
// another engine's source.
func TestProviderSourceExport_RefusesAnotherTarget(t *testing.T) {
	c := qt.New(t)
	runtime := hclClaimsProvider(c)

	goResult, goErr := runtime.Annotations().Encode(c.Context(), annotation.EncodeRequest{Target: "postgres"})
	hclResult, hclErr := runtime.HCL().Encode(c.Context(), hclext.EncodeRequest{Target: "postgres"})

	c.Assert(goErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(goResult, qt.DeepEquals, annotation.EncodeResult{})
	c.Assert(hclErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(hclResult, qt.DeepEquals, hclext.EncodeResult{})
}
