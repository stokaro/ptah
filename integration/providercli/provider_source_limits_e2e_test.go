//go:build integration

package providercli_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/convert/goschematogo"
)

// TestProviderSourceLimits_GoRoundTrip exercises the subprocess, provider coverage
// conversion, source writer and source reader. Dropping either the header or
// its decoded knowledge would turn unknown retention into absence authority.
func TestProviderSourceLimits_GoRoundTrip(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "hcl-claims", []string{"FIXTURE_SOURCE_LIMITS=1"})
	before, after := goLimitRoundTrip(c, runtime)
	checkSourceLimitRoundTrip(c, before, after)
}

func TestProviderSourceLimits_HCLRoundTrip(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "hcl-claims", []string{"FIXTURE_SOURCE_LIMITS=1"})
	before, after := hclLimitRoundTrip(c, runtime)
	checkSourceLimitRoundTrip(c, before, after)
}

func TestProviderSourceGroups_GoRoundTrip(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "hcl-claims", []string{"FIXTURE_SOURCE_LIMITS=1", "FIXTURE_SOURCE_GROUPS=1"})
	before, after := goLimitRoundTrip(c, runtime)
	checkSourceLimitRoundTrip(c, before, after)
}

func TestProviderSourceGroups_HCLRoundTrip(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "hcl-claims", []string{"FIXTURE_SOURCE_LIMITS=1", "FIXTURE_SOURCE_GROUPS=1"})
	before, after := hclLimitRoundTrip(c, runtime)
	checkSourceLimitRoundTrip(c, before, after)
}

func checkSourceLimitRoundTrip(c *qt.C, before, after schemaext.Coverage) {
	c.Helper()
	c.Assert(before.KindRecords(), qt.HasLen, 1)
	c.Assert(before.KindRecords()[0].Knowledge.Unscoped(), qt.DeepEquals, schemaext.Knowledge{State: schemaext.Uninspected,
		Reason: `{"reason":"not-inspected","provenance":"observed"}`})
	c.Assert(after.KindRecords(), qt.DeepEquals, before.KindRecords())
	c.Assert(after.SubjectRecords(), qt.DeepEquals, before.SubjectRecords())
}

func goLimitRoundTrip(c *qt.C, runtime *engine.Runtime) (before, after schemaext.Coverage) {
	c.Helper()
	source := strings.Replace(hclExportSource, "package models", "package models\n\n//ptah:schema:notdescribed kind=\"retention\" reason=\"not-inspected\" provenance=\"observed\"\ntype _ struct{}", 1)
	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", source)
	c.Assert(err, qt.IsNil)
	before = db.FeatureCoverage
	files, err := goschematogo.Render(c.Context(), &db, goschematogo.Options{Annotations: runtime.Annotations(), Runtime: runtime,
		PackageName: "models", SingleFile: true, Dialect: "fixture"})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	written := string(files[0].Data)
	c.Assert(written, qt.Contains, `//ptah:schema:notdescribed kind="retention" reason="not-inspected" provenance="observed"`)
	again, err := goschema.ParseSource(runtime.Annotations(), files[0].Name, written)
	c.Assert(err, qt.IsNil)
	after = again.FeatureCoverage
	control, err := goschema.ParseSource(runtime.Annotations(), "records.go", hclExportSource)
	c.Assert(err, qt.IsNil)
	c.Assert(control.FeatureCoverage.KindRecords()[0].Knowledge.Unscoped().State, qt.Equals, schemaext.Complete)
	return before, after
}

func hclLimitRoundTrip(c *qt.C, runtime *engine.Runtime) (before, after schemaext.Coverage) {
	c.Helper()
	source := strings.Replace(hclClaimsDocument, `// ptah:not-described retention "audits"`, "// ptah:not-described retention reason=not-inspected provenance=observed", 1)
	db, err := atlashcl.ParseWithOptions(c.Context(), []byte(source), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	before = db.FeatureCoverage
	result, err := atlashclrender.Render(db, atlashclrender.Options{HCL: runtime.HCL(), Target: "fixture"})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	written := string(result.Data)
	c.Assert(written, qt.Contains, "// ptah:not-described retention reason=not-inspected provenance=observed")
	again, err := atlashcl.ParseWithOptions(c.Context(), result.Data, "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	after = again.FeatureCoverage
	control, err := atlashcl.ParseWithOptions(c.Context(), []byte(strings.Replace(source, "// ptah:not-described retention reason=not-inspected provenance=observed\n", "", 1)), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(control.FeatureCoverage.KindRecords()[0].Knowledge.Unscoped().State, qt.Equals, schemaext.Complete)
	return before, after
}

func TestProviderSourceLimits_UnknownHeadersRefused(t *testing.T) {
	_, runtime := yamlProviderEnvironment(qt.New(t), "hcl-claims", []string{"FIXTURE_SOURCE_LIMITS=1"})
	for _, header := range []string{
		`kind="unclaimed"`,
		`kind="retention" reason="made-up"`,
		`kind="retention" provenance="made-up"`,
	} {
		t.Run(header, func(t *testing.T) {
			c := qt.New(t)
			_, err := goschema.ParseSource(runtime.Annotations(), "limits.go", "package models\n//ptah:schema:notdescribed "+header+"\ntype _ struct{}\n")
			c.Assert(err, qt.IsNotNil)
		})
	}
	for _, header := range []string{"unclaimed", "retention reason=made-up", "retention provenance=made-up"} {
		t.Run(header, func(t *testing.T) {
			c := qt.New(t)
			_, err := atlashcl.ParseWithOptions(c.Context(), []byte("// ptah:not-described "+header+"\n"), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
			c.Assert(err, qt.IsNotNil)
		})
	}
}

func TestProviderSourceLimits_LostHeadersRefuseExport(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "hcl-claims", []string{"FIXTURE_SOURCE_LIMITS=1", "FIXTURE_SOURCE_LIMITS_DROP=1"})
	source := strings.Replace(hclExportSource, "package models", "package models\n\n//ptah:schema:notdescribed kind=\"retention\" provenance=\"observed\"\ntype _ struct{}", 1)
	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", source)
	c.Assert(err, qt.IsNil)
	files, err := goschematogo.Render(c.Context(), &db, goschematogo.Options{Annotations: runtime.Annotations(), Runtime: runtime,
		PackageName: "models", SingleFile: true, Dialect: "fixture"})
	c.Assert(err, qt.ErrorMatches, `provider encode source failed \(Internal\)`)
	c.Assert(files, qt.IsNil)
	result, err := atlashclrender.Render(&db, atlashclrender.Options{HCL: runtime.HCL(), Target: "fixture"})
	c.Assert(err, qt.ErrorMatches, `provider encode source failed \(Internal\)`)
	c.Assert(result.Data, qt.IsNil)
}
