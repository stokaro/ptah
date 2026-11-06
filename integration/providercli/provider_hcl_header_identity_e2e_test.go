//go:build integration

package providercli_test

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
)

// fixtureRetentionClaim is the identity the hcl-claims fixture describes for
// its desired retention model: the provider, the model's kind and
// representation, its schema version and the SHA-256 of the fixture's
// retentionDefinition.
var fixtureRetentionClaim = schemaext.CodecIdentity{Owner: "example.org/fixture", Kind: "example.org/fixture/retention", Representation: schemaext.Desired,
	Version: 1, Definition: "sha256:fcbd8cd8072ad2c9d16f312643c82871d269ab8af5a9049c08f54a1325de0469"}

// hclClaimsRelease starts the hcl-claims fixture built as release selects,
// with environment, and returns its runtime.
func hclClaimsRelease(c *qt.C, release testutils.FixtureRelease, environment ...string) *engine.Runtime {
	c.Helper()
	spec := fixtureReleaseSpec(c, "hcl-claims", release)
	spec.Environment = environment
	host, err := providerhost.Start(c.Context(), spec)
	c.Assert(err, qt.IsNil)
	c.Cleanup(host.Close)
	runtime, err := engine.New(host.Provider())
	c.Assert(err, qt.IsNil)
	return runtime
}

// renderedClaim is the HCL export of hclExportSource through runtime, whose
// header claims the source's retention values.
func renderedClaim(c *qt.C, runtime *engine.Runtime) []byte {
	c.Helper()
	source, err := goschema.ParseSource(runtime.Annotations(), "records.go", hclExportSource)
	c.Assert(err, qt.IsNil)
	result, err := atlashclrender.Render(&source, atlashclrender.Options{HCL: runtime.HCL(), Target: "fixture"})
	c.Assert(err, qt.IsNil)
	return result.Data
}

// headerModels are the model identities the captured header of document
// names, in its order.
func headerModels(c *qt.C, document []byte) []schemaext.CodecIdentity {
	c.Helper()
	header := coverageHeader.FindStringSubmatch(string(document))
	c.Assert(header, qt.HasLen, 2, qt.Commentf("%s", document))
	var wire struct {
		Kinds []struct {
			Model schemaext.CodecIdentity `json:"model"`
		} `json:"kinds"`
	}
	c.Assert(json.Unmarshal([]byte(strings.TrimPrefix(header[1], schemaext.CoverageHeaderMarker+" ")), &wire), qt.IsNil)
	models := make([]schemaext.CodecIdentity, 0, len(wire.Kinds))
	for _, kind := range wire.Kinds {
		models = append(models, kind.Model)
	}
	return models
}

// earlierDefinitionDocument is document with its header naming the retention
// model by the definition digest the fixture names as an earlier identity of
// it with FIXTURE_RETENTION_OWN_PREDECESSOR=1, as an export written before
// the definition text changed names it.
func earlierDefinitionDocument(c *qt.C, document []byte) []byte {
	c.Helper()
	earlier := strings.Replace(string(document), `"definition":"`+fixtureRetentionClaim.Definition+`"`, `"definition":"sha256:`+strings.Repeat("cd", 32)+`"`, 1)
	c.Assert(earlier, qt.Not(qt.Equals), string(document))
	return []byte(earlier)
}

// An HCL export names the provider's model in its header by the identity the
// provider describes, which is not the identity of the host's own codec: that
// one binds the provider's release and what the host negotiated with it.
func TestProviderHCLExport_TheHeaderNamesTheDescribedModel(t *testing.T) {
	c := qt.New(t)
	runtime := hclClaimsProvider(c)
	held, found := runtime.Codecs().Identity(fixtureRetentionClaim.Kind, schemaext.Desired)

	written := renderedClaim(c, runtime)

	c.Assert(headerModels(c, written), qt.DeepEquals, []schemaext.CodecIdentity{fixtureRetentionClaim})
	c.Assert(found, qt.IsTrue)
	c.Assert(held.Definition, qt.Not(qt.Equals), fixtureRetentionClaim.Definition)
	c.Assert(regexp.MustCompile(`"definition":"sha256:[0-9a-f]{64}"`).FindAllString(string(written), -1), qt.HasLen, 1)
}

// An HCL export one release of the provider wrote reads with the next
// release, which describes the same model: the two releases write the same
// document, and the later reads the claim the earlier made, though the host's
// own codecs of the two releases have different identities.
func TestProviderHCLRead_AnExportOfAnotherRelease_HappyPath(t *testing.T) {
	c := qt.New(t)
	earlier := hclClaimsRelease(c, testutils.FixtureRelease{Version: "1.0.0"})
	later := hclClaimsRelease(c, testutils.FixtureRelease{Version: "1.0.1"})
	source, err := goschema.ParseSource(later.Annotations(), "records.go", hclExportSource)
	c.Assert(err, qt.IsNil)
	written := renderedClaim(c, earlier)
	earlierCodec, _ := earlier.Codecs().Identity(fixtureRetentionClaim.Kind, schemaext.Desired)
	laterCodec, _ := later.Codecs().Identity(fixtureRetentionClaim.Kind, schemaext.Desired)

	read, err := atlashcl.ParseWithOptions(c.Context(), written, "schema.hcl", atlashcl.Options{HCL: later.HCL()})

	c.Assert(err, qt.IsNil)
	c.Assert(read.FeatureCoverage.KindRecords(), qt.DeepEquals, source.FeatureCoverage.KindRecords())
	c.Assert(read.FeatureCoverage.SubjectRecords(), qt.DeepEquals, source.FeatureCoverage.SubjectRecords())
	c.Assert(string(renderedClaim(c, later)), qt.Equals, string(written))
	c.Assert(earlierCodec, qt.Not(qt.Equals), laterCodec)
}

// A header written before the provider's definition text changed names the
// model by its earlier digest. A provider that names that identity as an
// earlier identity of the model itself reads the same claim the export made,
// and a file cut from the document carries the claim under the model's
// current identity.
func TestProviderHCLRead_AnEarlierDefinition_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := hclClaimsRelease(c, testutils.FixtureRelease{}, "FIXTURE_RETENTION_OWN_PREDECESSOR=1")
	source, err := goschema.ParseSource(runtime.Annotations(), "records.go", hclExportSource)
	c.Assert(err, qt.IsNil)
	written := renderedClaim(c, runtime)
	earlier := earlierDefinitionDocument(c, written)

	read, readErr := atlashcl.ParseWithOptions(c.Context(), earlier, "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	restated, restateErr := runtime.HCL().SplitRestate(schemaext.CoverageHeaderBodies(string(earlier)), nil)

	c.Assert(readErr, qt.IsNil)
	c.Assert(read.FeatureCoverage.KindRecords(), qt.DeepEquals, source.FeatureCoverage.KindRecords())
	c.Assert(read.FeatureCoverage.SubjectRecords(), qt.DeepEquals, source.FeatureCoverage.SubjectRecords())
	c.Assert(restateErr, qt.IsNil)
	c.Assert(restated, qt.DeepEquals, []string{coverageHeader.FindStringSubmatch(string(written))[1]})
}

// The same document read through a provider that names no earlier identity
// of the model is refused: its header names a definition the provider no
// longer has.
func TestProviderHCLRead_AnEarlierDefinition_FailurePath(t *testing.T) {
	c := qt.New(t)
	runtime := hclClaimsProvider(c)
	earlier := earlierDefinitionDocument(c, renderedClaim(c, runtime))

	read, err := atlashcl.ParseWithOptions(c.Context(), earlier, "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})

	c.Assert(err, qt.ErrorIs, schemaext.ErrIncompatibleCodec)
	c.Assert(err, qt.ErrorMatches, `.*incompatible feature codec: coverage model "example\.org/fixture/retention"/desired`)
	c.Assert(read, qt.IsNil)
}
