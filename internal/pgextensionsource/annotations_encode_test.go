package pgextensionsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/pgextensionsource"
)

func TestExtensionExportRoundTripWithoutTables(t *testing.T) {
	c := qt.New(t)
	owners := must.Must(annotation.NewSet(pgextensionsource.Annotations()))
	first := must.Must(pgextension.DesiredObject(pgextension.Ref("pg_trgm"), pgextension.DesiredExtension{
		Schema: "ext", Version: "1.6", IfNotExists: true, Comment: "a \"quoted\" comment\non another line",
	}))
	first.Targets = []string{"cockroachdb", "postgres"}
	second := must.Must(pgextension.DesiredObject(pgextension.Ref("citext"), pgextension.DesiredExtension{}))
	declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(first, second)), FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired))}
	files, err := goschematogo.Render(t.Context(), declared, goschematogo.Options{Annotations: owners, SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(string(files[0].Data), qt.Matches, `(?s).*name="citext".*name="pg_trgm".*`)
	parsed, err := goschema.ParseSource(owners, files[0].Name, files[0].Data)
	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(parsed.FeatureObjects.All()), qt.DeepEquals, must.Must(declared.FeatureObjects.All()))
	c.Assert(parsed.FeatureCoverage.KindRecords(), qt.DeepEquals, declared.FeatureCoverage.KindRecords())
	again, err := goschematogo.Render(t.Context(), &parsed, goschematogo.Options{Annotations: owners, SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(again, qt.DeepEquals, files)
	c.Assert(first.Targets, qt.DeepEquals, []string{"cockroachdb", "postgres"})
}

func TestExtensionExportRequiresItsSelectedOwner(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgextension.DesiredObject(pgextension.Ref("citext"), pgextension.DesiredExtension{}))))}
	files, err := goschematogo.Render(t.Context(), declared, goschematogo.Options{Annotations: annotation.None(), SingleFile: true})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*Go annotations cannot represent feature object .*citext.*`)
	c.Assert(files, qt.IsNil)
}

func TestExtensionEncoderRefusesUnreadableCoverage(t *testing.T) {
	owners := must.Must(annotation.NewSet(pgextensionsource.Annotations()))
	for _, test := range []struct {
		name  string
		known schemaext.Coverage
		want  string
	}{
		{"namespace", must.Must(pgextension.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "extension catalog is unavailable"}, nil)),
			`.*cannot preserve extension namespace coverage: extension catalog is unavailable`},
		{"subject", must.Must(pgextension.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
			{Kind: pgextension.ExtensionKind, Subject: pgextension.Ref("citext"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "extension properties are unavailable"}},
		})), `.*cannot preserve extension .*citext.* coverage: extension properties are unavailable`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := owners.Encode(t.Context(), annotation.EncodeRequest{Coverage: test.known})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(result, qt.DeepEquals, annotation.EncodeResult{})
		})
	}
}

func TestExtensionEncoderRefusesObservedValues(t *testing.T) {
	c := qt.New(t)
	owners := must.Must(annotation.NewSet(pgextensionsource.Annotations()))
	observed := must.Must(pgextension.ObservedObject(pgextension.Ref("citext"), pgextension.ObservedExtension{}))
	result, err := owners.Encode(t.Context(), annotation.EncodeRequest{Objects: []schemaext.Object{observed}})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*Go annotations require a desired extension .*citext.*`)
	c.Assert(result, qt.DeepEquals, annotation.EncodeResult{})
}
