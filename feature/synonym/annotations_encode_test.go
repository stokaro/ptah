package synonym_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/synonym"
	"ptah.run/internal/convert/goschematogo"
)

// The source has no table to carry standalone declarations. Every address and
// literal must survive export, including an empty required value.
func TestGoExportRoundTripWithoutTables(t *testing.T) {
	owners := must.Must(annotation.NewSet(synonym.Annotations()))
	source := []byte(`package models

//ptah:schema:synonym name="remote_orders" schema="app" target="[srv]..[dbo].[order.lines]" comment="a \"quoted\" alias\non another line"
//ptah:schema:synonym name="local_orders" target="orders"
type PtahSchemaObjects struct{}
`)
	for _, single := range []bool{true, false} {
		t.Run(map[bool]string{true: "single", false: "split"}[single], func(t *testing.T) {
			c := qt.New(t)
			declared := must.Must(goschema.ParseSource(owners, "source.go", source))
			files, err := goschematogo.Render(t.Context(), &declared, goschematogo.Options{Annotations: owners, SingleFile: single})
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 1)
			parsed := must.Must(goschema.ParseSource(owners, files[0].Name, files[0].Data))
			c.Assert(must.Must(parsed.FeatureObjects.All()), qt.DeepEquals, must.Must(declared.FeatureObjects.All()))
			c.Assert(parsed.FeatureCoverage.KindRecords(), qt.DeepEquals, declared.FeatureCoverage.KindRecords())
			again, err := goschematogo.Render(t.Context(), &parsed, goschematogo.Options{Annotations: owners, SingleFile: single})
			c.Assert(err, qt.IsNil)
			c.Assert(again, qt.DeepEquals, files)
		})
	}
}

func TestGoExportRequiresTheAnnotationOwner(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(synonym.DeclaredObject(synonym.DesiredSynonym{Synonym: synonym.Synonym{Name: "orders", Target: "dbo.orders"}})))}
	files, err := goschematogo.Render(t.Context(), db, goschematogo.Options{Annotations: annotation.None()})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*Go annotations cannot represent feature object.*`)
	c.Assert(files, qt.IsNil)
}

func TestAnnotationEncoderKeepsImplicitTargetBinding(t *testing.T) {
	owners := must.Must(annotation.NewSet(synonym.Annotations()))
	for _, targets := range [][]string{nil, {platform.Oracle, platform.SQLServer}} {
		c := qt.New(t)
		object := synonym.DeclaredObject(synonym.DesiredSynonym{Synonym: synonym.Synonym{Name: "orders", Target: "dbo.orders"}})
		object.Targets = targets
		encoded := must.Must(owners.Encode(t.Context(), annotation.EncodeRequest{Objects: []schemaext.Object{object}}))
		c.Assert(encoded.Directives, qt.HasLen, 1)
		c.Assert(encoded.Directives[0].Targets, qt.IsNil)
		c.Assert(object.Targets, qt.DeepEquals, targets)
	}
}

func TestAnnotationEncoderRefusesUnrepresentableInput(t *testing.T) {
	owners := must.Must(annotation.NewSet(synonym.Annotations()))
	object := synonym.DeclaredObject(synonym.DesiredSynonym{Synonym: synonym.Synonym{Name: "orders", Target: "dbo.orders"}})
	narrow := object
	narrow.Targets = []string{platform.Postgres}
	wrong := object
	wrong.Ref.Name.Source, wrong.Ref.Name.Normalized = "other", "other"
	for _, test := range []struct {
		name     string
		request  annotation.EncodeRequest
		sentinel error
		message  string
	}{
		{"observed", annotation.EncodeRequest{Objects: []schemaext.Object{synonym.ObservedObject(synonym.ObservedSynonym{Synonym: synonym.Synonym{Name: "orders", Target: "dbo.orders"}})}}, ptaherr.ErrUnsupportedFeature, `.*Go annotations require a desired .*`},
		{"scope", annotation.EncodeRequest{Objects: []schemaext.Object{narrow}}, ptaherr.ErrUnsupportedFeature, `.*cannot preserve .* target scope.*postgres.*`},
		{"identity", annotation.EncodeRequest{Objects: []schemaext.Object{wrong}}, schemaext.ErrInvalidValue, `.*identity .* differs from its declaration`},
		{"namespace", annotation.EncodeRequest{Coverage: must.Must(synonym.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "catalog is unavailable"}, nil))}, ptaherr.ErrUnsupportedFeature, `.*namespace coverage: catalog is unavailable`},
		{"subject", annotation.EncodeRequest{Coverage: must.Must(synonym.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{Kind: synonym.Kind, Subject: object.Ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "properties are unavailable"}}}))}, ptaherr.ErrUnsupportedFeature, `.*coverage: properties are unavailable`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := owners.Encode(t.Context(), test.request)
			c.Assert(err, qt.ErrorIs, test.sentinel)
			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(result, qt.DeepEquals, annotation.EncodeResult{})
		})
	}
}

func TestAnnotationEncoderRefusesOneSynonymTarget(t *testing.T) {
	c := qt.New(t)
	owners := must.Must(annotation.NewSet(synonym.Annotations()))
	object := synonym.DeclaredObject(synonym.DesiredSynonym{Synonym: synonym.Synonym{Name: "orders", Target: "dbo.orders"}})
	object.Targets = []string{platform.SQLServer}
	result, err := owners.Encode(t.Context(), annotation.EncodeRequest{Objects: []schemaext.Object{object}})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*cannot preserve synonym .* target scope.*sqlserver.*`)
	c.Assert(result, qt.DeepEquals, annotation.EncodeResult{})
}
