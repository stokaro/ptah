package mssqlproperty_test

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
	"ptah.run/dialect/mssql/mssqlproperty"
	"ptah.run/internal/convert/goschematogo"
)

// The source has no table to carry standalone declarations. Every address and
// literal must survive export, including an empty required value.
func TestGoExportRoundTripWithoutTables(t *testing.T) {
	owners := must.Must(annotation.NewSet(mssqlproperty.Annotations()))
	source := []byte(`package models

//ptah:schema:extendedproperty name="classification" schema="app" table="orders" column="email" value="pii" comment="a \"quoted\" value\non another line"
//ptah:schema:extendedproperty name="classification" schema="app" table="orders" value="table"
//ptah:schema:extendedproperty name="classification" schema="app" value="schema"
//ptah:schema:extendedproperty name="classification" value=""
//ptah:schema:extendedproperty name="padding" value="  "
type PtahSchemaObjects struct{}
`)
	for _, single := range []bool{true, false} {
		t.Run(map[bool]string{true: "single", false: "split"}[single], func(t *testing.T) {
			c := qt.New(t)
			declared, err := goschema.ParseSource(owners, "source.go", source)
			c.Assert(err, qt.IsNil)
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
	db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(mssqlproperty.DeclaredObject(mssqlproperty.DesiredProperty{Property: mssqlproperty.Property{Name: "classification", Value: "pii"}})))}
	files, err := goschematogo.Render(t.Context(), db, goschematogo.Options{Annotations: annotation.None()})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*Go annotations cannot represent feature object.*`)
	c.Assert(files, qt.IsNil)
}

func TestAnnotationEncoderKeepsImplicitTargetBinding(t *testing.T) {
	owners := must.Must(annotation.NewSet(mssqlproperty.Annotations()))
	for _, targets := range [][]string{nil, {platform.SQLServer}} {
		c := qt.New(t)
		object := mssqlproperty.DeclaredObject(mssqlproperty.DesiredProperty{Property: mssqlproperty.Property{Name: "classification", Value: "pii"}})
		object.Targets = targets
		encoded := must.Must(owners.Encode(t.Context(), annotation.EncodeRequest{Objects: []schemaext.Object{object}}))
		c.Assert(encoded.Directives, qt.HasLen, 1)
		c.Assert(encoded.Directives[0].Targets, qt.IsNil)
		c.Assert(object.Targets, qt.DeepEquals, targets)
	}
}

func TestAnnotationEncoderRefusesUnrepresentableInput(t *testing.T) {
	owners := must.Must(annotation.NewSet(mssqlproperty.Annotations()))
	object := mssqlproperty.DeclaredObject(mssqlproperty.DesiredProperty{Property: mssqlproperty.Property{Name: "classification", Value: "pii"}})
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
		{"observed", annotation.EncodeRequest{Objects: []schemaext.Object{mssqlproperty.ObservedObject(mssqlproperty.ObservedProperty{Property: mssqlproperty.Property{Name: "classification", Value: "pii"}, ValueType: "nvarchar"})}}, ptaherr.ErrUnsupportedFeature, `.*Go annotations require a desired .*`},
		{"scope", annotation.EncodeRequest{Objects: []schemaext.Object{narrow}}, ptaherr.ErrUnsupportedFeature, `.*cannot preserve .* target scope.*postgres.*`},
		{"identity", annotation.EncodeRequest{Objects: []schemaext.Object{wrong}}, schemaext.ErrInvalidValue, `.*identity .* differs from its declaration`},
		{"namespace", annotation.EncodeRequest{Coverage: must.Must(mssqlproperty.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "catalog is unavailable"}, nil))}, ptaherr.ErrUnsupportedFeature, `.*namespace coverage: catalog is unavailable`},
		{"subject", annotation.EncodeRequest{Coverage: must.Must(mssqlproperty.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{Kind: mssqlproperty.Kind, Subject: object.Ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "properties are unavailable"}}}))}, ptaherr.ErrUnsupportedFeature, `.*coverage: properties are unavailable`},
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

func TestAnnotationEncoderRefusesObjectCommentsAsProperties(t *testing.T) {
	c := qt.New(t)
	owners := must.Must(annotation.NewSet(mssqlproperty.Annotations()))
	object := mssqlproperty.DeclaredObject(mssqlproperty.DesiredProperty{Property: mssqlproperty.Property{Name: "ms_description", Value: "comment"}})
	result, err := owners.Encode(t.Context(), annotation.EncodeRequest{Objects: []schemaext.Object{object}})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*represent MS_Description as an object comment`)
	c.Assert(result, qt.DeepEquals, annotation.EncodeResult{})
}

// Empty values are data; missing values and empty identities are mistakes.
func TestPropertyAnnotationRequiresItsAttributes(t *testing.T) {
	owners := must.Must(annotation.NewSet(mssqlproperty.Annotations()))
	for _, test := range []struct{ name, attributes, want string }{
		{"missing value", `name="classification"`, "value"},
		{"missing name", `value=""`, "name"},
		{"empty name", `name="" value=""`, "name"},
		{"blank name", `name=" " value=""`, "name"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := goschema.ParseSource(owners, "source.go", []byte("package models\n//ptah:schema:extendedproperty "+test.attributes+"\ntype Property struct{}\n"))
			c.Assert(err, qt.ErrorIs, ptaherr.ErrMissingRequiredAttribute)
			c.Assert(err, qt.ErrorMatches, `.*missing required annotation attribute "`+test.want+`".*`)
		})
	}
}
