package pgtablesource_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/pgtablesource"
	"ptah.run/internal/sqlschema"
)

func TestExclusionSources_QuotedHCLRoundTrip(t *testing.T) {
	for _, test := range []struct{ name, sql string }{
		{name: "mixed case", sql: `CREATE TABLE "Rooms" (room int, CONSTRAINT "NoOverlap" EXCLUDE USING btree (room WITH =));`},
		{name: "dotted names", sql: `CREATE TABLE "App.Space"."Rooms.Dot" (room int, CONSTRAINT "No.Overlap" EXCLUDE USING btree (room WITH =));`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owners := must.Must(hclext.NewSet(pgtablesource.ExclusionHCL()))
			db, _, err := sqlschema.ReadWith(t.Context(), []byte(test.sql), "postgres", nil, sqlschema.Options{Owners: exclusionSQLOwners()})
			c.Assert(err, qt.IsNil)
			rendered, err := atlashclrender.Render(&db, atlashclrender.Options{HCL: owners, Dialect: "postgres"})
			c.Assert(err, qt.IsNil)
			c.Assert(rendered.Diagnostics, qt.HasLen, 0)
			returned, err := atlashcl.ParseWithOptions(t.Context(), rendered.Data, "schema.hcl", atlashcl.Options{HCL: owners})
			c.Assert(err, qt.IsNil)
			c.Assert(returned.FeatureObjects.Refs(), qt.DeepEquals, db.FeatureObjects.Refs())
			c.Assert(returned.FeatureObjects.Len(), qt.Equals, 1)
		})
	}
}

const exclusionGo = `package entities
//ptah:schema:table name="rooms" schema="app"
//ptah:schema:exclude name="no_overlap" using="btree" elements="room WITH =" condition="room > 0" deferrable initially="deferred" comment="Room's schedule" requires="btree_gist"
//ptah:schema:exclude name="other" using="btree" elements="(room + 1) WITH ="
type Room struct {
 //ptah:schema:field name="room" type="INTEGER"
 Room int
}
`

func exclusionSourceOwners() annotation.Set {
	return must.Must(annotation.NewSet(pgtablesource.ExclusionAnnotations()))
}

func TestExclusionSources_GoReachesBuiltinRenderer(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	db, err := goschema.ParseSource(runtime.Annotations(), "schema.go", exclusionGo)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Constraints, qt.HasLen, 0)
	c.Assert(db.FeatureObjects.Select(func(ref objectidentity.ID) bool { return ref.Kind == objectidentity.Kind(pgtable.ExclusionKind) }).Len(), qt.Equals, 2)
	sql, err := builtin.GetOrderedCreateStatements(&db, "postgres")
	c.Assert(err, qt.IsNil)
	text := strings.Join(sql, "\n")
	c.Assert(text, qt.Contains, `CONSTRAINT "no_overlap" EXCLUDE USING btree (room WITH =) WHERE (room > 0) DEFERRABLE INITIALLY DEFERRED`)
	c.Assert(text, qt.Contains, `CONSTRAINT "other" EXCLUDE USING btree ((room + 1) WITH =)`)
	c.Assert(text, qt.Contains, "ADD CONSTRAINT")
	c.Assert(text, qt.Contains, "IS 'Room''s schedule'")
}

func TestExclusionSources_GoRoundTrip(t *testing.T) {
	c := qt.New(t)
	owners := exclusionSourceOwners()
	db, err := goschema.ParseSource(owners, "schema.go", exclusionGo)
	c.Assert(err, qt.IsNil)
	files, err := goschematogo.Render(t.Context(), &db, goschematogo.Options{Annotations: owners, PackageName: "entities", SingleFile: true, Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	returned, err := goschema.ParseSource(owners, files[0].Name, files[0].Data)
	c.Assert(err, qt.IsNil)
	c.Assert(returned.FeatureObjects.Equal(db.FeatureObjects), qt.IsTrue)
	c.Assert(string(files[0].Data), qt.Contains, "ptah:schema:exclude")
}

func TestExclusionSources_YAMLParity(t *testing.T) {
	c := qt.New(t)
	declared, err := goschema.ParseSource(exclusionSourceOwners(), "schema.go", exclusionGo)
	c.Assert(err, qt.IsNil)
	owners := must.Must(yamlext.NewSet(pgtablesource.ExclusionYAML()))
	db, err := yamlschema.Parse(t.Context(), owners, []byte(`tables:
  rooms:
    name: rooms
    schema: app
    struct_name: Room
    columns:
      room: {type: INTEGER}
exclusions:
  no_overlap:
    table: app.rooms
    struct_name: Room
    using: btree
    elements: room WITH =
    condition: room > 0
    deferrable: true
    initially: deferred
    comment: "Room's schedule"
    requires: [btree_gist]
  other:
    table: app.rooms
    struct_name: Room
    using: btree
    elements: (room + 1) WITH =
`))
	c.Assert(err, qt.IsNil)
	c.Assert(db.Constraints, qt.HasLen, 0)
	c.Assert(db.FeatureObjects.Equal(declared.FeatureObjects), qt.IsTrue)
}

func TestExclusionSources_HCLRoundTrip(t *testing.T) {
	c := qt.New(t)
	owners := must.Must(hclext.NewSet(pgtablesource.ExclusionHCL()))
	db, err := goschema.ParseSource(exclusionSourceOwners(), "schema.go", exclusionGo)
	c.Assert(err, qt.IsNil)
	text, err := atlashclrender.Render(&db, atlashclrender.Options{HCL: owners, Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	c.Assert(text.Diagnostics, qt.HasLen, 0)
	returned, err := atlashcl.ParseWithOptions(t.Context(), text.Data, "schema.hcl", atlashcl.Options{HCL: owners})
	c.Assert(err, qt.IsNil)
	c.Assert(returned.Constraints, qt.HasLen, 0)
	c.Assert(returned.FeatureObjects.Len(), qt.Equals, 2)
	for _, name := range []string{"no_overlap", "other"} {
		ref := pgtable.ExclusionRef("app.rooms", name, identifier.ForDialect("postgres"))
		before, found, err := db.FeatureObjects.Get(ref)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		after, found, err := returned.FeatureObjects.Get(ref)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		c.Assert(after.Value.(*pgtable.DesiredExclusion).Exclusion, qt.DeepEquals, before.Value.(*pgtable.DesiredExclusion).Exclusion)
	}
}

func TestExclusionSources_RefuseMalformedGo(t *testing.T) {
	for _, directive := range []string{
		`name="bad" using="btree"`,
		`name="bad" using="btree" elements="room WITH =" unknown="x"`,
		`name="bad" using="btree" elements="room WITH =" deferrable="typo"`,
		`name="bad" using="btree" elements="room WITH =" initially="deferred"`,
		`name="bad" using="btree" elements="room WITH =" requires="ext,ext"`,
		`name="bad" using="btree" elements="room WITH =" requires="ext,"`,
	} {
		t.Run(directive, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource(exclusionSourceOwners(), "schema.go", "package entities\n//ptah:schema:table name=\"rooms\"\n//ptah:schema:exclude "+directive+"\ntype Room struct{}\n")
			c.Assert(err, qt.IsNotNil)
			c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

func TestExclusionSources_RefuseDuplicates(t *testing.T) {
	c := qt.New(t)
	_, err := goschema.ParseSource(exclusionSourceOwners(), "schema.go", strings.Replace(exclusionGo, `name="other"`, `name="no_overlap"`, 1))
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
}

func TestExclusionSources_HCLRefusesDuplicateConstraints(t *testing.T) {
	c := qt.New(t)
	owners, err := hclext.NewSet(pgtablesource.ExclusionHCL())
	c.Assert(err, qt.IsNil)
	db, err := atlashcl.ParseWithOptions(t.Context(), []byte(`schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
  exclude {
    name = "users_id_excl"
    using = "gist"
    elements = "id WITH ="
  }
  exclude {
    name = "users_id_excl"
    using = "gist"
    elements = "id WITH ="
  }
}`), "schema.hcl", atlashcl.Options{HCL: owners})
	c.Assert(err, qt.ErrorMatches, `(?s).*declared twice: duplicate feature identity:.*users_id_excl`)
	c.Assert(db, qt.IsNil)
}

func exclusionExportRequest() annotation.EncodeRequest {
	ref := pgtable.ExclusionRef("app.rooms", "no_overlap", identifier.ForDialect("postgres"))
	value := &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "btree", Elements: "room WITH ="}}
	return annotation.EncodeRequest{Objects: []schemaext.Object{{Ref: ref, Value: value}}, Tables: annotation.Tables{{Schema: "app", Name: "rooms", Struct: "Room"}}}
}

func TestExclusionSources_RefuseLossyExports(t *testing.T) {
	c := qt.New(t)
	request := exclusionExportRequest()
	request.Coverage = must.Must(pgtable.ExclusionCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: pgtable.ExclusionKind, Subject: request.Objects[0].Ref, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "catalog read is incomplete"}}}))
	encoded, err := pgtablesource.ExclusionAnnotations().Encoder.EncodeAnnotations(t.Context(), request)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(encoded, qt.DeepEquals, annotation.EncodeResult{})
	hcl, err := pgtablesource.ExclusionHCL().Encoder.EncodeHCL(t.Context(), hclext.EncodeRequest{Objects: request.Objects, Coverage: request.Coverage})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(hcl, qt.DeepEquals, hclext.EncodeResult{})
}

func TestExclusionSources_Cancellation(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	encoded, err := pgtablesource.ExclusionAnnotations().Encoder.EncodeAnnotations(ctx, exclusionExportRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(encoded, qt.DeepEquals, annotation.EncodeResult{})
	decoded, err := pgtablesource.ExclusionHCL().Decoder.DecodeHCL(ctx, hclext.Request{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(decoded, qt.DeepEquals, hclext.Result{})
}

func TestExclusionSources_RetentionPreservesMeaningAndScope(t *testing.T) {
	c := qt.New(t)
	db, err := goschema.ParseSource(exclusionSourceOwners(), "schema.go", exclusionGo)
	c.Assert(err, qt.IsNil)
	declaration := annotation.Declaration{Directive: pgtablesource.ExclusionDirective, Struct: "Room", Attributes: map[string]string{
		"name": "other", "using": "btree", "elements": "(room + 1) WITH =",
	}}
	request := annotation.RetentionRequest{Declarations: []annotation.Declaration{declaration}, Tables: annotation.Tables{{Schema: "app", Name: "rooms", Struct: "Room"}}, Objects: db.FeatureObjects}
	retained, err := exclusionSourceOwners().Retained(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(retained, qt.DeepEquals, []bool{true})
	request.Declarations[0].Targets = []string{"postgres"}
	retained, err = exclusionSourceOwners().Retained(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(retained, qt.DeepEquals, []bool{false})
	request.Declarations[0].Targets = nil
	request.Declarations[0].Attributes["elements"] = "room WITH ="
	retained, err = exclusionSourceOwners().Retained(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(retained, qt.DeepEquals, []bool{false})
	c.Assert(db.FeatureObjects.Equal(request.Objects), qt.IsTrue)
}
