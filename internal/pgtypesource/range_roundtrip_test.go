package pgtypesource_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/pgtypeprovider"
	"ptah.run/internal/pgtypesource"
	"ptah.run/internal/sqlschema"
)

func TestRangeSourceRoundTripPreservesQuotedNames(t *testing.T) {
	for _, test := range []struct{ name, sql, schema, typeName string }{
		{name: "spaces", sql: `CREATE TYPE " app "." span " AS RANGE (SUBTYPE = numeric);`, schema: " app ", typeName: " span "},
		{name: "literal quotes", sql: `CREATE TYPE """app"""."""span""" AS RANGE (SUBTYPE = numeric);`, schema: `"app"`, typeName: `"span"`},
		{name: "literal dots", sql: `CREATE TYPE "app.ns"."span.type" AS RANGE (SUBTYPE = numeric);`, schema: "app.ns", typeName: "span.type"},
		{name: "quoted case", sql: `CREATE TYPE "App"."Span" AS RANGE (SUBTYPE = numeric);`, schema: "App", typeName: "Span"},
		{name: "comment with explicit default schema", sql: `CREATE TYPE span AS RANGE (SUBTYPE = numeric); COMMENT ON TYPE public.span IS 'range';`, typeName: "span"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := builtintest.Runtime()
			original, _, err := sqlschema.ReadWith(c.Context(), []byte(test.sql), "postgres", nil, sqlschema.Options{Owners: runtime.SQL()})
			c.Assert(err, qt.IsNil)
			object, value := oneRange(c, &original)
			c.Assert(object.Ref.Name.Source, qt.Equals, test.typeName)
			c.Assert(pgtype.AuthoredSchema(object.Ref), qt.Equals, test.schema)
			files, err := goschematogo.Render(c.Context(), &original, goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true})
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 1)
			fromGo, err := goschema.ParseSource(runtime.Annotations(), files[0].Name, files[0].Data)
			c.Assert(err, qt.IsNil)
			hcl, err := atlashclrender.Render(&original, atlashclrender.Options{HCL: runtime.HCL()})
			c.Assert(err, qt.IsNil)
			fromHCL, err := atlashcl.ParseWithOptions(t.Context(), hcl.Data, "roundtrip.hcl", atlashcl.Options{HCL: runtime.HCL()})
			c.Assert(err, qt.IsNil)
			for i, read := range []*schemamodel.Database{&fromGo, fromHCL} {
				after, model := oneRange(c, read)
				c.Assert(after.Ref, qt.DeepEquals, object.Ref, qt.Commentf("source %d: Go=%s HCL=%s", i, files[0].Data, hcl.Data))
				model.StructName = ""
				c.Assert(model, qt.DeepEquals, value)
			}
		})
	}
}

func TestRangeLimitsPreserveUnmanagedTypes(t *testing.T) {
	ref := pgtype.Ref(pgtype.RangeKind, " app ", `"span.type"`)
	for _, test := range []struct {
		name     string
		coverage schemaext.Coverage
		state    schemaext.KnowledgeState
		subjects int
	}{
		{name: "uninspected namespace", coverage: must.Must(pgtype.Coverage(pgtype.RangeKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "catalog is unavailable"}, nil)), state: schemaext.Uninspected},
		{name: "one unread type", coverage: must.Must(pgtype.Coverage(pgtype.RangeKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{Kind: pgtype.RangeKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "catalog row is incomplete"}}})), state: schemaext.Complete, subjects: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owners := must.Must(annotation.NewSet(pgtypesource.Annotations(pgtypeprovider.Targets())))
			original := &schemamodel.Database{FeatureCoverage: test.coverage}
			files, err := goschematogo.Render(c.Context(), original, goschematogo.Options{Annotations: owners, SingleFile: true})
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 1)
			fromGo, err := goschema.ParseSource(owners, files[0].Name, files[0].Data)
			c.Assert(err, qt.IsNil)
			hcl, err := atlashclrender.Render(original, atlashclrender.Options{HCL: builtintest.Runtime().HCL()})
			c.Assert(err, qt.IsNil)
			fromHCL, err := atlashcl.ParseWithOptions(t.Context(), hcl.Data, "limited.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
			c.Assert(err, qt.IsNil)
			for _, read := range []*schemamodel.Database{&fromGo, fromHCL} {
				known := read.FeatureCoverage.SelectKinds([]schemaext.Kind{pgtype.RangeKind})
				c.Assert(known.KindRecords(), qt.HasLen, 1)
				c.Assert(known.KindRecords()[0].Knowledge.Unscoped().State, qt.Equals, test.state)
				c.Assert(known.Lookup(pgtype.RangeKind, ref).State, qt.Equals, schemaext.Uninspected)
				c.Assert(known.SubjectRecords(), qt.HasLen, test.subjects)
			}
		})
	}
}

// TestRangeLimitsAbsentCoverageWritesNoLimit pins that a description holding
// no range record writes no limit in either source format. A read of a target
// without range types, such as SQLite or MySQL, records none, and a limit
// there would declare a gap in a family the target does not have.
func TestRangeLimitsAbsentCoverageWritesNoLimit(t *testing.T) {
	c := qt.New(t)
	owners := must.Must(annotation.NewSet(pgtypesource.Annotations(pgtypeprovider.Targets())))
	original := &schemamodel.Database{}

	files, err := goschematogo.Render(c.Context(), original, goschematogo.Options{Annotations: owners, SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(string(files[0].Data), qt.Not(qt.Contains), "notdescribed")

	hcl, err := atlashclrender.Render(original, atlashclrender.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(string(hcl.Data), qt.Not(qt.Contains), "not-described")
}

func TestRangeLimitsRefuseLossOfCompleteException(t *testing.T) {
	c := qt.New(t)
	ref := pgtype.Ref(pgtype.RangeKind, "app", "span")
	known := must.Must(pgtype.Coverage(pgtype.RangeKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "partial source"}, []schemaext.SubjectCoverage{{Kind: pgtype.RangeKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}))
	encoded, err := must.Must(annotation.NewSet(pgtypesource.Annotations(pgtypeprovider.Targets()))).Encode(c.Context(), annotation.EncodeRequest{Coverage: known})
	c.Assert(err, qt.ErrorMatches, `.*cannot preserve a complete subject inside an unknown namespace`)
	c.Assert(encoded, qt.DeepEquals, annotation.EncodeResult{})
}

func TestRangeHCLCanceledBatchReturnsNoDeclarations(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	result, err := pgtypesource.HCL(pgtypeprovider.Targets()).Decoder.DecodeHCL(ctx, hclext.Request{Claims: []hclext.Claim{{Block: "range", Name: "span", Attributes: map[string]hclext.Value{"subtype": {Text: "numeric"}}}}})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, hclext.Result{})
}
