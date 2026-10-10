package featureselect_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/featureselect"
)

const (
	spanKind  schemaext.Kind = "example.org/span"
	plainKind schemaext.Kind = "example.org/plain"
)

// span is a fixture model that binds several tables without belonging to one,
// the shape of a SQL Server security policy.
type span struct {
	Tables []string `json:"tables"`
}

func (*span) Kind() schemaext.Kind { return spanKind }
func (v *span) Clone() schemaext.Value {
	return &span{Tables: slices.Clone(v.Tables)}
}
func (v *span) Equal(other schemaext.Value) bool {
	right, ok := other.(*span)
	return ok && slices.Equal(v.Tables, right.Tables)
}

// plain is a fixture model whose owner describes no relations.
type plain struct{}

func (*plain) Kind() schemaext.Kind             { return plainKind }
func (*plain) Clone() schemaext.Value           { return &plain{} }
func (*plain) Equal(other schemaext.Value) bool { _, ok := other.(*plain); return ok }

// spanRelations reports every table a span names as a dependency.
type spanRelations struct{}

func (spanRelations) DescribeRelations(_ context.Context, request schemaext.RelationRequest) (schemaext.RelationResult, error) {
	builder := objectidentity.NewBuilder(request.Identifiers)
	result := schemaext.RelationResult{Complete: true}
	for _, value := range request.Values {
		record := schemaext.ValueRelations{Subject: value.Subject, Complete: true}
		for _, table := range value.Value.(*span).Tables {
			record.Dependencies = append(record.Dependencies, builder.Table(table))
		}
		result.Values = append(result.Values, record)
	}
	return result, nil
}

func modelCodecs[T schemaext.Value](prototype T) []schemaext.Codec {
	var codecs []schemaext.Codec
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		codecs = append(codecs, schemaext.ModelCodec[T]{Prototype: prototype, Representation: representation, Version: 1,
			Definition: json.RawMessage(`{"type":"object"}`)}.Codec())
	}
	return codecs
}

// fixtureRuntime registers the span owner, with relation discovery on both
// representations, and the plain owner, without any, on one target.
func fixtureRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{ID: "example.org/fixture",
		Codecs:  slices.Concat(modelCodecs(&span{}), modelCodecs(&plain{})),
		Targets: []engine.Target{{Name: "spans"}},
		Relations: []engine.RelationDiscovery{
			{Target: "spans", Representation: schemaext.Desired, Kinds: []schemaext.Kind{spanKind}, Service: spanRelations{}},
			{Target: "spans", Representation: schemaext.Observed, Kinds: []schemaext.Kind{spanKind}, Service: spanRelations{}},
		}}))
}

var semantics = identifier.ForDialect("postgres")

func ref(kind schemaext.Kind, name string) objectidentity.ID {
	return objectidentity.NewBuilder(semantics).SchemaScopedParts(objectidentity.Kind(kind), "app", name)
}

func side(representation schemaext.Representation, objects ...schemaext.Object) featureselect.Side {
	kinds := make([]schemaext.KindCoverage, 0)
	for _, model := range fixtureRuntime().Codecs().Definitions() {
		if model.Representation == representation {
			kinds = append(kinds, schemaext.KindCoverage{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
		}
	}
	var subjects []schemaext.SubjectCoverage
	for _, object := range objects {
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: object.Value.Kind(), Subject: object.Ref,
			Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
	}
	return featureselect.Side{Representation: representation, Objects: must.Must(schemaext.NewObjects(objects...)),
		Coverage: must.Must(schemaext.NewCoverage(representation, kinds, subjects))}
}

func keepTables(names ...string) func(schema, table string) bool {
	return func(schema, table string) bool { return slices.Contains(names, schema+"."+table) }
}

// TestBindingsSelect_HappyPath pins how a selection decides an object that
// binds tables without belonging to one: kept whole when every table it binds
// is selected, left out with its knowledge when none is, and kept when its
// owner records no table or describes no relations.
func TestBindingsSelect_HappyPath(t *testing.T) {
	c := qt.New(t)
	current := side(schemaext.Observed,
		schemaext.Object{Ref: ref(spanKind, "both"), Value: &span{Tables: []string{"app.a", "app.b"}}},
		schemaext.Object{Ref: ref(spanKind, "elsewhere"), Value: &span{Tables: []string{"app.c"}}},
		schemaext.Object{Ref: ref(spanKind, "none"), Value: &span{}},
		schemaext.Object{Ref: ref(plainKind, "plain"), Value: &plain{}})
	bindings := must.Must(featureselect.CaptureBindings(context.Background(), fixtureRuntime(), "spans", semantics, current))

	objects, coverage, err := bindings.Select(current.Objects, current.Coverage, keepTables("app.a", "app.b"))

	c.Assert(err, qt.IsNil)
	c.Assert(objects.Refs(), qt.DeepEquals, []objectidentity.ID{ref(plainKind, "plain"), ref(spanKind, "both"), ref(spanKind, "none")})
	var subjects []objectidentity.ID
	for _, record := range coverage.SubjectRecords() {
		subjects = append(subjects, record.Subject)
	}
	c.Assert(subjects, qt.Not(qt.Contains), ref(spanKind, "elsewhere"))
	c.Assert(subjects, qt.HasLen, 3)
}

// TestBindingsSelect_FailurePath pins the refusal of a scope that selects
// some of the tables an object binds, including a table only the other side
// of the comparison binds, and that the zero bindings never refuse.
func TestBindingsSelect_FailurePath(t *testing.T) {
	declared := side(schemaext.Desired, schemaext.Object{Ref: ref(spanKind, "moved"), Value: &span{Tables: []string{"app.a"}}})
	tests := []struct {
		name    string
		sides   []featureselect.Side
		keep    []string
		wantErr string
	}{
		{name: "one of two tables", keep: []string{"app.a"},
			sides:   []featureselect.Side{side(schemaext.Observed, schemaext.Object{Ref: ref(spanKind, "both"), Value: &span{Tables: []string{"app.a", "app.b"}}})},
			wantErr: `the scope selects part of a feature object: example.org/span app.both binds app.a, which the scope selects, and app.b, which it does not; select every table it binds or none of them`},
		{name: "a table only the current side binds", keep: []string{"app.a"},
			sides:   []featureselect.Side{declared, side(schemaext.Observed, schemaext.Object{Ref: ref(spanKind, "moved"), Value: &span{Tables: []string{"app.b"}}})},
			wantErr: `.*example.org/span app.moved binds app.a, which the scope selects, and app.b, which it does not.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			bindings := must.Must(featureselect.CaptureBindings(context.Background(), fixtureRuntime(), "spans", semantics, test.sides...))
			objects, coverage, err := bindings.Select(test.sides[0].Objects, test.sides[0].Coverage, keepTables(test.keep...))
			c.Assert(err, qt.ErrorIs, featureselect.ErrPartialScope)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(objects.Len(), qt.Equals, 0)
			c.Assert(coverage.SubjectRecords(), qt.HasLen, 0)
		})
	}
}

// TestBindingsSelect_ZeroBindingsKeepStandaloneObjects pins that selecting
// without captured bindings keeps every standalone object, as Tables does.
func TestBindingsSelect_ZeroBindingsKeepStandaloneObjects(t *testing.T) {
	c := qt.New(t)
	current := side(schemaext.Observed, schemaext.Object{Ref: ref(spanKind, "both"), Value: &span{Tables: []string{"app.a", "app.b"}}})

	objects, _, err := featureselect.Bindings{}.Select(current.Objects, current.Coverage, keepTables("app.a"))

	c.Assert(err, qt.IsNil)
	c.Assert(objects.Refs(), qt.DeepEquals, []objectidentity.ID{ref(spanKind, "both")})
}

// TestCaptureBindings_SecurityPolicy pins the first owner with relation
// discovery: a SQL Server security policy binding two tables is refused for a
// scope holding one of them and kept for a scope holding both.
func TestCaptureBindings_SecurityPolicy(t *testing.T) {
	c := qt.New(t)
	predicate := func(table string) mssqlschema.Predicate {
		return mssqlschema.Predicate{Type: mssqlschema.Filter, Function: mssqlschema.ObjectName{Schema: "rls", Name: "fn"},
			Arguments: []string{"tenant_id"}, Table: mssqlschema.ObjectName{Schema: "app", Name: table}}
	}
	policy := must.Must(mssqlschema.DesiredSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", "tenancy"),
		mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{predicate("orders"), predicate("invoices")}}))
	declared := featureselect.Side{Representation: schemaext.Desired, Objects: must.Must(schemaext.NewObjects(policy)),
		Coverage: must.Must(mssqlschema.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))}
	bindings := must.Must(featureselect.CaptureBindings(context.Background(), must.Must(builtin.New()), "sqlserver",
		identifier.ForDialect("sqlserver"), declared))

	_, _, partial := bindings.Select(declared.Objects, declared.Coverage, keepTables("app.orders"))
	whole, _, err := bindings.Select(declared.Objects, declared.Coverage, keepTables("app.orders", "app.invoices"))

	c.Assert(partial, qt.ErrorIs, featureselect.ErrPartialScope)
	c.Assert(partial, qt.ErrorMatches, `.*ptah.run/mssql/security-policy rls.tenancy binds app.orders, which the scope selects, and app.invoices, which it does not.*`)
	c.Assert(err, qt.IsNil)
	c.Assert(whole.Len(), qt.Equals, 1)
	c.Assert(fmt.Sprint(whole.Refs()[0].Name.Source), qt.Equals, "tenancy")
}
