package schemadiff_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
	"ptah.run/migration/schemadiff"
)

type objectProbeRuntime struct {
	engine.SchemaRuntime
	normalize func(context.Context, schemaext.NormalizationRequest) (schemaext.NormalizationResult, error)
	validated *schemamodel.Database
	stop      error
}

func (r *objectProbeRuntime) NormalizeObjects(ctx context.Context, request schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
	return r.normalize(ctx, request)
}

func (r *objectProbeRuntime) ValidateSchema(_ context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	r.validated = request.Schema
	return schemavalidation.Result{}, r.stop
}

// serverSpelling is what PostgreSQL stores for the declared clause.
const serverSpelling = "(EXISTS ( SELECT 1\n   FROM orders))"

// TestLiveComparisonNormalizesObjectsInTheSessionsSchema holds the identities
// a normalization request carries to the connection's default schema,
// `reporting`. The reader writes that schema as empty, and a policy built
// without a schema holds PostgreSQL's offline default, public: unbound, the
// policy read on `reporting.audit` reaches the owner as `public.audit`, the
// probe finds no observation for the declaration, and the comparison plans an
// ALTER POLICY on every run. The answer comes back under the identity the
// declaration was given.
func TestLiveComparisonNormalizesObjectsInTheSessionsSchema(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{name: "a policy declared on reporting.audit", schema: "reporting"},
		{name: "a policy declared on audit", schema: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := pgpolicy.PolicyRef(test.schema, "audit", "audit_visible")
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{Schema: test.schema, Name: "audit", StructName: "Audit"}},
				FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgpolicy.DesiredPolicyObject(
					declared, pgpolicy.DesiredPolicy{Command: pgpolicy.CommandAll, Using: new("EXISTS (SELECT 1 FROM orders)")})))),
			}
			current := &catalog.Database{
				Tables: []catalog.Table{{Name: "audit"}},
				FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgpolicy.ObservedPolicyObject(
					pgpolicy.PolicyRef("", "audit", "audit_visible"),
					pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}},
						Using: new(serverSpelling), Composition: pgpolicy.Permissive})))),
			}
			var captured schemaext.NormalizationRequest
			stop := errors.New("stop at validation")
			runtime := &objectProbeRuntime{SchemaRuntime: must.Must(builtin.New()), stop: stop,
				normalize: func(_ context.Context, request schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
					captured = request
					objects := must.Must(request.Desired.Objects.All())
					answered := objects[0].Value.(*pgpolicy.DesiredPolicy).Copy()
					answered.Normalized = &pgpolicy.NormalizedPolicy{Using: new(serverSpelling), Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}}
					objects[0].Value = answered
					return schemaext.NormalizationResult{Complete: true,
						Desired: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(objects...)), Coverage: request.Desired.Coverage}}, nil
				}}

			diff, err := schemadiff.CompareWithDatabase(t.Context(), facetProbeConnection{schema: "reporting"}, desired, current, nil, runtime)

			c.Assert(err, qt.ErrorIs, stop)
			c.Assert(diff, qt.IsNil)
			c.Assert(captured.Desired.Objects.Refs(), qt.HasLen, 1)
			c.Assert(captured.Current.Objects.Refs(), qt.HasLen, 1)
			c.Assert(captured.Desired.Objects.Refs()[0].Schema.Normalized, qt.Equals, "reporting")
			c.Assert(captured.Current.Objects.Refs()[0].Key(), qt.Equals, captured.Desired.Objects.Refs()[0].Key())
			c.Assert(runtime.validated, qt.IsNotNil)
			c.Assert(runtime.validated.FeatureObjects.Refs(), qt.DeepEquals, desired.FeatureObjects.Refs())
			object, found, err := runtime.validated.FeatureObjects.Get(declared)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			value, ok := object.Value.(*pgpolicy.DesiredPolicy)
			c.Assert(ok, qt.IsTrue)
			c.Assert(value.Normalized, qt.IsNotNil)
			c.Assert(*value.Normalized.Using, qt.Equals, serverSpelling)
		})
	}
}
