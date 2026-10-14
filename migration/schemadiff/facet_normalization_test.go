package schemadiff_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
	"ptah.run/migration/schemadiff"
)

type facetProbeConnection struct{ schema string }

func (c facetProbeConnection) Info() catalog.ServerInfo {
	semantics := identifier.ForDialect("postgres")
	semantics.DefaultSchema = c.schema
	return catalog.ServerInfo{Dialect: "postgres", Schema: c.schema, IdentifierSemantics: semantics}
}
func (c facetProbeConnection) ResolveIdentifierSemantics(context.Context, []string) (identifier.Semantics, error) {
	return c.Info().IdentifierSemantics, nil
}
func (facetProbeConnection) WithRolledBackTransaction(context.Context, string, func(context.Context, *sql.Tx) error) (bool, error) {
	return false, nil
}

type facetProbeRuntime struct {
	engine.SchemaRuntime
	normalize func(context.Context, schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error)
	validated *schemamodel.Database
	stop      error
}

func (r *facetProbeRuntime) NormalizeFacets(ctx context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
	return r.normalize(ctx, request)
}
func (r *facetProbeRuntime) ValidateSchema(_ context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	r.validated = request.Schema
	return schemavalidation.Result{}, r.stop
}

func facetProbeSchema() *schemamodel.Database {
	return &schemamodel.Database{Tables: []schemamodel.Table{{Name: "items", StructName: "Item", Facets: must.Must(schemaext.NewFacets(&pgpolicy.DesiredTableState{}))}}}
}

// TestLiveComparisonNormalizesFacetsBeforeValidation proves the connected
// entry point dispatches attached settings without any named feature object.
func TestLiveComparisonNormalizesFacetsBeforeValidation(t *testing.T) {
	c := qt.New(t)
	conn := facetProbeConnection{schema: "app"}
	desired := facetProbeSchema()
	current := &catalog.Database{Tables: []catalog.Table{{Name: "items", Schema: "app"}}}
	var captured schemaext.FacetNormalizationRequest
	stop := errors.New("stop at validation")
	runtime := &facetProbeRuntime{SchemaRuntime: must.Must(builtin.New()), stop: stop, normalize: func(_ context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
		captured = request
		request.Desired.Records[0].Values = must.Must(schemaext.NewFacets(&pgpolicy.DesiredTableState{Enabled: true}))
		return schemaext.FacetNormalizationResult{Complete: true, Desired: request.Desired}, nil
	}}
	diff, err := schemadiff.CompareWithDatabase(t.Context(), conn, desired, current, nil, runtime)
	c.Assert(err, qt.ErrorIs, stop)
	c.Assert(diff, qt.IsNil)
	c.Assert(captured.Target, qt.Equals, "postgres")
	c.Assert(captured.Session, qt.Equals, conn)
	c.Assert(captured.Desired.Records, qt.HasLen, 1)
	c.Assert(captured.Desired.Records[0].Subject.Schema.Normalized, qt.Equals, "app")
	c.Assert(captured.Owners, qt.DeepEquals, []schemaext.ParentState{
		{Subject: objectidentity.NewBuilder(conn.Info().IdentifierSemantics).PrimaryKeyParts("", "items"), Desired: true},
		{Subject: objectidentity.NewBuilder(conn.Info().IdentifierSemantics).TableParts("", "items"), Desired: true, Current: true}})
	c.Assert(captured.Current.Records, qt.HasLen, 0)
	c.Assert(runtime.validated, qt.IsNotNil)
	normalized, found, err := schemaext.FacetAs[*pgpolicy.DesiredTableState](runtime.validated.Tables[0].Facets, pgpolicy.TableStateKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(normalized.Enabled, qt.IsTrue)
	original, _, err := schemaext.FacetAs[*pgpolicy.DesiredTableState](desired.Tables[0].Facets, pgpolicy.TableStateKind)
	c.Assert(err, qt.IsNil)
	c.Assert(original.Enabled, qt.IsFalse)
}

// TestLiveComparisonKeepsFeatureObjectsUnbound pins that facet normalization
// hands back the caller's feature objects and coverage. A policy written without a schema
// is bound to the connection's default schema for the owners' request, and
// the bound copy reached the comparison: the reverse migration then lowered a
// policy in app.items beside a table declared without a schema, and refused it
// as having no declared parent table.
func TestLiveComparisonKeepsFeatureObjectsUnbound(t *testing.T) {
	c := qt.New(t)
	desired := facetProbeSchema()
	desired.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(pgpolicy.DesiredPolicyObject(
		pgpolicy.PolicyRef("", "items", "tenant"), pgpolicy.DesiredPolicy{Command: pgpolicy.CommandAll}))))
	limited := must.Must(pgpolicy.Coverage(pgpolicy.PolicyKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{
		Kind: pgpolicy.PolicyKind, Subject: pgpolicy.PolicyRef("", "items", "legacy"), Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "left unmanaged"},
	}}))
	desired.FeatureCoverage = must.Must(limited.Combine(must.Must(pgpolicy.Coverage(pgpolicy.TableStateKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))))
	current := &catalog.Database{Tables: []catalog.Table{{Name: "items", Schema: "app"}}}
	stop := errors.New("stop at validation")
	runtime := &facetProbeRuntime{SchemaRuntime: must.Must(builtin.New()), stop: stop, normalize: func(_ context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
		return schemaext.FacetNormalizationResult{Complete: true, Desired: request.Desired}, nil
	}}
	diff, err := schemadiff.CompareWithDatabase(t.Context(), facetProbeConnection{schema: "app"}, desired, current, nil, runtime)
	c.Assert(err, qt.ErrorIs, stop)
	c.Assert(diff, qt.IsNil)
	c.Assert(runtime.validated, qt.IsNotNil)
	c.Assert(runtime.validated.FeatureObjects.Refs(), qt.DeepEquals, desired.FeatureObjects.Refs())
	c.Assert(runtime.validated.FeatureCoverage.Equal(desired.FeatureCoverage), qt.IsTrue)
}

func TestLiveComparisonRejectsIncompleteFacetNormalization(t *testing.T) {
	c := qt.New(t)
	runtime := &facetProbeRuntime{SchemaRuntime: must.Must(builtin.New()), normalize: func(_ context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
		return schemaext.FacetNormalizationResult{Desired: request.Desired}, nil
	}}
	diff, err := schemadiff.CompareWithDatabase(t.Context(), facetProbeConnection{schema: "public"}, facetProbeSchema(), &catalog.Database{}, nil, runtime)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(diff, qt.IsNil)
	c.Assert(runtime.validated, qt.IsNil)
}

func TestLiveComparisonPreservesFacetNormalizationFailure(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("facet probe failed")
	runtime := &facetProbeRuntime{SchemaRuntime: must.Must(builtin.New()), normalize: func(context.Context, schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
		return schemaext.FacetNormalizationResult{}, failure
	}}
	diff, err := schemadiff.CompareWithDatabase(t.Context(), facetProbeConnection{schema: "public"}, facetProbeSchema(), &catalog.Database{}, nil, runtime)
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(diff, qt.IsNil)
	c.Assert(runtime.validated, qt.IsNil)
}
