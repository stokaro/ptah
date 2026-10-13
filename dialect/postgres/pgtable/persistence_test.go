package pgtable_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
)

func persistenceSubject() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("postgres")).Table("app.cache")
}

func persistenceCoverage(representation schemaext.Representation, state schemaext.KnowledgeState) schemaext.Coverage {
	return must.Must(pgtable.PersistenceCoverage(representation, schemaext.Knowledge{State: state, Reason: "source knowledge"}, nil))
}

func TestPersistenceCodecs_HappyPath(t *testing.T) {
	for _, codec := range pgtable.PersistenceCodecs() {
		t.Run(string(codec.Representation), func(t *testing.T) {
			c := qt.New(t)
			value, err := codec.Decode(json.RawMessage(`{"unlogged":true}`))
			c.Assert(err, qt.IsNil)
			encoded, err := codec.Encode(value)
			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, `{"unlogged":true}`)
			copied, err := codec.Clone(value)
			c.Assert(err, qt.IsNil)
			c.Assert(copied, qt.DeepEquals, value)
			c.Assert(copied, qt.Not(qt.Equals), value)
		})
	}
}

func TestPersistenceCodecs_FailurePath(t *testing.T) {
	for _, codec := range pgtable.PersistenceCodecs() {
		for _, text := range []string{`null`, `{}`, `{"unlogged":null}`, `{"unlogged":"true"}`, `{"unlogged":true,"extra":1}`, `{"unlogged":true,"unlogged":true}`} {
			t.Run(string(codec.Representation)+text, func(t *testing.T) {
				c := qt.New(t)
				value, err := codec.Decode(json.RawMessage(text))
				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(value, qt.IsNil)
			})
		}
	}
}

func TestPersistenceChangeCodecAndReverse(t *testing.T) {
	for _, change := range []*pgtable.PersistenceChange{
		{After: &pgtable.DesiredPersistence{Unlogged: true}},
		{Before: &pgtable.ObservedPersistence{Unlogged: true}},
	} {
		c := qt.New(t)
		codec := pgtable.PersistenceChangeCodec()
		data, err := codec.Encode(change)
		c.Assert(err, qt.IsNil)
		got, err := codec.Decode(data)
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, change)
		inverse, err := change.Reverse()
		c.Assert(err, qt.IsNil)
		restored, err := inverse.Reverse()
		c.Assert(err, qt.IsNil)
		c.Assert(restored, qt.DeepEquals, change)
		c.Assert(restored, qt.Not(qt.Equals), change)
	}
}

func TestPersistenceChangeCodec_FailurePath(t *testing.T) {
	for _, text := range []string{`null`, `{}`, `{"before":null,"after":null}`, `{"after":{"unlogged":true}}`, `{"before":null,"after":{"unlogged":false}}`, `{"before":{"unlogged":true},"after":{"unlogged":true}}`} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			value, err := pgtable.PersistenceChangeCodec().Decode(json.RawMessage(text))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

func TestPersistenceConversionAndReversal(t *testing.T) {
	c := qt.New(t)
	service := pgtable.PersistenceService{}
	declared := &pgtable.DesiredPersistence{Unlogged: true}
	observed, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{declared}})
	c.Assert(err, qt.IsNil)
	restored, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "postgres", From: schemaext.Observed, To: schemaext.Desired, Values: observed})
	c.Assert(err, qt.IsNil)
	c.Assert(restored, qt.DeepEquals, []schemaext.Value{declared})
	restored[0].(*pgtable.DesiredPersistence).Unlogged = false
	c.Assert(declared.Unlogged, qt.IsTrue)
	reversed, err := service.ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "postgres", Changes: []schemaext.ChangeRecord{{Subject: persistenceSubject(), Value: &pgtable.PersistenceChange{After: declared}}}})
	c.Assert(err, qt.IsNil)
	c.Assert(reversed, qt.HasLen, 1)
	c.Assert(reversed[0].Change.Value, qt.DeepEquals, &pgtable.PersistenceChange{Before: &pgtable.ObservedPersistence{Unlogged: true}})
	c.Assert(reversed[0].ForwardState[0].Value, qt.DeepEquals, &pgtable.ObservedPersistence{Unlogged: true})
}

func TestPersistenceService_RefusesUnsupportedTargets(t *testing.T) {
	for _, target := range []string{"", "unknown"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgtable.SupportsPersistence(target), qt.IsFalse)
			service := pgtable.PersistenceService{}
			result, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: target, From: schemaext.Desired, To: schemaext.Observed})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
			c.Assert(result, qt.IsNil)
			comparison, err := service.CompareFacets(t.Context(), schemaext.FacetComparisonRequest{Target: target})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
			c.Assert(comparison, qt.DeepEquals, schemaext.FacetComparisonResult{})
			reverse, err := service.ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: target})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
			c.Assert(reverse, qt.IsNil)
		})
	}
}

func TestPersistenceService_CanceledBatch(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	service := pgtable.PersistenceService{}
	values, err := service.ConvertFeatures(ctx, schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(values, qt.IsNil)
	comparison, err := service.CompareFacets(ctx, schemaext.FacetComparisonRequest{Target: "postgres"})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(comparison, qt.DeepEquals, schemaext.FacetComparisonResult{})
	reverse, err := service.ReverseChanges(ctx, schemaext.ReversalRequest{Target: "postgres"})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(reverse, qt.IsNil)
}

func TestPersistenceComparison_Knowledge(t *testing.T) {
	for _, test := range []struct {
		name      string
		desired   bool
		current   bool
		state     schemaext.KnowledgeState
		changes   int
		undecided int
	}{
		{"logged to unlogged", true, false, schemaext.Complete, 1, 0},
		{"unlogged to logged", false, true, schemaext.Complete, 1, 0},
		{"both logged", false, false, schemaext.Complete, 0, 0},
		{"both unlogged", true, true, schemaext.Complete, 0, 0},
		{"uninspected", true, false, schemaext.Uninspected, 0, 1},
		{"unrepresentable", false, true, schemaext.Unrepresentable, 0, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			subject := persistenceSubject()
			desired := must.Must(pgtable.WithPersistence(schemaext.Facets{}, pgtable.Persistence{Unlogged: test.desired}))
			current := must.Must(pgtable.WithObservedPersistence(schemaext.Facets{}, pgtable.Persistence{Unlogged: test.current}))
			request := schemaext.FacetComparisonRequest{Target: "postgres", Kinds: []schemaext.Kind{pgtable.PersistenceKind},
				Owners:  []schemaext.ParentState{{Subject: subject, Desired: true, Current: true}},
				Desired: schemaext.FacetState{Coverage: persistenceCoverage(schemaext.Desired, schemaext.Complete), Records: []schemaext.FacetRecord{{Subject: subject, Values: desired}}},
				Current: schemaext.FacetState{Coverage: persistenceCoverage(schemaext.Observed, test.state), Records: []schemaext.FacetRecord{{Subject: subject, Values: current}}},
			}
			result, err := (pgtable.PersistenceService{}).CompareFacets(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
		})
	}
}

func TestPersistenceCodecs_ExplicitLoggedDeclaration(t *testing.T) {
	for _, codec := range pgtable.PersistenceCodecs() {
		t.Run(string(codec.Representation), func(t *testing.T) {
			c := qt.New(t)
			value, err := codec.Decode(json.RawMessage(`{"unlogged":false}`))
			c.Assert(err, qt.IsNil)
			encoded, err := codec.Encode(value)
			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, `{"unlogged":false}`)
		})
	}
}

func TestPersistenceConversion_DefinedTargets(t *testing.T) {
	for _, target := range pgtable.PersistenceTargets() {
		for _, unlogged := range []bool{true, false} {
			t.Run(target+strconv.FormatBool(unlogged), func(t *testing.T) {
				c := qt.New(t)
				service := pgtable.PersistenceService{}
				input := &pgtable.DesiredPersistence{Unlogged: unlogged}
				observed, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: target, From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{input}})
				c.Assert(err, qt.IsNil)
				c.Assert(observed, qt.DeepEquals, []schemaext.Value{&pgtable.ObservedPersistence{Unlogged: unlogged}})
				restored, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: target, From: schemaext.Observed, To: schemaext.Desired, Values: observed})
				c.Assert(err, qt.IsNil)
				c.Assert(restored, qt.DeepEquals, []schemaext.Value{input})
				restored[0].(*pgtable.DesiredPersistence).Unlogged = !unlogged
				c.Assert(input.Unlogged, qt.Equals, unlogged)
			})
		}
	}
}

func TestPersistenceReversal_RefusesForeignTargets(t *testing.T) {
	for _, target := range []string{"mysql", "mariadb", "sqlite", "sqlserver", "ydb", "clickhouse", "oracle", "cockroachdb", "yugabytedb", "spanner"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			reverse, err := (pgtable.PersistenceService{}).ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: target,
				Changes: []schemaext.ChangeRecord{{Subject: persistenceSubject(), Value: &pgtable.PersistenceChange{After: &pgtable.DesiredPersistence{Unlogged: true}}}}})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
			c.Assert(reverse, qt.IsNil)
		})
	}
}

func TestPersistenceComparison_MissingCoverage(t *testing.T) {
	for _, row := range []struct {
		name                string
		desired, current    schemaext.Facets
		declared, inspected bool
		changes, undecided  int
	}{
		{name: "unmanaged source", current: must.Must(schemaext.NewFacets(&pgtable.ObservedPersistence{Unlogged: true})), inspected: true},
		{name: "unlogged against unknown", desired: must.Must(schemaext.NewFacets(&pgtable.DesiredPersistence{Unlogged: true})), undecided: 1},
		{name: "logged default against unknown", declared: true, undecided: 1},
		{name: "explicit logged against observed", desired: must.Must(schemaext.NewFacets(&pgtable.DesiredPersistence{})), current: must.Must(schemaext.NewFacets(&pgtable.ObservedPersistence{Unlogged: true})), changes: 1},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			subject := persistenceSubject()
			declared := map[bool]schemaext.Coverage{true: persistenceCoverage(schemaext.Desired, schemaext.Complete)}[row.declared]
			inspected := map[bool]schemaext.Coverage{true: persistenceCoverage(schemaext.Observed, schemaext.Complete)}[row.inspected]
			result, err := (pgtable.PersistenceService{}).CompareFacets(t.Context(), schemaext.FacetComparisonRequest{Target: "postgres", Kinds: []schemaext.Kind{pgtable.PersistenceKind},
				Owners:  []schemaext.ParentState{{Subject: subject, Desired: true, Current: true}},
				Desired: schemaext.FacetState{Coverage: declared, Records: []schemaext.FacetRecord{{Subject: subject, Values: row.desired}}},
				Current: schemaext.FacetState{Coverage: inspected, Records: []schemaext.FacetRecord{{Subject: subject, Values: row.current}}},
			})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, row.changes)
			c.Assert(result.Undecided, qt.HasLen, row.undecided)
		})
	}
}
