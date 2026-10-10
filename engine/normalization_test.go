package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

type normalizationFunc func(context.Context, schemaext.NormalizationRequest) (schemaext.NormalizationResult, error)

func (f normalizationFunc) NormalizeObjects(ctx context.Context, request schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
	return f(ctx, request)
}

// probeSession is a session the runtime must hand through untouched.
type probeSession struct{ schemaext.ProbeSession }

func normalizationProvider(service schemaext.NormalizationService) engine.Provider {
	provider := conversionProvider(nil)
	provider.Conversions = nil
	provider.Normalizations = []engine.Normalization{{Target: "custom", Kinds: []schemaext.Kind{conversionFirst}, Service: service}}
	return provider
}

func normalizationRef(kind schemaext.Kind, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts(objectidentity.Kind(kind), "app", name)
}

func normalizationRequest(c *qt.C) schemaext.NormalizationRequest {
	c.Helper()
	return schemaext.NormalizationRequest{Target: "alternate", Session: &probeSession{},
		Desired: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(
			schemaext.Object{Ref: normalizationRef(conversionFirst, "a"), Value: &conversionValue{ID: conversionFirst, Number: 3}},
			schemaext.Object{Ref: normalizationRef(conversionSecond, "b"), Value: &conversionValue{ID: conversionSecond, Number: 5}},
		))},
	}
}

// TestNormalizeObjects_DispatchesOnlyTheKindsAnOwnerNormalizes pins the
// dispatch: an owner receives the declared objects of its kinds and the
// session it probes with, its reply replaces those objects, and a kind no
// owner normalizes passes through unchanged rather than being refused.
func TestNormalizeObjects_DispatchesOnlyTheKindsAnOwnerNormalizes(t *testing.T) {
	c := qt.New(t)
	var received schemaext.NormalizationRequest
	runtime := mustRuntime(c, normalizationProvider(normalizationFunc(func(_ context.Context, request schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
		received = request
		objects := must.Must(request.Desired.Objects.All())
		replaced := request.Desired
		for _, object := range objects {
			replaced.Objects = must.Must(replaced.Objects.Replace(schemaext.Object{Ref: object.Ref,
				Value: &conversionValue{ID: conversionFirst, Number: object.Value.(*conversionValue).Number * 10}}))
		}
		return schemaext.NormalizationResult{Complete: true, Desired: replaced}, nil
	})))
	request := normalizationRequest(c)

	result, err := runtime.NormalizeObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(received.Target, qt.Equals, "custom")
	c.Assert(received.Session, qt.Equals, request.Session)
	c.Assert(received.Desired.Objects.Refs(), qt.DeepEquals, []objectidentity.ID{normalizationRef(conversionFirst, "a")})
	c.Assert(must.Must(result.Desired.Objects.All()), qt.DeepEquals, []schemaext.Object{
		{Ref: normalizationRef(conversionFirst, "a"), Value: &conversionValue{ID: conversionFirst, Number: 30}},
		{Ref: normalizationRef(conversionSecond, "b"), Value: &conversionValue{ID: conversionSecond, Number: 5}},
	})
	c.Assert(must.Must(request.Desired.Objects.All())[0].Value.(*conversionValue).Number, qt.Equals, 3)
}

// TestNormalizeObjects_RefusesAReplyThatIsNotTheDeclaration pins what a
// reply may not do: leave the batch incomplete, drop or add an object, change
// a kind, or fail -- and that no partial result survives any of them.
func TestNormalizeObjects_RefusesAReplyThatIsNotTheDeclaration(t *testing.T) {
	failure := errors.New("probe failed")
	tests := []struct {
		name  string
		reply func(schemaext.NormalizationRequest) (schemaext.NormalizationResult, error)
		want  error
	}{
		{name: "incomplete", want: schemaext.ErrInvalidValue, reply: func(request schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
			return schemaext.NormalizationResult{Desired: request.Desired}, nil
		}},
		{name: "an object dropped", want: schemaext.ErrInvalidValue, reply: func(schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
			return schemaext.NormalizationResult{Complete: true}, nil
		}},
		{name: "a failed probe", want: failure, reply: func(schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
			return schemaext.NormalizationResult{}, failure
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, normalizationProvider(normalizationFunc(func(_ context.Context, request schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
				return test.reply(request)
			})))

			result, err := runtime.NormalizeObjects(t.Context(), normalizationRequest(c))

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result.Complete, qt.IsFalse)
			c.Assert(result.Desired.Objects.Len(), qt.Equals, 0)
		})
	}
}

// TestNormalizeObjects_RefusesAnUnknownTarget pins that a target no provider
// declares is refused before any owner is asked.
func TestNormalizeObjects_RefusesAnUnknownTarget(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, normalizationProvider(normalizationFunc(func(context.Context, schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
		c.Fatalf("an owner was asked for an unknown target")
		return schemaext.NormalizationResult{}, nil
	})))
	request := normalizationRequest(c)
	request.Target = "elsewhere"

	result, err := runtime.NormalizeObjects(t.Context(), request)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(result.Complete, qt.IsFalse)
}

// TestNew_RefusesAnIncompleteNormalization pins the registration rules: an
// owner normalizes only kinds whose desired codec it owns, and one target and
// kind has one normalizer.
func TestNew_RefusesAnIncompleteNormalization(t *testing.T) {
	service := normalizationFunc(func(context.Context, schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
		return schemaext.NormalizationResult{}, nil
	})
	tests := []struct {
		name           string
		normalizations []engine.Normalization
	}{
		{name: "a kind the owner has no codec for", normalizations: []engine.Normalization{{Target: "custom", Kinds: []schemaext.Kind{"example.org/other"}, Service: service}}},
		{name: "no kinds", normalizations: []engine.Normalization{{Target: "custom", Service: service}}},
		{name: "no service", normalizations: []engine.Normalization{{Target: "custom", Kinds: []schemaext.Kind{conversionFirst}}}},
		{name: "a target nobody declares", normalizations: []engine.Normalization{{Target: "elsewhere", Kinds: []schemaext.Kind{conversionFirst}, Service: service}}},
		{name: "a kind normalized twice", normalizations: []engine.Normalization{
			{Target: "custom", Kinds: []schemaext.Kind{conversionFirst}, Service: service},
			{Target: "custom", Kinds: []schemaext.Kind{conversionFirst}, Service: service},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := normalizationProvider(service)
			provider.Normalizations = test.normalizations

			runtime, err := engine.New(provider)

			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

// TestNormalizeObjects_LeavesUnownedKindsAsTheyCame pins that a kind no owner
// normalizes is neither sent anywhere nor snapshotted: a comparison against a
// target with no normalizer, or a declaration holding only other kinds, costs
// no codec work and returns the declaration it was given.
func TestNormalizeObjects_LeavesUnownedKindsAsTheyCame(t *testing.T) {
	tests := []struct {
		name           string
		normalizations []engine.Normalization
	}{
		{name: "a target with no normalizer"},
		{name: "a normalizer of another kind", normalizations: []engine.Normalization{{Target: "custom", Kinds: []schemaext.Kind{conversionFirst},
			Service: normalizationFunc(func(context.Context, schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
				return schemaext.NormalizationResult{}, errors.New("an owner was asked about a kind it does not normalize")
			})}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			clones := 0
			provider := normalizationProvider(nil)
			provider.Normalizations = test.normalizations
			for i := range provider.Codecs {
				clone := provider.Codecs[i].Clone
				provider.Codecs[i].Clone = func(value schemaext.Payload) (schemaext.Payload, error) {
					clones++
					return clone(value)
				}
			}
			runtime := mustRuntime(c, provider)
			request := normalizationRequest(c)
			request.Desired.Objects = request.Desired.Objects.Select(func(ref objectidentity.ID) bool { return schemaext.Kind(ref.Kind) == conversionSecond })
			clones = 0

			result, err := runtime.NormalizeObjects(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(must.Must(result.Desired.Objects.All()), qt.DeepEquals, must.Must(request.Desired.Objects.All()))
			c.Assert(clones, qt.Equals, 0)
		})
	}
}
