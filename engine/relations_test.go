package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

type linkedValue struct {
	Model   schemaext.Kind
	Targets []objectidentity.ID
	Text    string
}

func (v *linkedValue) Kind() schemaext.Kind { return v.Model }
func (v *linkedValue) Clone() schemaext.Value {
	return &linkedValue{Model: v.Model, Targets: slices.Clone(v.Targets), Text: v.Text}
}
func (v *linkedValue) Equal(other schemaext.Value) bool {
	w, ok := other.(*linkedValue)
	return ok && w != nil && v.Model == w.Model && v.Text == w.Text && slices.Equal(v.Targets, w.Targets)
}

func linkedCodec(kind schemaext.Kind, representation schemaext.Representation) schemaext.Codec {
	encode := func(value schemaext.Payload) (json.RawMessage, error) { return json.Marshal(value) }
	return schemaext.Codec{Prototype: &linkedValue{Model: kind}, Representation: representation, Version: 1,
		Definition: json.RawMessage(`{"Model":"kind","Targets":"ordered reference list","Text":"string"}`),
		Clone:      func(value schemaext.Payload) (schemaext.Payload, error) { return value.(*linkedValue).Clone(), nil },
		Encode:     encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) { return schemaext.DecodeJSON[*linkedValue](data) },
	}
}

type relationsFunc func(context.Context, schemaext.RelationRequest) (schemaext.RelationResult, error)

func (f relationsFunc) DescribeRelations(ctx context.Context, request schemaext.RelationRequest) (schemaext.RelationResult, error) {
	return f(ctx, request)
}

func relationReply(request schemaext.RelationRequest) schemaext.RelationResult {
	result := schemaext.RelationResult{Complete: true, Values: make([]schemaext.ValueRelations, len(request.Values))}
	for i, value := range request.Values {
		result.Values[i] = schemaext.ValueRelations{Subject: value.Subject, Complete: true,
			Dependencies: slices.Clone(value.Value.(*linkedValue).Targets)}
	}
	return result
}

func relationProvider(service schemaext.RelationService) engine.Provider {
	return engine.Provider{ID: "example.org/relations", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}}},
		Codecs: []schemaext.Codec{linkedCodec(conversionFirst, schemaext.Desired), linkedCodec(conversionSecond, schemaext.Desired),
			linkedCodec(conversionFirst, schemaext.Observed), linkedCodec(conversionSecond, schemaext.Observed)},
		Relations: []engine.RelationDiscovery{{Target: "custom", Representation: schemaext.Desired,
			Kinds: []schemaext.Kind{conversionFirst, conversionSecond}, Service: service}},
	}
}

func relationObject(kind schemaext.Kind, name string, targets ...objectidentity.ID) schemaext.RelationValue {
	b := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	return schemaext.RelationValue{Subject: schemaext.RelationSubject{Kind: kind, Placement: schemaext.ObjectPlacement,
		Subject: b.SchemaScopedParts(objectidentity.Kind(kind), "security", name)},
		Value: &linkedValue{Model: kind, Targets: targets, Text: "complete declaration"}}
}

func relationRequest(runtime *engine.Runtime, values ...schemaext.RelationValue) schemaext.RelationRequest {
	return schemaext.RelationRequest{Target: " ALTERNATE ", Representation: schemaext.Desired,
		Identifiers: identifier.ForDialect("postgres"), Values: values,
		Coverage: facetCoverage(runtime.Codecs(), schemaext.Desired, schemaext.Complete)}
}

func TestRelationsCaptureKeepsMultiTableObjectsAndSharedDependencies(t *testing.T) {
	c := qt.New(t)
	b := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	first, second, unrelated := b.Table("public.first"), b.Table("public.second"), b.Table("public.unrelated")
	function := b.Function("security.permitted", "integer")
	var received schemaext.RelationRequest
	var reply schemaext.RelationResult
	calls := 0
	provider := relationProvider(relationsFunc(func(_ context.Context, request schemaext.RelationRequest) (schemaext.RelationResult, error) {
		calls++
		received = request
		reply = relationReply(request)
		request.Values[0].Value.(*linkedValue).Text = "service mutation"
		return reply, nil
	}))
	runtime := mustRuntime(c, provider)
	provider.Relations[0].Kinds[0] = "example.org/mutated"
	provider.Relations[0].Service = nil
	values := []schemaext.RelationValue{
		relationObject(conversionFirst, "pair", first, second, function),
		relationObject(conversionSecond, "shared", second),
		relationObject(conversionFirst, "separate", unrelated),
	}
	request := relationRequest(runtime, values...)
	snapshot, err := runtime.CaptureRelations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(received.Target, qt.Equals, "custom")
	c.Assert(received.Kinds, qt.DeepEquals, []schemaext.Kind{conversionFirst, conversionSecond})
	c.Assert(received.Values, qt.HasLen, 3)
	c.Assert(snapshot.Coverage(), qt.DeepEquals, request.Coverage)
	values[0].Value.(*linkedValue).Targets[0] = unrelated
	received.Values[0].Value.(*linkedValue).Targets[1] = unrelated
	reply.Values[0].Dependencies[0] = unrelated
	related, err := snapshot.CaptureRelated(t.Context(), []objectidentity.ID{first})
	c.Assert(err, qt.IsNil)
	c.Assert(related.Values, qt.HasLen, 2)
	c.Assert(related.Values[0].Value.(*linkedValue).Targets, qt.DeepEquals, []objectidentity.ID{first, second, function})
	c.Assert(related.Values[0].Value.(*linkedValue).Text, qt.Equals, "complete declaration")
	c.Assert(related.Values[1].Subject.Subject, qt.Equals, values[1].Subject.Subject)
	c.Assert(related.Required, qt.ContentEquals, []objectidentity.ID{first, second, function})
	related.Values[0].Value.(*linkedValue).Targets[0] = unrelated
	again, err := snapshot.CaptureRelated(t.Context(), []objectidentity.ID{function})
	c.Assert(err, qt.IsNil)
	c.Assert(again.Values[0].Value.(*linkedValue).Targets[0], qt.Equals, first)
	c.Assert(again.Values, qt.HasLen, 2)
}

func TestRelationsCaptureIsolatesTargetFactsBetweenOwners(t *testing.T) {
	c := qt.New(t)
	var received schemaext.RelationRequest
	provider := relationProvider(relationsFunc(func(_ context.Context, request schemaext.RelationRequest) (schemaext.RelationResult, error) {
		request.Capabilities[capability.CreateIndexConcurrently] = false
		request.Identifiers.ResolvedNames[0].Key = "mutated"
		return relationReply(request), nil
	}))
	provider.Relations[0].Kinds = []schemaext.Kind{conversionFirst}
	provider.Relations = append(provider.Relations, engine.RelationDiscovery{
		Target: "custom", Representation: schemaext.Desired, Kinds: []schemaext.Kind{conversionSecond},
		Service: relationsFunc(func(_ context.Context, request schemaext.RelationRequest) (schemaext.RelationResult, error) {
			received = request
			return relationReply(request), nil
		}),
	})
	runtime := mustRuntime(c, provider)
	request := relationRequest(runtime)
	request.Capabilities = capability.Capabilities{capability.CreateIndexConcurrently: true}
	request.Identifiers = identifier.ForSQLServerCatalog("Latin1_General_CI_AS").WithResolvedNames([]identifier.ResolvedName{{Name: "orders", Key: "orders"}})
	snapshot, err := runtime.CaptureRelations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(snapshot.Records(), qt.HasLen, 0)
	c.Assert(request.Capabilities.Has(capability.CreateIndexConcurrently), qt.IsTrue)
	c.Assert(received.Capabilities.Has(capability.CreateIndexConcurrently), qt.IsTrue)
	c.Assert(request.Identifiers.ResolvedNames, qt.DeepEquals, []identifier.ResolvedName{{Name: "orders", Key: "orders"}})
	c.Assert(received.Identifiers.ResolvedNames, qt.DeepEquals, request.Identifiers.ResolvedNames)
}

func TestRelationsCaptureDerivesExclusiveOwnershipAndColumnParents(t *testing.T) {
	c := qt.New(t)
	b := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	table := b.Table("public.orders")
	child := relationObject(conversionFirst, "child")
	child.Subject.Subject.Schema, child.Subject.Subject.Parent = table.Schema, table.Name
	facet := schemaext.RelationValue{Subject: schemaext.RelationSubject{
		Kind: conversionSecond, Placement: schemaext.FacetPlacement, Subject: table}, Value: &linkedValue{Model: conversionSecond}}
	columnPolicy := relationObject(conversionFirst, "column", b.Column("public.orders", "tenant"))
	runtime := mustRuntime(c, relationProvider(relationsFunc(func(_ context.Context, r schemaext.RelationRequest) (schemaext.RelationResult, error) {
		return relationReply(r), nil
	})))
	snapshot, err := runtime.CaptureRelations(t.Context(), relationRequest(runtime, child, facet, columnPolicy))
	c.Assert(err, qt.IsNil)
	related, err := snapshot.CaptureRelated(t.Context(), []objectidentity.ID{table})
	c.Assert(err, qt.IsNil)
	c.Assert(related.Values, qt.DeepEquals, []schemaext.RelationValue{child, facet, columnPolicy})
	c.Assert(related.Required, qt.ContentEquals, []objectidentity.ID{table, b.Column("public.orders", "tenant")})
}

func TestRelationsCaptureDoesNotCollideQualifiedComponents(t *testing.T) {
	c := qt.New(t)
	b := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	first, second := b.TableParts(`"a.b"`, "c"), b.TableParts("a", `"b.c"`)
	runtime := mustRuntime(c, relationProvider(relationsFunc(func(_ context.Context, r schemaext.RelationRequest) (schemaext.RelationResult, error) {
		return relationReply(r), nil
	})))
	one, two := relationObject(conversionFirst, "one", first), relationObject(conversionFirst, "two", second)
	snapshot, err := runtime.CaptureRelations(t.Context(), relationRequest(runtime, one, two))
	c.Assert(err, qt.IsNil)
	related, err := snapshot.CaptureRelated(t.Context(), []objectidentity.ID{first})
	c.Assert(err, qt.IsNil)
	c.Assert(related.Values, qt.DeepEquals, []schemaext.RelationValue{one})
}

func TestRelationsRefuseIncompleteContextWithoutLosingKnownDefinitions(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*schemaext.RelationRequest)
		reply   func(*schemaext.RelationResult)
	}{
		{name: "unenrolled namespace", prepare: func(r *schemaext.RelationRequest) { r.Coverage = schemaext.Coverage{} }, reply: func(*schemaext.RelationResult) {}},
		{name: "namespace incompletely enumerated", prepare: func(r *schemaext.RelationRequest) {
			kinds := r.Coverage.KindRecords()
			kinds[0].Knowledge = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "permission-limited source"}
			r.Coverage, _ = schemaext.NewCoverage(r.Representation, kinds, nil)
		}, reply: func(*schemaext.RelationResult) {}},
		{name: "unknown subject", prepare: func(r *schemaext.RelationRequest) {
			r.Coverage, _ = schemaext.NewCoverage(r.Representation, r.Coverage.KindRecords(), []schemaext.SubjectCoverage{{
				Kind: conversionFirst, Subject: comparedRef(conversionFirst, "hidden"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "hidden bindings"}}})
		}, reply: func(*schemaext.RelationResult) {}},
		{name: "unresolved expression", prepare: func(*schemaext.RelationRequest) {}, reply: func(r *schemaext.RelationResult) {
			r.Values[0].Complete = false
			r.Values[0].Reason = "function reference is ambiguous"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, relationProvider(relationsFunc(func(_ context.Context, r schemaext.RelationRequest) (schemaext.RelationResult, error) {
				reply := relationReply(r)
				test.reply(&reply)
				return reply, nil
			})))
			request := relationRequest(runtime, relationObject(conversionFirst, "known"))
			test.prepare(&request)
			snapshot, err := runtime.CaptureRelations(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(snapshot.Records(), qt.HasLen, 1)
			c.Assert(snapshot.Coverage(), qt.DeepEquals, request.Coverage)
			related, err := snapshot.CaptureRelated(t.Context(), []objectidentity.ID{request.Values[0].Subject.Subject})
			c.Assert(err, qt.ErrorIs, schemaext.ErrIncompleteRelations)
			c.Assert(related, qt.DeepEquals, schemaext.RelatedFeatures{})
		})
	}
}

func TestRelationsRefuseMalformedReplies(t *testing.T) {
	for _, test := range []struct {
		name   string
		adjust func(*schemaext.RelationResult)
	}{
		{name: "incomplete batch", adjust: func(r *schemaext.RelationResult) { r.Complete = false }},
		{name: "missing receipt", adjust: func(r *schemaext.RelationResult) { r.Values = nil }},
		{name: "extra receipt", adjust: func(r *schemaext.RelationResult) { r.Values = append(r.Values, r.Values[0]) }},
		{name: "changed identity", adjust: func(r *schemaext.RelationResult) { r.Values[0].Subject.Subject.Name.Source = "other" }},
		{name: "changed placement", adjust: func(r *schemaext.RelationResult) { r.Values[0].Subject.Placement = schemaext.FacetPlacement }},
		{name: "unknown without reason", adjust: func(r *schemaext.RelationResult) { r.Values[0].Complete = false }},
		{name: "complete with unresolved reason", adjust: func(r *schemaext.RelationResult) { r.Values[0].Reason = "unknown" }},
		{name: "invalid reference", adjust: func(r *schemaext.RelationResult) { r.Values[0].Dependencies = []objectidentity.ID{{}} }},
		{name: "duplicate dependency", adjust: func(r *schemaext.RelationResult) {
			r.Values[0].Dependencies = append(r.Values[0].Dependencies, r.Values[0].Dependencies[0])
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, relationProvider(relationsFunc(func(_ context.Context, r schemaext.RelationRequest) (schemaext.RelationResult, error) {
				reply := relationReply(r)
				test.adjust(&reply)
				return reply, nil
			})))
			value := relationObject(conversionFirst, "policy", objectidentity.NewBuilder(identifier.ForDialect("postgres")).Table("orders"))
			snapshot, err := runtime.CaptureRelations(t.Context(), relationRequest(runtime, value))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(snapshot.Records(), qt.HasLen, 0)
		})
	}
}

func TestRelationsPreflightMissingOwnersBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	calls := 0
	provider := relationProvider(relationsFunc(func(_ context.Context, r schemaext.RelationRequest) (schemaext.RelationResult, error) {
		calls++
		return relationReply(r), nil
	}))
	provider.Relations[0].Kinds = []schemaext.Kind{conversionFirst}
	runtime := mustRuntime(c, provider)
	snapshot, err := runtime.CaptureRelations(t.Context(), relationRequest(runtime, relationObject(conversionFirst, "known"), relationObject(conversionSecond, "unassigned")))
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(calls, qt.Equals, 0)
	c.Assert(snapshot.Records(), qt.HasLen, 0)
}

func TestRelationsCancellationAndFailureDiscardAllEarlierResults(t *testing.T) {
	failure := errors.New("provider disconnected")
	for _, test := range []struct {
		name string
		want error
		fail func(context.CancelFunc, schemaext.RelationRequest) (schemaext.RelationResult, error)
	}{
		{name: "service failure", want: failure, fail: func(_ context.CancelFunc, r schemaext.RelationRequest) (schemaext.RelationResult, error) {
			return relationReply(r), failure
		}},
		{name: "cancellation", want: context.Canceled, fail: func(cancel context.CancelFunc, r schemaext.RelationRequest) (schemaext.RelationResult, error) {
			cancel()
			return relationReply(r), nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			provider := relationProvider(relationsFunc(func(_ context.Context, r schemaext.RelationRequest) (schemaext.RelationResult, error) {
				calls++
				return relationReply(r), nil
			}))
			provider.Relations[0].Kinds = []schemaext.Kind{conversionFirst}
			provider.Relations = append(provider.Relations, engine.RelationDiscovery{Target: "custom", Representation: schemaext.Desired,
				Kinds: []schemaext.Kind{conversionSecond}, Service: relationsFunc(func(_ context.Context, r schemaext.RelationRequest) (schemaext.RelationResult, error) {
					calls++
					return test.fail(cancel, r)
				})})
			runtime := mustRuntime(c, provider)
			snapshot, err := runtime.CaptureRelations(ctx, relationRequest(runtime, relationObject(conversionFirst, "first"), relationObject(conversionSecond, "second")))
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(calls, qt.Equals, 2)
			c.Assert(snapshot.Records(), qt.HasLen, 0)
		})
	}
}
