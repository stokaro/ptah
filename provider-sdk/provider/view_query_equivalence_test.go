package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// viewQueryExchange asks whether the declared queries of view active and
// materialized view totals are the ones the database holds, and answers that
// active's, which differs in the case of a column name alone, is not, and
// totals' is.
func viewQueryExchange() (*protocol.Description, *protocol.CompareViewQueriesRequest, *protocol.ComparedViewQueries) {
	d := commonDescription()
	d.Features = append(d.Features, provider.ViewQueryEquivalenceFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ViewQueryEquivalenceFeature)
	request := &protocol.CompareViewQueriesRequest{Engine: "fixture", Identifiers: commonIdentifiers(), Capabilities: map[string]bool{"views": true},
		Pairs: []*protocol.ViewQueryPair{
			{Current: &protocol.ObservedView{Identity: commonRef("view", "app", "active"), Body: "SELECT Id FROM users"}, Declared: "SELECT id FROM users"},
			{Current: &protocol.ObservedView{Identity: commonRef("materialized_view", "app", "totals"), Body: "SELECT count(*) FROM users"},
				Declared: "SELECT count(*) FROM users"},
		}}
	result := &protocol.ComparedViewQueries{Complete: true, Queries: []*protocol.ComparedViewQuery{
		{Identity: commonRef("view", "app", "active")},
		{Identity: commonRef("materialized_view", "app", "totals"), Equivalent: true},
	}}
	return d, request, result
}

func TestViewQueryEquivalence_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("a reply answers each pair in order and survives the wire", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := viewQueryExchange()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		before := proto.CloneOf(request)
		c.Assert(provider.ValidateComparedViewQueries(d, request, result), qt.IsNil)
		c.Assert(proto.Equal(before, request), qt.IsTrue)
		data, err := proto.Marshal(result)
		c.Assert(err, qt.IsNil)
		decoded := &protocol.ComparedViewQueries{}
		c.Assert(proto.Unmarshal(data, decoded), qt.IsNil)
		c.Assert(provider.ValidateComparedViewQueries(d, request, decoded), qt.IsNil)
		c.Assert(decoded.Queries[0].GetEquivalent(), qt.IsFalse)
		c.Assert(decoded.Queries[1].GetEquivalent(), qt.IsTrue)
	})
	t.Run("a request with no pair has an empty answer", func(t *testing.T) {
		c := qt.New(t)
		d, request, _ := viewQueryExchange()
		request.Pairs = nil
		c.Assert(provider.ValidateComparedViewQueries(d, request, &protocol.ComparedViewQueries{Complete: true}), qt.IsNil)
	})
	t.Run("the display name of an answer is presentation", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := viewQueryExchange()
		result.Queries[0].Identity.DisplayName = "app.active"
		c.Assert(provider.ValidateComparedViewQueries(d, request, result), qt.IsNil)
	})
	t.Run("views of two databases are two views", func(t *testing.T) {
		c := qt.New(t)
		d, request, _ := viewQueryExchange()
		second := proto.CloneOf(request.Pairs[0])
		second.Current.Identity.Database = "other"
		request.Pairs = append(request.Pairs, second)
		c.Assert(provider.ValidateCompareViewQueriesRequest(d, request), qt.IsNil)
	})
	// The RPC is new and adds nothing to an existing message, so a host that
	// lists no feature still sees the engine compare view queries, and calls
	// it only if it knows the call.
	t.Run("the feature is the provider's alone", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := viewQueryExchange()
		c.Assert(provider.TwoWayFeatures(), qt.Not(qt.Contains), provider.ViewQueryEquivalenceFeature)
		negotiated := provider.NegotiatedDescription(d, nil)
		c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.ViewQueryEquivalenceFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
}

// A request names an engine that compares view queries, and each pair holds
// a view of a kind the engine carries, as read, with a declared query beside
// it.
func TestCompareViewQueriesRequest_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.CompareViewQueriesRequest)
	}{
		{"the provider does not compare view queries", `.*view query equivalence: the provider does not compare view queries`,
			func(d *protocol.Description, _ *protocol.CompareViewQueriesRequest) {
				d.Features = withoutFeature(d.Features, provider.ViewQueryEquivalenceFeature)
			}},
		{"the engine does not compare view queries", `.*view query equivalence: the engine does not compare view queries`,
			func(d *protocol.Description, _ *protocol.CompareViewQueriesRequest) {
				d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.ViewQueryEquivalenceFeature)
			}},
		{"another engine", `.*value engine is not owned by this provider`, func(_ *protocol.Description, r *protocol.CompareViewQueriesRequest) { r.Engine = "other" }},
		{"no identifier facts", `.*identifier rules are required`, func(_ *protocol.Description, r *protocol.CompareViewQueriesRequest) { r.Identifiers = nil }},
		{"an empty capability", `.*empty comparison capability`, func(_ *protocol.Description, r *protocol.CompareViewQueriesRequest) {
			r.Capabilities[" "] = true
		}},
		{"no held view", `.*view query equivalence: pair 0 requires its held view`, func(_ *protocol.Description, r *protocol.CompareViewQueriesRequest) {
			r.Pairs[0].Current = nil
		}},
		{"a held view of another engine", `.*view query equivalence: pair 0 holds no view or materialized view of the engine`,
			func(_ *protocol.Description, r *protocol.CompareViewQueriesRequest) {
				r.Pairs[0].Current.Identity.Engine = "other"
			}},
		{"a held table", `.*view query equivalence: pair 0 holds no view or materialized view of the engine`,
			func(_ *protocol.Description, r *protocol.CompareViewQueriesRequest) {
				r.Pairs[0].Current.Identity.Kind = "table"
			}},
		{"a held view without a name", `.*view query equivalence: pair 1 holds no view or materialized view of the engine`,
			func(_ *protocol.Description, r *protocol.CompareViewQueriesRequest) {
				r.Pairs[1].Current.Identity.Name = ""
			}},
		{"a held view with a path", `.*view query equivalence: pair 0 holds no view or materialized view of the engine`,
			func(_ *protocol.Description, r *protocol.CompareViewQueriesRequest) {
				r.Pairs[0].Current.Identity.Path = []string{"users"}
			}},
		{"a held view with an overload", `.*view query equivalence: pair 0 holds no view or materialized view of the engine`,
			func(_ *protocol.Description, r *protocol.CompareViewQueriesRequest) {
				r.Pairs[0].Current.Identity.Overload = []string{"integer"}
			}},
		{"a kind the engine does not carry", `.*view query equivalence: the engine does not carry the materialized_view objects pair 1 holds`,
			func(d *protocol.Description, _ *protocol.CompareViewQueriesRequest) {
				d.Engines[0].CommonObjects.Kinds = kindsWithout(protocol.CommonObjectSupport_MATERIALIZED_VIEW)
			}},
		{"a host without common objects", `.*view query equivalence: the engine does not carry the view objects pair 0 holds`,
			func(d *protocol.Description, _ *protocol.CompareViewQueriesRequest) {
				d.Engines[0].CommonObjects = nil
			}},
		{"a held view without its query", `.*view query equivalence: held view "active" has no query`, func(_ *protocol.Description, r *protocol.CompareViewQueriesRequest) {
			r.Pairs[0].Current.Body = " \n"
		}},
		{"a held view with facets", `.*view query equivalence: held view "totals" carries no facets`, func(_ *protocol.Description, r *protocol.CompareViewQueriesRequest) {
			r.Pairs[1].Current.Facets = []*protocol.Envelope{{Kind: "example.org/fixture/view-options"}}
		}},
		{"no declared query", `.*view query equivalence: the pair of view "totals" has no declared query`, func(_ *protocol.Description, r *protocol.CompareViewQueriesRequest) {
			r.Pairs[1].Declared = "\t"
		}},
		{"a view named twice", `.*view query equivalence: view "active" is named twice`, func(_ *protocol.Description, r *protocol.CompareViewQueriesRequest) {
			r.Pairs = append(r.Pairs, proto.CloneOf(r.Pairs[0]))
		}},
		// The engine folds table names, and a view and a materialized view
		// share one namespace, so "Active" is the view active again.
		{"a view and a materialized view of one folded name", `.*view query equivalence: view "Active" is named twice`,
			func(_ *protocol.Description, r *protocol.CompareViewQueriesRequest) {
				second := proto.CloneOf(r.Pairs[1])
				second.Current.Identity.Name = "Active"
				r.Pairs = append(r.Pairs, second)
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := viewQueryExchange()
			test.mutate(d, request)
			err := provider.ValidateCompareViewQueriesRequest(d, request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// A reply is complete and answers each pair of the request in order, under
// the same view.
func TestComparedViewQueries_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.ComparedViewQueries)
	}{
		{"incomplete", `.*view query equivalence: the reply does not answer every pair`, func(r *protocol.ComparedViewQueries) { r.Complete = false }},
		{"a pair missing", `.*view query equivalence: the reply does not answer every pair`, func(r *protocol.ComparedViewQueries) {
			r.Queries = r.Queries[:1]
		}},
		{"another order", `.*view query equivalence: answer 0 names "totals" where the request names "active"`, func(r *protocol.ComparedViewQueries) {
			r.Queries[0], r.Queries[1] = r.Queries[1], r.Queries[0]
		}},
		{"a renamed view", `.*view query equivalence: answer 0 names "inactive" where the request names "active"`, func(r *protocol.ComparedViewQueries) {
			r.Queries[0].Identity.Name = "inactive"
		}},
		{"another kind", `.*view query equivalence: answer 1 names "totals" where the request names "totals"`, func(r *protocol.ComparedViewQueries) {
			r.Queries[1].Identity.Kind = "view"
		}},
		{"another schema", `.*view query equivalence: answer 0 names "active" where the request names "active"`, func(r *protocol.ComparedViewQueries) {
			r.Queries[0].Identity.Schema = "other"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := viewQueryExchange()
			test.mutate(result)
			err := provider.ValidateComparedViewQueries(d, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// A missing request and a missing reply are refused as incomplete.
func TestViewQueryEquivalenceMissingMessages_FailurePath(t *testing.T) {
	t.Parallel()
	t.Run("no request", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := viewQueryExchange()
		err := provider.ValidateCompareViewQueriesRequest(d, nil)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(err, qt.ErrorMatches, `.*view query equivalence: the request is missing`)
	})
	t.Run("no reply", func(t *testing.T) {
		c := qt.New(t)
		d, request, _ := viewQueryExchange()
		err := provider.ValidateComparedViewQueries(d, request, nil)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(err, qt.ErrorMatches, `.*view query equivalence: the reply does not answer every pair`)
	})
}

// An engine that compares view queries needs the provider's feature.
func TestViewQueryEquivalenceDescription_FailurePath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, _, _ := viewQueryExchange()
	d.Features = withoutFeature(d.Features, provider.ViewQueryEquivalenceFeature)
	err := provider.ValidateDescription(d, nil)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, errorEndsWith("engine schema.view-query-equivalence requires the provider's"))
}
