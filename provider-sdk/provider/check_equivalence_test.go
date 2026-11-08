package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// checkEquivalenceExchange asks whether the declared `score >= 0` and the
// held CHECK score_check, which the server stores with a cast literal, are one
// constraint, and answers that they are.
func checkEquivalenceExchange() (*protocol.Description, *protocol.CompareChecksRequest, *protocol.ComparedChecks) {
	d, planning, _ := planningExchange()
	d.Features = append(d.Features, provider.CheckEquivalenceFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.CheckEquivalenceFeature)
	table := planning.Tables[0]
	request := &protocol.CompareChecksRequest{Engine: planning.Engine, TargetFeatures: planning.TargetFeatures, Identifiers: planning.Identifiers,
		Capabilities: planning.Capabilities, Tables: []*protocol.CheckComparisonTable{{
			Subject: proto.CloneOf(table.Subject),
			Current: &protocol.ObservedTable{Name: table.Current.Name, Schema: table.Current.Schema,
				Columns: []*protocol.ObservedColumn{{Name: "score", DataType: "bigint", IsNullable: "NO"}, {Name: "kind", DataType: "character varying", IsNullable: "YES"}},
				Constraints: []*protocol.ObservedConstraint{
					{Name: "score_check", TableName: table.Current.Name, Schema: table.Current.Schema, Type: "CHECK", CheckClause: new("(score >= '0'::bigint)")},
					{Name: "kind_check", TableName: table.Current.Name, Schema: table.Current.Schema, Type: "CHECK", CheckClause: new("(kind = ANY (ARRAY['a', 'b']))")},
				}},
			Pairs: []*protocol.CheckComparisonPair{{Constraint: "score_check", Declared: "score >= 0"}, {Constraint: "kind_check", Declared: "kind IN ('a', 'c')"}},
		}}}
	result := &protocol.ComparedChecks{Complete: true, Tables: []*protocol.ComparedCheckTable{{Subject: proto.CloneOf(table.Subject),
		Checks: []*protocol.ComparedCheck{{Constraint: "score_check", Equivalent: true}, {Constraint: "kind_check"}}}}}
	return d, request, result
}

func TestCheckEquivalence_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("a reply answers each pair in order and survives the wire", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := checkEquivalenceExchange()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		before := proto.CloneOf(request)
		c.Assert(provider.ValidateComparedChecks(d, request, result), qt.IsNil)
		c.Assert(proto.Equal(before, request), qt.IsTrue)
		data, err := proto.Marshal(result)
		c.Assert(err, qt.IsNil)
		decoded := &protocol.ComparedChecks{}
		c.Assert(proto.Unmarshal(data, decoded), qt.IsNil)
		c.Assert(provider.ValidateComparedChecks(d, request, decoded), qt.IsNil)
		c.Assert(decoded.Tables[0].Checks[0].GetEquivalent(), qt.IsTrue)
		c.Assert(decoded.Tables[0].Checks[1].GetEquivalent(), qt.IsFalse)
	})
	t.Run("a request with no table has an empty answer", func(t *testing.T) {
		c := qt.New(t)
		d, request, _ := checkEquivalenceExchange()
		request.Tables = nil
		c.Assert(provider.ValidateComparedChecks(d, request, &protocol.ComparedChecks{Complete: true}), qt.IsNil)
	})
	// The RPC is new and adds nothing to an existing message, so a host that
	// lists no feature still sees the engine compare checks, and calls it only
	// if it knows the call.
	t.Run("the feature is the provider's alone", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := checkEquivalenceExchange()
		c.Assert(provider.TwoWayFeatures(), qt.Not(qt.Contains), provider.CheckEquivalenceFeature)
		negotiated := provider.NegotiatedDescription(d, nil)
		c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.CheckEquivalenceFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
}

// A request names an engine that compares checks, and each pair names a CHECK
// the held table keeps, with a declared expression beside it.
func TestCompareChecksRequest_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.CompareChecksRequest)
	}{
		{"the provider does not compare checks", `.*CHECK equivalence: the provider does not compare checks`, func(d *protocol.Description, _ *protocol.CompareChecksRequest) {
			d.Features = withoutFeature(d.Features, provider.CheckEquivalenceFeature)
		}},
		{"the engine does not compare checks", `.*CHECK equivalence: the engine does not compare checks`, func(d *protocol.Description, _ *protocol.CompareChecksRequest) {
			d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.CheckEquivalenceFeature)
		}},
		{"another engine", `.*value engine is not owned by this provider`, func(_ *protocol.Description, r *protocol.CompareChecksRequest) { r.Engine = "other" }},
		{"no identifier facts", `.*identifier rules are required`, func(_ *protocol.Description, r *protocol.CompareChecksRequest) { r.Identifiers = nil }},
		{"no held table", `.*CHECK equivalence: a table requires its held state`, func(_ *protocol.Description, r *protocol.CompareChecksRequest) {
			r.Tables[0].Current = nil
		}},
		{"a held table of another name", `.*captured table identity differs from its subject`, func(_ *protocol.Description, r *protocol.CompareChecksRequest) {
			r.Tables[0].Current.Name = "other"
		}},
		{"captured features", `.*CHECK equivalence: a held table carries no captured features`, func(_ *protocol.Description, r *protocol.CompareChecksRequest) {
			r.Tables[0].Current.Features = &protocol.CapturedFeatures{}
		}},
		{"no pair", `.*CHECK equivalence: a table requires a pair`, func(_ *protocol.Description, r *protocol.CompareChecksRequest) {
			r.Tables[0].Pairs = nil
		}},
		{"a constraint named twice", `.*CHECK equivalence: table "C" names constraint "score_check" twice`, func(_ *protocol.Description, r *protocol.CompareChecksRequest) {
			r.Tables[0].Pairs[1].Constraint = "score_check"
		}},
		{"a constraint the table does not hold", `.*names "missing", which is no CHECK the table holds with an expression`, func(_ *protocol.Description, r *protocol.CompareChecksRequest) {
			r.Tables[0].Pairs[0].Constraint = "missing"
		}},
		{"a held constraint that is no CHECK", `.*names "score_check", which is no CHECK the table holds with an expression`, func(_ *protocol.Description, r *protocol.CompareChecksRequest) {
			r.Tables[0].Current.Constraints[0].Type = "UNIQUE"
		}},
		{"a held CHECK without its expression", `.*names "score_check", which is no CHECK the table holds with an expression`, func(_ *protocol.Description, r *protocol.CompareChecksRequest) {
			r.Tables[0].Current.Constraints[0].CheckClause = nil
		}},
		{"no declared expression", `.*the pair of CHECK "kind_check" of table "C" has no declared expression`, func(_ *protocol.Description, r *protocol.CompareChecksRequest) {
			r.Tables[0].Pairs[1].Declared = " "
		}},
		{"two tables of one subject", `.*duplicate comparison identity`, func(_ *protocol.Description, r *protocol.CompareChecksRequest) {
			r.Tables = append(r.Tables, proto.CloneOf(r.Tables[0]))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := checkEquivalenceExchange()
			test.mutate(d, request)
			err := provider.ValidateCompareChecksRequest(d, request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// A reply is complete and answers each table and each pair of the request in
// order, under the same subject and constraint name.
func TestComparedChecks_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.ComparedChecks)
	}{
		{"incomplete", `.*CHECK equivalence: the reply does not answer every table`, func(r *protocol.ComparedChecks) { r.Complete = false }},
		{"a table missing", `.*CHECK equivalence: the reply does not answer every table`, func(r *protocol.ComparedChecks) { r.Tables = nil }},
		{"another table", `.*CHECK equivalence: answer 0 names another table than the request`, func(r *protocol.ComparedChecks) {
			r.Tables[0].Subject.Name = &protocol.ComparisonPart{Source: "other", Normalized: "other"}
		}},
		{"a pair missing", `.*CHECK equivalence: the answer for table "C" does not answer every pair`, func(r *protocol.ComparedChecks) {
			r.Tables[0].Checks = r.Tables[0].Checks[:1]
		}},
		{"another order", `.*CHECK equivalence: answer 0 for table "C" names "kind_check" where the request names "score_check"`, func(r *protocol.ComparedChecks) {
			r.Tables[0].Checks[0], r.Tables[0].Checks[1] = r.Tables[0].Checks[1], r.Tables[0].Checks[0]
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := checkEquivalenceExchange()
			test.mutate(result)
			err := provider.ValidateComparedChecks(d, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// An engine that compares checks needs the provider's feature.
func TestCheckEquivalenceDescription_FailurePath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, _, _ := checkEquivalenceExchange()
	d.Features = withoutFeature(d.Features, provider.CheckEquivalenceFeature)
	err := provider.ValidateDescription(d, nil)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, errorEndsWith("engine schema.check-equivalence requires the provider's"))
}
