package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// viewQueryContextExchange is the view query exchange of an engine that lists
// schema.view-query-context, for a target whose features are targetFeatures:
// the request carries the two columns of table users, and the reply leaves
// view active undecided.
func viewQueryContextExchange(targetFeatures []string) (*protocol.Description, *protocol.CompareViewQueriesRequest, *protocol.ComparedViewQueries) {
	d, request, result := viewQueryExchange()
	d.Features = append(d.Features, provider.ViewQueryContextFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ViewQueryContextFeature)
	request.TargetFeatures = targetFeatures
	users := &protocol.ComparisonIdentity{Kind: "table", Schema: &protocol.ComparisonPart{Source: "app", Normalized: "app"},
		Name: &protocol.ComparisonPart{Source: "users", Normalized: "users"}}
	request.DeclaredColumns = []*protocol.ComparisonColumn{{Table: users, Name: "id", Type: "bigint"}, {Table: proto.CloneOf(users), Name: "name", Type: "text"}}
	result.Queries[0].Undecided = true
	return d, request, result
}

// contextTargets are the target features of the exchange's request: view
// query context for a listed target, none otherwise.
func contextTargets(listed bool) []string {
	return map[bool][]string{false: nil, true: {provider.ViewQueryContextFeature}}[listed]
}

// withoutViewQueryContext lists every two-way feature but view query context.
func withoutViewQueryContext() []string {
	return slices.DeleteFunc(provider.TwoWayFeatures(), func(feature string) bool { return feature == provider.ViewQueryContextFeature })
}

func TestViewQueryContext_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("declared columns and an undecided answer survive the wire", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := viewQueryContextExchange(contextTargets(true))
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		data, err := proto.Marshal(result)
		c.Assert(err, qt.IsNil)
		decoded := &protocol.ComparedViewQueries{}
		c.Assert(proto.Unmarshal(data, decoded), qt.IsNil)
		c.Assert(provider.ValidateComparedViewQueries(d, request, decoded), qt.IsNil)
		c.Assert(decoded.Queries[0].GetUndecided(), qt.IsTrue)
		c.Assert(decoded.Queries[1].GetUndecided(), qt.IsFalse)
	})
	t.Run("a target that does not list the feature gets no columns and answers every pair", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := viewQueryContextExchange(contextTargets(false))
		request.DeclaredColumns = nil
		result.Queries[0].Undecided = false
		c.Assert(provider.ValidateComparedViewQueries(d, request, result), qt.IsNil)
	})
	t.Run("a host that does not list the feature sees an engine without it", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := viewQueryContextExchange(contextTargets(true))
		negotiated := provider.NegotiatedDescription(d, withoutViewQueryContext())
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.ViewQueryContextFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.ViewQueryEquivalenceFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
}

// Declared columns and an undecided answer need the feature on both sides,
// each column names a table and has a name and a type, a table's column is
// listed once, and an undecided answer is not equivalent.
func TestViewQueryContext_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		listed bool
		kind   string
		column string
		want   string
	}{
		{"columns for a target that does not list the feature", false, "table", "name",
			`.*view query equivalence: declared columns need schema\.view-query-context on both sides`},
		{"a column of a view", true, "view", "name", `.*view query equivalence: declared column "id" names a "view", not a table`},
		{"a column without a name", true, "table", "", `.*view query equivalence: a declared column requires a name and a type`},
		{"a column named twice", true, "table", "id", `.*view query equivalence: declared column "id" of table "users" is named twice`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := viewQueryContextExchange(contextTargets(test.listed))
			for _, column := range request.DeclaredColumns {
				column.Table.Kind = test.kind
			}
			request.DeclaredColumns[1].Name = test.column

			err := provider.ValidateCompareViewQueriesRequest(d, request)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
	t.Run("an undecided answer for a target that does not list the feature", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := viewQueryContextExchange(contextTargets(false))
		request.DeclaredColumns = nil

		err := provider.ValidateComparedViewQueries(d, request, result)

		c.Assert(err, qt.ErrorMatches, `.*view query equivalence: answer 0 is undecided, which needs schema\.view-query-context on both sides`)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
	t.Run("an undecided answer that is equivalent", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := viewQueryContextExchange(contextTargets(true))
		result.Queries[0].Equivalent = true

		err := provider.ValidateComparedViewQueries(d, request, result)

		c.Assert(err, qt.ErrorMatches, `.*view query equivalence: answer 0 is undecided and equivalent`)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
}

// The engine's feature needs the provider's and view query equivalence on the
// engine.
func TestViewQueryContextDescription_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		without string
		scope   string
	}{
		{"an engine feature without the provider's", provider.ViewQueryContextFeature, "provider"},
		{"an engine that does not compare view queries", provider.ViewQueryEquivalenceFeature, "engine"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := viewQueryContextExchange(contextTargets(true))
			features := map[string]*[]string{"provider": &d.Features, "engine": &d.Engines[0].Features}[test.scope]
			*features = slices.DeleteFunc(*features, func(feature string) bool { return feature == test.without })

			err := provider.ValidateDescription(d, nil)

			c.Assert(err, qt.ErrorMatches, `.*engine schema\.view-query-context requires the provider's and schema\.view-query-equivalence`)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
