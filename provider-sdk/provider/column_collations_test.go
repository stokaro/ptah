package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// collationExchange is a column comparison under negotiated collations: the
// engine lists the feature, the request names it among the target's features
// and asks the collation of its pair, and the reply answers it.
func collationExchange(difference *protocol.ColumnDifference) (*protocol.Description, *protocol.CompareColumnsRequest, *protocol.ComparedColumns) {
	d, request, result := columnExchange()
	d.Features = append(d.Features, provider.ColumnCollationsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ColumnCollationsFeature)
	request.TargetFeatures = append(request.TargetFeatures, provider.ColumnCollationsFeature)
	pair := request.Tables[0].Pairs[0]
	pair.Aspects = append(pair.Aspects, protocol.ColumnAspect_COLUMN_COLLATION)
	column := result.Tables[0].Columns[0]
	column.Aspects = append(column.Aspects, &protocol.ColumnAspectReceipt{Aspect: protocol.ColumnAspect_COLUMN_COLLATION,
		Receipt: &protocol.ColumnAspectReceipt_Difference{Difference: difference}})
	return d, request, result
}

func TestColumnCollations_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name       string
		difference *protocol.ColumnDifference
	}{
		{"the same collation", &protocol.ColumnDifference{Relation: protocol.ColumnDifference_SAME, Current: "NOCASE", Desired: "nocase"}},
		{"a declared collation the column does not hold", &protocol.ColumnDifference{Relation: protocol.ColumnDifference_CHANGED, Desired: "NOCASE"}},
		{"a held collation the declaration drops", &protocol.ColumnDifference{Relation: protocol.ColumnDifference_CHANGED, Current: "NOCASE"}},
		{"both sides on the default", &protocol.ColumnDifference{Relation: protocol.ColumnDifference_SAME}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := collationExchange(test.difference)

			c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
			c.Assert(provider.ValidateComparedColumns(d, request, result), qt.IsNil)
		})
	}
}

// A reply that answers the collation needs SAME or CHANGED, one line for each
// spelling, and two different collations for a change.
func TestColumnCollationReceipt_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name       string
		difference *protocol.ColumnDifference
		want       string
	}{
		{"no relation", &protocol.ColumnDifference{Current: "a", Desired: "b"}, `.*a collation difference needs SAME or CHANGED`},
		{"a change that names one collation twice", &protocol.ColumnDifference{Relation: protocol.ColumnDifference_CHANGED, Current: "NOCASE", Desired: "NOCASE"},
			`.*a changed collation names two different collations`},
		{"a spelling over two lines", &protocol.ColumnDifference{Relation: protocol.ColumnDifference_CHANGED, Desired: "NO\nCASE"},
			`.*a collation spelling is one line without surrounding white space`},
		{"a spelling with surrounding white space", &protocol.ColumnDifference{Relation: protocol.ColumnDifference_SAME, Current: " NOCASE"},
			`.*a collation spelling is one line without surrounding white space`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := collationExchange(test.difference)

			err := provider.ValidateComparedColumns(d, request, result)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// A host that does not list the feature asks no collation: the request is
// refused before a provider sees it, whether the engine or the target lacks
// the feature.
func TestColumnCollationRequest_FailurePath(t *testing.T) {
	t.Run("a target that does not list the feature", func(t *testing.T) {
		c := qt.New(t)
		d, request, _ := collationExchange(&protocol.ColumnDifference{Relation: protocol.ColumnDifference_SAME})
		request.TargetFeatures = slices.DeleteFunc(request.TargetFeatures, func(feature string) bool { return feature == provider.ColumnCollationsFeature })

		err := provider.ValidateColumnsRequest(d, request)

		c.Assert(err, qt.ErrorMatches, `.*a collation aspect was not negotiated`)
	})
	t.Run("a host that does not list the feature", func(t *testing.T) {
		c := qt.New(t)
		d, request, _ := collationExchange(&protocol.ColumnDifference{Relation: protocol.ColumnDifference_SAME})
		negotiated := provider.NegotiatedDescription(d, slices.DeleteFunc(provider.TwoWayFeatures(), func(feature string) bool {
			return feature == provider.ColumnCollationsFeature
		}))
		// Such a host names the target's features as it negotiated them.
		request.TargetFeatures = negotiated.Engines[0].Features

		err := provider.ValidateColumnsRequest(negotiated, request)

		c.Assert(err, qt.ErrorMatches, `.*a collation aspect was not negotiated`)
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.ColumnCollationsFeature)
	})
}

// The engine's feature needs the provider's and the column comparison it
// extends.
func TestColumnCollationsDescription_FailurePath(t *testing.T) {
	t.Run("no provider feature", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := collationExchange(&protocol.ColumnDifference{Relation: protocol.ColumnDifference_SAME})
		d.Features = slices.DeleteFunc(d.Features, func(feature string) bool { return feature == provider.ColumnCollationsFeature })

		c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*column collations require the provider's feature and the column comparison`)
	})
	t.Run("no column comparison on the engine", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := collationExchange(&protocol.ColumnDifference{Relation: protocol.ColumnDifference_SAME})
		d.Engines[0].Features = []string{provider.ColumnCollationsFeature}

		c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*column collations require the provider's feature and the column comparison`)
	})
}
