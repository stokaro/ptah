package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// enumDescription is a description whose engine states its enum columns in
// columns, with the feature listed by the provider and the engine.
func enumDescription(columns *protocol.EnumColumns) *protocol.Description {
	description := dataDescription()
	description.Features = append(description.Features, provider.EnumColumnsFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.EnumColumnsFeature)
	description.Engines[0].EnumColumns = columns
	return description
}

func TestEnumColumns_HappyPath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		columns *protocol.EnumColumns
	}{
		{name: "a type that lists the values", columns: &protocol.EnumColumns{Form: protocol.EnumColumns_VALUES_TYPE}},
		{name: "a text column under a CHECK", columns: &protocol.EnumColumns{Form: protocol.EnumColumns_CHECK, CheckType: "TEXT"}},
		{name: "a column type with a length", columns: &protocol.EnumColumns{Form: protocol.EnumColumns_CHECK, CheckType: "NVARCHAR(255)"}},
		{name: "no enum at all", columns: &protocol.EnumColumns{Form: protocol.EnumColumns_REFUSED}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateDescription(enumDescription(test.columns), nil), qt.IsNil)
		})
	}
	// A host that does not list the feature keeps the enum a type of its own,
	// so it receives neither the form nor the feature.
	t.Run("a host that does not list the feature receives no form", func(t *testing.T) {
		c := qt.New(t)
		description := enumDescription(&protocol.EnumColumns{Form: protocol.EnumColumns_CHECK, CheckType: "TEXT"})
		negotiated := provider.NegotiatedDescription(description, withoutFeature(provider.TwoWayFeatures(), provider.EnumColumnsFeature))
		c.Assert(negotiated.Engines[0].GetEnumColumns(), qt.IsNil)
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.EnumColumnsFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.EnumColumnsFeature)
	})
	t.Run("a host that lists the feature receives the form", func(t *testing.T) {
		c := qt.New(t)
		description := enumDescription(&protocol.EnumColumns{Form: protocol.EnumColumns_REFUSED})
		negotiated := provider.NegotiatedDescription(description, provider.TwoWayFeatures())
		c.Assert(negotiated.Engines[0].GetEnumColumns().GetForm(), qt.Equals, protocol.EnumColumns_REFUSED)
		c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.EnumColumnsFeature)
	})
}

func TestEnumColumns_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		columns *protocol.EnumColumns
		want    string
	}{
		{name: "the feature without a form", columns: nil, want: `.*an engine states its enum columns exactly when it lists schema\.enum-columns`},
		{name: "no form", columns: &protocol.EnumColumns{}, want: `.*enum columns of engine rows state no form`},
		{name: "an unknown form", columns: &protocol.EnumColumns{Form: protocol.EnumColumns_Form(9)}, want: `.*enum columns of engine rows state no form`},
		{name: "a CHECK without a column type", columns: &protocol.EnumColumns{Form: protocol.EnumColumns_CHECK},
			want: `.*enum columns of engine rows are CHECK columns and need a column type on one line`},
		{name: "a CHECK column type with white space around it", columns: &protocol.EnumColumns{Form: protocol.EnumColumns_CHECK, CheckType: " TEXT"},
			want: `.*enum columns of engine rows are CHECK columns and need a column type on one line`},
		{name: "a CHECK column type on two lines", columns: &protocol.EnumColumns{Form: protocol.EnumColumns_CHECK, CheckType: "VARCHAR(\n10)"},
			want: `.*enum columns of engine rows are CHECK columns and need a column type on one line`},
		{name: "a CHECK column type that is not UTF-8", columns: &protocol.EnumColumns{Form: protocol.EnumColumns_CHECK, CheckType: "TEXT\xff"},
			want: `.*enum columns of engine rows are CHECK columns and need a column type on one line`},
		{name: "a column type beside a values type", columns: &protocol.EnumColumns{Form: protocol.EnumColumns_VALUES_TYPE, CheckType: "TEXT"},
			want: `.*enum columns of engine rows state a CHECK column type for a form without a CHECK`},
		{name: "a column type beside a refusal", columns: &protocol.EnumColumns{Form: protocol.EnumColumns_REFUSED, CheckType: "TEXT"},
			want: `.*enum columns of engine rows state a CHECK column type for a form without a CHECK`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := provider.ValidateDescription(enumDescription(test.columns), nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
	t.Run("a form without the engine's feature", func(t *testing.T) {
		c := qt.New(t)
		description := enumDescription(&protocol.EnumColumns{Form: protocol.EnumColumns_REFUSED})
		description.Engines[0].Features = withoutFeature(description.Engines[0].Features, provider.EnumColumnsFeature)
		err := provider.ValidateDescription(description, nil)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(err, qt.ErrorMatches, `.*an engine states its enum columns exactly when it lists schema\.enum-columns`)
	})
	t.Run("the engine's feature without the provider's", func(t *testing.T) {
		c := qt.New(t)
		description := enumDescription(&protocol.EnumColumns{Form: protocol.EnumColumns_REFUSED})
		description.Features = withoutFeature(description.Features, provider.EnumColumnsFeature)
		err := provider.ValidateDescription(description, nil)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(err, qt.ErrorMatches, `.*enum columns require the provider's feature`)
	})
}
