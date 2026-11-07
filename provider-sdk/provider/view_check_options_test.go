package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// checkOptionDescription is commonDescription with view check options listed
// by the provider and its engine.
func checkOptionDescription() *protocol.Description {
	d := commonDescription()
	d.Features = append(d.Features, provider.ViewCheckOptionsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ViewCheckOptionsFeature)
	return d
}

// checkOptionRequest prepares one view with the check option given, under a
// target that lists view check options when listed is true.
func checkOptionRequest(withCheck bool, option protocol.SQLView_CheckOption, listed bool) *protocol.PrepareCommonObjectsRequest {
	request := prepareRequest()
	request.Declared.Views[0].WithCheck, request.Declared.Views[0].CheckOption = withCheck, option
	if listed {
		request.TargetFeatures = []string{provider.CommonObjectsFeature, provider.ViewCheckOptionsFeature}
	}
	return request
}

func TestViewCheckOptions_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name      string
		withCheck bool
		option    protocol.SQLView_CheckOption
		listed    bool
	}{
		{"a LOCAL check option", true, protocol.SQLView_LOCAL, true},
		{"a CASCADED check option", true, protocol.SQLView_CASCADED, true},
		{"no check option", false, protocol.SQLView_CHECK_OPTION_NONE, true},
		{"a check option without its level, for a target that does not list the feature", true, protocol.SQLView_CHECK_OPTION_NONE, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := checkOptionRequest(test.withCheck, test.option, test.listed)
			prepared := preparedCommon()
			prepared.Prepared.Views[0].WithCheck, prepared.Prepared.Views[0].CheckOption = test.withCheck, test.option

			c.Assert(provider.ValidateDescription(checkOptionDescription(), nil), qt.IsNil)
			c.Assert(provider.ValidatePreparedCommonObjects(checkOptionDescription(), request, prepared), qt.IsNil)
		})
	}
}

// A level reaches a target only under the feature, with_check agrees with it,
// and the level is one this SDK knows.
func TestViewCheckOptions_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name      string
		withCheck bool
		option    protocol.SQLView_CheckOption
		listed    bool
		want      string
	}{
		{"a level for a target that does not list the feature", true, protocol.SQLView_LOCAL, false, `.*the check option level of view active was not negotiated`},
		{"a level without with_check", false, protocol.SQLView_LOCAL, true, `.*view active states with_check exactly when its check option is CASCADED or LOCAL`},
		{"with_check without a level", true, protocol.SQLView_CHECK_OPTION_NONE, true, `.*view active states with_check exactly when its check option is CASCADED or LOCAL`},
		{"an unknown level", true, protocol.SQLView_CheckOption(7), true, `.*view active states an unknown check option level`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := provider.ValidatePrepareCommonObjectsRequest(checkOptionDescription(), checkOptionRequest(test.withCheck, test.option, test.listed))

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// A host that does not list the feature sees no engine that carries levels,
// so a level it is sent is refused as unnegotiated, and a preparation keeps
// the level it was given.
func TestViewCheckOptionNegotiation_FailurePath(t *testing.T) {
	t.Run("a host that does not list the feature", func(t *testing.T) {
		c := qt.New(t)
		negotiated := provider.NegotiatedDescription(checkOptionDescription(), slices.DeleteFunc(provider.TwoWayFeatures(), func(feature string) bool {
			return feature == provider.ViewCheckOptionsFeature
		}))

		err := provider.ValidatePrepareCommonObjectsRequest(negotiated, checkOptionRequest(true, protocol.SQLView_LOCAL, false))

		c.Assert(err, qt.ErrorMatches, `.*the check option level of view active was not negotiated`)
	})
	t.Run("a preparation that changes the level", func(t *testing.T) {
		c := qt.New(t)
		prepared := preparedCommon()
		prepared.Prepared.Views[0].WithCheck, prepared.Prepared.Views[0].CheckOption = true, protocol.SQLView_CASCADED

		err := provider.ValidatePreparedCommonObjects(checkOptionDescription(), checkOptionRequest(true, protocol.SQLView_LOCAL, true), prepared)

		c.Assert(err, qt.ErrorMatches, `.*preparation changed the check option level of view active`)
	})
	t.Run("an engine feature without the provider's", func(t *testing.T) {
		c := qt.New(t)
		d := checkOptionDescription()
		d.Features = slices.DeleteFunc(d.Features, func(feature string) bool { return feature == provider.ViewCheckOptionsFeature })

		c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*view check options require the provider's feature`)
	})
}
