package provider_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func version(major, minor uint32) *protocol.ContractVersion {
	return &protocol.ContractVersion{Major: major, Minor: minor}
}

func TestContractVersion_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(proto.Equal(provider.Contract(), version(provider.Revision, provider.ContractMinor)), qt.IsTrue)
	c.Assert(provider.ContractString(provider.Contract()), qt.Equals, "25.5")
	// A peer of minor 0 announces no version and declares no minimum.
	c.Assert(proto.Equal(provider.MinimumContract(&protocol.Description{Revision: 25}), version(25, 0)), qt.IsTrue)
	c.Assert(proto.Equal(provider.HostContract(&protocol.DescribeRequest{Revision: 25}), version(25, 0)), qt.IsTrue)
	c.Assert(proto.Equal(provider.MinimumContract(&protocol.Description{Revision: 25, MinContract: version(25, 1)}), version(25, 1)), qt.IsTrue)
	tests := []struct {
		name          string
		host, minimum *protocol.ContractVersion
	}{
		{"the same version", version(25, 1), version(25, 1)},
		{"a newer host minor", version(25, 3), version(25, 1)},
		{"a host of minor 0 and no minimum", version(25, 0), version(25, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.CheckContract(test.host, test.minimum), qt.IsNil)
		})
	}
}

// A provider needs a host of its major and of at least its minimum minor; the
// refusal names both versions.
func TestContractVersion_FailurePath(t *testing.T) {
	tests := []struct {
		name          string
		host, minimum *protocol.ContractVersion
		want          string
	}{
		{"an older host minor", version(25, 0), version(25, 1),
			`provider requires contract 25\.1 or a later minor of it; this host speaks 25\.0`},
		{"another major above", version(25, 1), version(26, 0),
			`provider requires contract 26\.0 or a later minor of it; this host speaks 25\.1`},
		{"another major below", version(26, 4), version(25, 0),
			`provider requires contract 25\.0 or a later minor of it; this host speaks 26\.4`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := provider.CheckContract(test.host, test.minimum)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			var refused *provider.ContractError
			c.Assert(err, qt.ErrorAs, &refused)
			c.Assert(proto.Equal(refused.Minimum, test.minimum), qt.IsTrue)
		})
	}
}

// A provider may require at most the minor of the SDK it is built with, and
// only within its own major.
func TestMinimumContractDescription_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		minimum *protocol.ContractVersion
	}{
		{"a minor above the SDK's", version(provider.Revision, provider.ContractMinor+1)},
		{"another major", version(provider.Revision+1, 0)},
		{"an older major", version(provider.Revision-1, provider.ContractMinor)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description := dataDescription()
			description.MinContract = test.minimum
			c.Assert(provider.ValidateDescription(description, nil), qt.ErrorMatches, `.*minimum contract .* is outside contract 25\.5`)
		})
	}
	c := qt.New(t)
	description := dataDescription()
	for _, minimum := range []*protocol.ContractVersion{nil, version(provider.Revision, 0), version(provider.Revision, provider.ContractMinor)} {
		description.MinContract = minimum
		c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
	}
}

func TestValidateHostFeatures_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.ValidateHostFeatures(nil), qt.IsNil)
	c.Assert(provider.ValidateHostFeatures(provider.TwoWayFeatures()), qt.IsNil)
	// A newer host lists features this SDK does not know.
	c.Assert(provider.ValidateHostFeatures([]string{"units.statement-timeout", "lint.later-facts"}), qt.IsNil)
}

func TestValidateHostFeatures_FailurePath(t *testing.T) {
	for _, features := range [][]string{{""}, {"query.dialect", "query.dialect"}, {"query dialect"}, {"query.dialect\n"}} {
		c := qt.New(t)
		c.Assert(provider.ValidateHostFeatures(features), qt.ErrorIs, provider.ErrProtocol)
	}
}

// twoWayDescription offers every two-way feature, beside services that need
// no negotiation.
func twoWayDescription() *protocol.Description {
	description := dataDescription()
	description.Features = append(description.Features, provider.ExecuteFeature, provider.StatementTimeoutFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.LintStatementFactsFeature, provider.DevRealmsFeature)
	engine.QueryDialect = queryDialect()
	return description
}

// A host that lists nothing gets the description of contract 25.0 and its
// services; a host that lists a feature gets that feature and no other.
func TestNegotiatedDescription(t *testing.T) {
	c := qt.New(t)
	offered := twoWayDescription()
	before := proto.CloneOf(offered)
	old := provider.NegotiatedDescription(offered, nil)
	c.Assert(old.Features, qt.DeepEquals, append(dataFeatures(), provider.ExecuteFeature))
	c.Assert(old.Engines[0].Features, qt.DeepEquals, dataFeatures())
	c.Assert(old.Engines[0].QueryDialect, qt.IsNil)
	c.Assert(proto.Equal(offered, before), qt.IsTrue)
	timeouts := provider.NegotiatedDescription(offered, []string{provider.StatementTimeoutFeature, "a.later-feature"})
	c.Assert(timeouts.Features, qt.DeepEquals, append(dataFeatures(), provider.ExecuteFeature, provider.StatementTimeoutFeature))
	c.Assert(timeouts.Engines[0].Features, qt.DeepEquals, dataFeatures())
	all := provider.NegotiatedDescription(offered, provider.TwoWayFeatures())
	c.Assert(proto.Equal(all, offered), qt.IsTrue)
	c.Assert(provider.NegotiatedDescription(nil, nil), qt.IsNil)
}

func TestHostFeaturesTravelInTheContext(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.HostFeatures(context.Background()), qt.IsNil)
	listed := []string{provider.QueryDialectFeature}
	ctx := provider.ContextWithHostFeatures(context.Background(), listed)
	listed[0] = "changed"
	got := provider.HostFeatures(ctx)
	c.Assert(got, qt.DeepEquals, []string{provider.QueryDialectFeature})
	got[0] = "changed"
	c.Assert(provider.HostFeatures(ctx), qt.DeepEquals, []string{provider.QueryDialectFeature})
}
