package provider_test

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// reasonDescription is commonDescription whose engine states why its reads
// leave roles out of scope.
func reasonDescription() *protocol.Description {
	d := commonDescription()
	d.Features = append(d.Features, provider.RoleScopeReasonsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.RoleScopeReasonsFeature)
	return d
}

// reasonInspection is a read of reasonDescription's engine that names a role
// out of scope and says why.
func reasonInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	_, request, snapshot := commonInspection()
	snapshot.CommonObjects.RolesOutOfScopeReason = "the cluster made them"
	return reasonDescription(), request, snapshot
}

func TestRoleScopeReasonsDescription_HappyPath(t *testing.T) {
	c := qt.New(t)
	d := reasonDescription()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	listed := provider.NegotiatedDescription(d, []string{provider.CommonObjectsFeature, provider.RoleScopeReasonsFeature})
	c.Assert(proto.Equal(listed, d), qt.IsTrue)
}

func TestRoleScopeReasonsDescription_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"the engine's feature without the provider's", func(d *protocol.Description) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.RoleScopeReasonsFeature })
		}},
		{"an engine that carries no role", func(d *protocol.Description) {
			d.Engines[0].CommonObjects.Kinds = kindsWithout(protocol.CommonObjectSupport_ROLE)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d := reasonDescription()
			test.mutate(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*role scope reasons need .*`)
		})
	}
}

// A host that does not list the feature receives an engine without it, and a
// host that does not list the common objects receives neither, so the
// negotiated description stays valid either way.
func TestRoleScopeReasonsNegotiation(t *testing.T) {
	for _, test := range []struct {
		name     string
		features []string
	}{
		{"the feature not listed", []string{provider.CommonObjectsFeature}},
		{"the common objects not listed", []string{provider.RoleScopeReasonsFeature}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			negotiated := provider.NegotiatedDescription(reasonDescription(), test.features)
			c.Assert(negotiated.GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.RoleScopeReasonsFeature)
			c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		})
	}
}

func TestRolesOutOfScopeReason_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, snapshot := reasonInspection()
	_, err := provider.InspectionValues(d, request, snapshot)
	c.Assert(err, qt.IsNil)
}

func TestRolesOutOfScopeReason_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.ObservedCommonObjects)
	}{
		{"a reason the engine did not negotiate", "a reason for roles out of scope needs schema.role-scope-reasons",
			func(d *protocol.Description, _ *protocol.ObservedCommonObjects) {
				d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.RoleScopeReasonsFeature })
			}},
		{"a reason without a role out of scope", "a reason for roles out of scope needs a role out of scope",
			func(_ *protocol.Description, o *protocol.ObservedCommonObjects) { o.RolesOutOfScope = nil }},
		{"a reason that ends with a period", "a reason for roles out of scope is one trimmed line",
			func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
				o.RolesOutOfScopeReason = "the cluster made them."
			}},
		{"a reason of two lines", "a reason for roles out of scope is one trimmed line",
			func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
				o.RolesOutOfScopeReason = "the cluster\nmade them"
			}},
		{"an untrimmed reason", "a reason for roles out of scope is one trimmed line",
			func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
				o.RolesOutOfScopeReason = " the cluster made them"
			}},
		{"a reason above 300 bytes", "a reason for roles out of scope is one trimmed line",
			func(_ *protocol.Description, o *protocol.ObservedCommonObjects) {
				o.RolesOutOfScopeReason = strings.Repeat("x", 301)
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, snapshot := reasonInspection()
			test.mutate(d, snapshot.CommonObjects)
			_, err := provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want)+".*")
		})
	}
}
