package providerrelease_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerrelease"
	"ptah.run/provider-sdk/provider"
)

func TestMeets(t *testing.T) {
	for _, test := range []struct {
		name    string
		host    providerrelease.Contract
		minimum providerrelease.Contract
		want    bool
	}{
		{"same version", providerrelease.Contract{Major: 25, Minor: 1}, providerrelease.Contract{Major: 25, Minor: 1}, true},
		{"host minor above the minimum", providerrelease.Contract{Major: 25, Minor: 3}, providerrelease.Contract{Major: 25, Minor: 0}, true},
		{"host minor below the minimum", providerrelease.Contract{Major: 25, Minor: 0}, providerrelease.Contract{Major: 25, Minor: 1}, false},
		{"older major", providerrelease.Contract{Major: 25, Minor: 9}, providerrelease.Contract{Major: 24, Minor: 0}, false},
		{"newer major", providerrelease.Contract{Major: 25, Minor: 9}, providerrelease.Contract{Major: 26, Minor: 0}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(providerrelease.Meets(test.host, test.minimum), qt.Equals, test.want)
		})
	}
}

// The host compares a release's minimum with the contract it announces to the
// providers it starts. Measured with minor 0 in its place: an installed YDB
// provider that needs 25.7 was refused with "this Ptah speaks contract 25.0"
// by a host that announces 25.22.
func TestHostContractIsTheContractTheHostAnnounces(t *testing.T) {
	c := qt.New(t)
	c.Assert(providerrelease.HostContract(), qt.Equals, providerrelease.Contract{Major: provider.Revision, Minor: provider.ContractMinor})
	c.Assert(providerrelease.Meets(providerrelease.HostContract(), providerrelease.Contract{Major: provider.Revision, Minor: provider.ContractMinor}), qt.IsTrue)
}

func TestContractWording(t *testing.T) {
	c := qt.New(t)
	minimum := providerrelease.Contract{Major: 26, Minor: 2}
	c.Assert(minimum.String(), qt.Equals, "26.2")
	c.Assert(minimum.Requirement(), qt.Equals, "26.2 or a later 26.x")
}
