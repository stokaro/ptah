package providerrelease_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/internal/providerrelease"
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

func TestHostContractIsTheSDKRevisionAtMinorZero(t *testing.T) {
	c := qt.New(t)
	c.Assert(providerrelease.HostContract(), qt.Equals, providerrelease.Contract{Major: provider.Revision, Minor: 0})
}

func TestContractWording(t *testing.T) {
	c := qt.New(t)
	minimum := providerrelease.Contract{Major: 26, Minor: 2}
	c.Assert(minimum.String(), qt.Equals, "26.2")
	c.Assert(minimum.Requirement(), qt.Equals, "26.2 or a later 26.x")
}
