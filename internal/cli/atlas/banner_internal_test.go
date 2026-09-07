package atlas

// White-box testing required: bannerName is the strict-profile decision, and
// it is deliberately not observable through the command's captured output --
// the banner's writer gate suppresses it for every buffer and pipe, so a
// black-box test would pass whether the profile were consulted or not. That
// vacuous control is the reason this decision has a name to ask.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
)

// TestBannerName_TheStrictProfileCarriesNone pins both answers.
//
// The full profile's row is not decoration: without it, a bannerName that
// returned the empty string for everything would satisfy the strict row and
// remove the banner from a surface that is meant to have one.
func TestBannerName_TheStrictProfileCarriesNone(t *testing.T) {
	tests := []struct {
		name   string
		policy atlascompatpolicy.Policy
		want   string
	}{
		{name: "strict CE", policy: atlascompatpolicy.StrictCE(), want: ""},
		{name: "the full compatibility surface", policy: atlascompatpolicy.Full(), want: "ptah-compat"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(bannerName("ptah-compat", test.policy), qt.Equals, test.want)
		})
	}
}

// TestBannerName_FollowsTheNameTheBinaryWasInvokedAs is the rule the rest of
// this tree already keeps: a drop-in installed as atlas calls itself atlas, so
// the banner over its entry screen has to say the same thing.
func TestBannerName_FollowsTheNameTheBinaryWasInvokedAs(t *testing.T) {
	c := qt.New(t)

	c.Assert(bannerName("atlas", atlascompatpolicy.Full()), qt.Equals, "atlas")
}
