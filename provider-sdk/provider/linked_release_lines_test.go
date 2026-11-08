package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// linkedDescription is linesDescription with linked release lines
// negotiated: the provider and its engine list the feature.
func linkedDescription(lines ...*protocol.ReleaseLine) *protocol.Description {
	description := linesDescription(lines...)
	description.Features = append(description.Features, provider.LinkedReleaseLinesFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.LinkedReleaseLinesFeature)
	return description
}

// linkedLines are the lines of an engine linked into its provider: the line it
// links, certified by the version linked, and an older line it keeps.
func linkedLines() []*protocol.ReleaseLine {
	return []*protocol.ReleaseLine{
		{Line: "1.4", Support: protocol.ReleaseLine_CERTIFIED, Linked: "1.4.2", Note: "The provider's tests run the engine it links"},
		{Line: "1.3", Support: protocol.ReleaseLine_BEST_EFFORT, Unprobed: "the provider links 1.4"},
	}
}

func TestLinkedReleaseLines_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("a certified line names the version the provider links", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(provider.ValidateDescription(linkedDescription(linkedLines()...), nil), qt.IsNil)
	})
	t.Run("a legacy tested line names a version equal to the line", func(t *testing.T) {
		c := qt.New(t)
		line := &protocol.ReleaseLine{Line: "1.0.9", Support: protocol.ReleaseLine_LEGACY_TESTED, Linked: "1.0.9"}
		c.Assert(provider.ValidateDescription(linkedDescription(line), nil), qt.IsNil)
	})
	// A host that cannot read the version sees no evidence for the claim, so
	// the line reaches it as best effort, with the version in the reason.
	t.Run("a host that does not list the feature reads the line as best effort", func(t *testing.T) {
		c := qt.New(t)
		negotiated := provider.NegotiatedDescription(linkedDescription(linkedLines()...), withoutFeature(provider.TwoWayFeatures(), provider.LinkedReleaseLinesFeature))
		line := negotiated.Engines[0].ReleaseLines[0]
		c.Assert(line.GetSupport(), qt.Equals, protocol.ReleaseLine_BEST_EFFORT)
		c.Assert(line.GetLinked(), qt.Equals, "")
		c.Assert(line.GetUnprobed(), qt.Equals, "the provider runs the engine it links, version 1.4.2, which this host reads no evidence of")
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.LinkedReleaseLinesFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.LinkedReleaseLinesFeature)
	})
}

func TestLinkedReleaseLines_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		want  string
		lines []*protocol.ReleaseLine
	}{
		{"an image and a linked engine", `.*release line 1\.4 names both an image and a linked engine`,
			[]*protocol.ReleaseLine{{Line: "1.4", Support: protocol.ReleaseLine_CERTIFIED, Linked: "1.4.2", Image: "example/engine:1.4"}}},
		{"a version of another line", `.*release line 1\.4 links engine version "1\.40\.1", which is not of the line`,
			[]*protocol.ReleaseLine{{Line: "1.4", Support: protocol.ReleaseLine_CERTIFIED, Linked: "1.40.1"}}},
		{"a version with a space", `.*release line 1\.4 links engine version "1\.4 2", which is not of the line`,
			[]*protocol.ReleaseLine{{Line: "1.4", Support: protocol.ReleaseLine_CERTIFIED, Linked: "1.4 2"}}},
		{"two linked lines", `.*release lines 1\.4, 1\.3 each name a linked engine, and an executable links one`,
			[]*protocol.ReleaseLine{{Line: "1.4", Support: protocol.ReleaseLine_CERTIFIED, Linked: "1.4.2"}, {Line: "1.3", Support: protocol.ReleaseLine_CERTIFIED, Linked: "1.3.0"}}},
		{"best effort with a linked engine and no reason", `.*release line 1\.4 has an image or a linked engine and no level that claims it`,
			[]*protocol.ReleaseLine{{Line: "1.4", Support: protocol.ReleaseLine_BEST_EFFORT, Linked: "1.4.2"}}},
		{"incompatible with a linked engine", `.*release line 1\.4 is incompatible and cannot be exercised`,
			[]*protocol.ReleaseLine{{Line: "1.4", Support: protocol.ReleaseLine_KNOWN_INCOMPATIBLE, Linked: "1.4.2"}}},
		{"the feature without a linked line", `.*an engine names a linked engine exactly when it lists engine\.linked-release-lines`,
			[]*protocol.ReleaseLine{{Line: "1.4", Support: protocol.ReleaseLine_CERTIFIED, Image: "example/engine:1.4"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := provider.ValidateDescription(linkedDescription(test.lines...), nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
	t.Run("a linked line without the engine's feature", func(t *testing.T) {
		c := qt.New(t)
		err := provider.ValidateDescription(linesDescription(linkedLines()...), nil)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(err, qt.ErrorMatches, `.*an engine names a linked engine exactly when it lists engine\.linked-release-lines`)
	})
	t.Run("the engine's feature without the provider's", func(t *testing.T) {
		c := qt.New(t)
		description := linkedDescription(linkedLines()...)
		description.Features = withoutFeature(description.Features, provider.LinkedReleaseLinesFeature)
		err := provider.ValidateDescription(description, nil)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(err, qt.ErrorMatches, `.*linked release lines require the provider's feature`)
	})
}
