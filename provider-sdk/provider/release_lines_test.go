package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

func linesDescription(lines ...*protocol.ReleaseLine) *protocol.Description {
	description, _, _ := creationExchange()
	description.Engines[0].ReleaseLines = lines
	return description
}

func TestReleaseLines_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		lines []*protocol.ReleaseLine
	}{
		{"none declared", nil},
		{"certified with an image", []*protocol.ReleaseLine{{Line: "26.2", Support: protocol.ReleaseLine_CERTIFIED, Image: "example/engine:26.2"}}},
		{"legacy tested with a label", []*protocol.ReleaseLine{{Line: "25.1", Label: "Engine 25", Support: protocol.ReleaseLine_LEGACY_TESTED, Image: "example/engine:25.1"}}},
		{"best effort with a reason", []*protocol.ReleaseLine{{Line: "24.3", Support: protocol.ReleaseLine_BEST_EFFORT, Unprobed: "end of life", Image: "example/engine:24.3"}}},
		{"best effort without an image", []*protocol.ReleaseLine{{Line: "23.1", Support: protocol.ReleaseLine_BEST_EFFORT}}},
		{"known incompatible", []*protocol.ReleaseLine{{Line: "22.1", Support: protocol.ReleaseLine_KNOWN_INCOMPATIBLE, Note: "no ALTER TABLE"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateDescription(linesDescription(test.lines...), nil), qt.IsNil)
		})
	}
}

func TestReleaseLines_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		lines []*protocol.ReleaseLine
	}{
		{"empty line", []*protocol.ReleaseLine{{Support: protocol.ReleaseLine_BEST_EFFORT}}},
		{"line with a space", []*protocol.ReleaseLine{{Line: "26 2", Support: protocol.ReleaseLine_BEST_EFFORT}}},
		{"line with outer space", []*protocol.ReleaseLine{{Line: " 26.2", Support: protocol.ReleaseLine_BEST_EFFORT}}},
		{"declared twice", []*protocol.ReleaseLine{{Line: "26.2", Support: protocol.ReleaseLine_BEST_EFFORT}, {Line: "26.2", Support: protocol.ReleaseLine_BEST_EFFORT}}},
		{"no level", []*protocol.ReleaseLine{{Line: "26.2"}}},
		{"certified without an image", []*protocol.ReleaseLine{{Line: "26.2", Support: protocol.ReleaseLine_CERTIFIED}}},
		{"certified and unprobed", []*protocol.ReleaseLine{{Line: "26.2", Support: protocol.ReleaseLine_CERTIFIED, Image: "i", Unprobed: "gone"}}},
		{"legacy tested without an image", []*protocol.ReleaseLine{{Line: "26.2", Support: protocol.ReleaseLine_LEGACY_TESTED}}},
		{"best effort with an image and no reason", []*protocol.ReleaseLine{{Line: "26.2", Support: protocol.ReleaseLine_BEST_EFFORT, Image: "i"}}},
		{"blank reason", []*protocol.ReleaseLine{{Line: "26.2", Support: protocol.ReleaseLine_BEST_EFFORT, Unprobed: "  "}}},
		{"incompatible with an image", []*protocol.ReleaseLine{{Line: "26.2", Support: protocol.ReleaseLine_KNOWN_INCOMPATIBLE, Image: "i"}}},
		{"invalid text", []*protocol.ReleaseLine{{Line: "26.2", Support: protocol.ReleaseLine_BEST_EFFORT, Note: "\xff"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateDescription(linesDescription(test.lines...), nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
