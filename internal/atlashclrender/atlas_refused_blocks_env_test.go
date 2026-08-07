package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashclrender"
)

// TestKeepAtlasRefusedBlocksReadsTheOptIn pins how the opt-in is parsed.
//
// The variable is the whole reason the compatibility surface may default to a
// narrower document at all: a capability that cannot be reached is a capability
// that was removed (AGENTS.md, "Compatibility never removes a capability"). The
// rows mirror [go.5x5.cz/ptah/internal/atlassource]'s own opt-in, so an
// operator who learned one spelling is not surprised by the other -- unset,
// empty, false and unparsable all keep the default.
func TestKeepAtlasRefusedBlocksReadsTheOptIn(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "unset keeps the compatible default", value: "", want: false},
		{name: "1 restores the superset", value: "1", want: true},
		{name: "true restores the superset", value: "true", want: true},
		{name: "TRUE restores the superset", value: "TRUE", want: true},
		{name: "0 keeps the compatible default", value: "0", want: false},
		{name: "false keeps the compatible default", value: "false", want: false},
		{name: "an unparsable value keeps the compatible default", value: "yes please", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlashclrender.KeepAtlasRefusedBlocksEnvVar, test.value)

			c.Assert(atlashclrender.KeepAtlasRefusedBlocks(), qt.Equals, test.want)
		})
	}
}

// TestKeepAtlasRefusedBlocksEnvVarIsSpelledLikeItsPrecedent pins the name the
// diagnostics, the documentation and the feature matrix all quote.
//
// The spelling is part of the interface: an operator meets it in a stderr
// warning and types it back. Renaming it silently would leave every document
// that quotes it wrong, so the constant is asserted rather than assumed.
func TestKeepAtlasRefusedBlocksEnvVarIsSpelledLikeItsPrecedent(t *testing.T) {
	c := qt.New(t)

	c.Assert(atlashclrender.KeepAtlasRefusedBlocksEnvVar, qt.Equals, "PTAH_ATLAS_INSPECT_ALL_BLOCKS")
}
