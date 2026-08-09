package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
)

// TestKeepAtlasRefusedBlocksReadsTheOptIn pins how the opt-in is parsed.
//
// The variable is the whole reason the compatibility surface may default to a
// narrower document at all: a capability that cannot be reached is a capability
// that was removed (AGENTS.md, "Compatibility never removes a capability"). The
// rows mirror [go.5x5.cz/ptah/internal/atlassource]'s own opt-in, so an
// operator who learned one spelling is not surprised by the other -- absence
// and a valid false keep the default, and anything else is refused by name and
// by value (stokaro/ptah#1334).
func TestKeepAtlasRefusedBlocksReadsTheOptIn(t *testing.T) {
	tests := []struct {
		name        string
		env         func(testing.TB)
		want        bool
		wantMessage string
	}{
		{
			name: "unset keeps the compatible default",
			env:  envbooltest.Unset(atlashclrender.KeepAtlasRefusedBlocksEnvVar),
		},
		{
			name: "1 restores the superset",
			env:  envbooltest.Set(atlashclrender.KeepAtlasRefusedBlocksEnvVar, "1"),
			want: true,
		},
		{
			name: "true restores the superset",
			env:  envbooltest.Set(atlashclrender.KeepAtlasRefusedBlocksEnvVar, "true"),
			want: true,
		},
		{
			name: "TRUE restores the superset",
			env:  envbooltest.Set(atlashclrender.KeepAtlasRefusedBlocksEnvVar, "TRUE"),
			want: true,
		},
		{
			name: "0 keeps the compatible default",
			env:  envbooltest.Set(atlashclrender.KeepAtlasRefusedBlocksEnvVar, "0"),
		},
		{
			name: "false keeps the compatible default",
			env:  envbooltest.Set(atlashclrender.KeepAtlasRefusedBlocksEnvVar, "false"),
		},
		{
			name:        "an unparsable value is refused",
			env:         envbooltest.Set(atlashclrender.KeepAtlasRefusedBlocksEnvVar, "yes please"),
			wantMessage: `invalid boolean value "yes please" for PTAH_ATLAS_INSPECT_ALL_BLOCKS`,
		},
		{
			name:        "an exported empty value is refused",
			env:         envbooltest.Set(atlashclrender.KeepAtlasRefusedBlocksEnvVar, ""),
			wantMessage: `invalid boolean value "" for PTAH_ATLAS_INSPECT_ALL_BLOCKS`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.env(t)

			got, err := atlashclrender.KeepAtlasRefusedBlocks()

			c.Assert(got, qt.Equals, test.want)
			c.Assert(errMessage(err), qt.Equals, test.wantMessage)
		})
	}
}

// errMessage renders an error for comparison against a table row without a
// branch in the test body.
func errMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
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
