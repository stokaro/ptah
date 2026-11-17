package testutils_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testutils"
)

func TestWithoutAtlasCommunityBuildNotice_HappyPath(t *testing.T) {
	const refusal = "Error: migration file 2_b.sql was added out of order.\n"
	tests := []struct {
		name   string
		output string
		want   string
	}{
		{
			name: "the notice and everything after it go",
			output: refusal + "You're running the community build of Atlas, which differs from the official version.\n" +
				"If this error persists, try installing the official version as a troubleshooting step:\n",
			want: refusal,
		},
		{
			name:   "output without the notice is unchanged",
			output: refusal,
			want:   refusal,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(testutils.WithoutAtlasCommunityBuildNotice(tc.output), qt.Equals, tc.want)
		})
	}
}
