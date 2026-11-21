package atlas

// White-box testing required: the refusals below fire only for a verb whose
// repeatable flag forwards to a native flag that is missing or of another
// type, and the shipped tree holds no such verb, so no exported entry point
// can build one. The verbs the tree does hold are tested black-box in
// repeated_flags_test.go.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/atlasargs"
)

// fakeRepeatedVerb is a verb whose repeatable --name forwards to the native
// --target of a command that registers only an integer --count.
func fakeRepeatedVerb(target string) atlasVerb {
	return atlasVerb{
		use:    "fake",
		native: "fake native",
		factory: func() *cobra.Command {
			cmd := &cobra.Command{Use: "native"}
			cmd.Flags().Int("count", 0, "a number")
			return cmd
		},
		flags: []atlasargs.Flag{atlasargs.NativeStringArray("name", "", "names", target)},
	}
}

func TestAtlasRepeatedFlagsForTarget_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		target string
		want   string
	}{
		{name: "the native flag is missing", target: "names",
			want: `atlas schema fake --name forwards to --names, which ptah fake native does not register`},
		{name: "the native flag takes neither form", target: "count",
			want: `atlas schema fake --name forwards to --count of type int, which takes neither a joined list nor one value per occurrence`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			verb := fakeRepeatedVerb(tt.target)

			c.Assert(func() { atlasRepeatedFlagsForTarget("schema", verb) }, qt.PanicMatches, tt.want)
		})
	}
}
