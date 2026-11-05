package tablerebuild_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/tablerebuild"
)

func TestRequested_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "asked", args: []string{"--allow-table-rebuild"}, want: true},
		{name: "not asked", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := &cobra.Command{Use: "probe"}
			tablerebuild.Register(cmd)
			c.Assert(cmd.Flags().Parse(test.args), qt.IsNil)

			got, err := tablerebuild.Requested(cmd)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
