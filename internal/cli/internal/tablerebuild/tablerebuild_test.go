package tablerebuild_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/core/planext"
	"ptah.run/internal/cli/internal/tablerebuild"
	"ptah.run/internal/planner/dialects/ydb"
)

// A planner's refusal tells the operator which flag asks for a rebuild, so
// the spellings are one: a renamed flag would leave the refusal pointing at a
// flag no command has. The built-in YDB planner names its constant, and a
// provider receives the shared default in its planning policy.
func TestFlagName_IsTheOneThePlannerNames(t *testing.T) {
	c := qt.New(t)
	c.Assert("--"+tablerebuild.FlagName, qt.Equals, ydb.TableRebuildFlag)
	c.Assert("--"+tablerebuild.FlagName, qt.Equals, planext.DefaultTableRebuildRequest)
}

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
