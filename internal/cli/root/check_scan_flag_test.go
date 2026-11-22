package root_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/checkscan"
	"ptah.run/internal/cli/internal/tablerebuild"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/envbool/envbooltest"
)

// planningVerbs walks the built tree for the commands that take flag.
func planningVerbs(flag string) []string {
	var verbs []string
	var walk func(cmd *cobra.Command)
	walk = func(cmd *cobra.Command) {
		if cmd.Flags().Lookup(flag) != nil {
			verbs = append(verbs, strings.TrimPrefix(cmd.CommandPath(), "ptah "))
		}
		for _, child := range cmd.Commands() {
			walk(child)
		}
	}
	walk(root.NewRootCommand())
	slices.Sort(verbs)
	return verbs
}

// Every command that plans a schema change takes --skip-check-scan: the set
// is the one that takes --allow-table-rebuild, walked from the built tree, so a
// planning verb added to one set and not the other fails here.
func TestCheckScanFlag_EveryPlanningVerbTakesIt(t *testing.T) {
	c := qt.New(t)

	got := planningVerbs(checkscan.FlagName)

	c.Assert(got, qt.DeepEquals, planningVerbs(tablerebuild.FlagName))
	c.Assert(got, qt.DeepEquals, []string{
		"migrations generate", "migrations plan", "schema apply", "schema compare", "schema diff", "schema plan",
	})
}

// The help of each verb names the variable the flag reads.
func TestCheckScanFlag_HelpNamesTheVariable(t *testing.T) {
	for _, verb := range planningVerbs(checkscan.FlagName) {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)

			stdout, _, err := executeRootCommand(append(strings.Fields(verb), "--help")...)

			c.Assert(err, qt.IsNil)
			c.Assert(stdout, qt.Matches, `(?s).*\n\s+--skip-check-scan\s+Add a CHECK constraint without the SELECTs[^\n]*\[env: PTAH_SKIP_CHECK_SCAN\]\n.*`)
		})
	}
}

// A malformed value is refused before the verb does any work: each invocation
// names nothing to plan, so a verb that read the variable later would stop on
// that instead.
func TestCheckScanFlag_EveryPlanningVerbRefusesAMalformedVariable(t *testing.T) {
	for _, verb := range planningVerbs(checkscan.FlagName) {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set("PTAH_SKIP_CHECK_SCAN", "maybe")(t)

			_, _, err := executeRootCommand(strings.Fields(verb)...)

			c.Assert(err, qt.ErrorMatches, `invalid boolean value "maybe" for PTAH_SKIP_CHECK_SCAN`)
		})
	}
}

// The control for the rows above: with a valid value each verb goes on to a
// refusal of its own.
func TestCheckScanFlag_EveryPlanningVerbTakesAValidVariable(t *testing.T) {
	for _, verb := range planningVerbs(checkscan.FlagName) {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set("PTAH_SKIP_CHECK_SCAN", "1")(t)

			_, _, err := executeRootCommand(strings.Fields(verb)...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err, qt.Not(qt.ErrorMatches), `.*PTAH_SKIP_CHECK_SCAN.*`)
		})
	}
}
