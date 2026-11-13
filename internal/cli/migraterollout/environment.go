package migraterollout

import (
	"fmt"
	"strings"

	"ptah.run/internal/cli/migrateup"
	"ptah.run/internal/envbinding"
)

// groupBypassFlags are the `ptah migrations up` flags whose variable would
// reach every target of a rollout at once.
//
// Each one either waives a guard a target owes its own evidence for -- a dirty
// revision row, a pre-migration check, the destructive lint gate, the approved
// sequence -- or changes what a target run does in a way the rollout's
// outcomes do not describe: a dry run, a run that stops short of the
// release, or an execution order that applies or skips a migration below the
// current version, which waives the ordering guard every target run applies.
// The per-target spellings in the definition, expect_sequence and
// allow_destructive, are the ones a rollout reads.
var groupBypassFlags = []string{
	"allow-dirty",
	"skip-checks",
	"allow-destructive",
	"expect-sequence",
	"dry-run",
	"limit",
	"to-version",
	"exec-order",
}

// refuseGroupBypasses refuses a rollout started with any of the variables
// groupBypassFlags bind set, whatever its value. A target run inherits the
// rollout's environment, so an exported variable would take a decision for
// every target that the rollout only lets a definition take for one.
//
// The names come from the `migrations up` command itself, and a flag missing
// there refuses every rollout rather than leaving its variable unguarded.
func refuseGroupBypasses(lookupEnv func(string) (string, bool)) error {
	up := migrateup.NewMigrateUpCommand()
	var set []string
	for _, name := range groupBypassFlags {
		flag := up.Flags().Lookup(name)
		if flag == nil {
			return fmt.Errorf("ptah migrations up has no --%s flag to guard", name)
		}
		variable, bound := envbinding.Of(envPrefix, flag)
		if !bound {
			continue
		}
		if _, present := lookupEnv(variable); present {
			set = append(set, variable)
		}
	}
	if len(set) == 0 {
		return nil
	}
	return fmt.Errorf("%s set: a rollout passes its environment to every target, so it does not run with a variable "+
		"that waives a target's guard or changes what its run does; unset it, and set expect_sequence or "+
		"allow_destructive on the targets that need them", strings.Join(set, ", "))
}
