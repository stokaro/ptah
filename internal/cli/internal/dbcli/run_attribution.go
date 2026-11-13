package dbcli

import (
	"fmt"
	"strings"

	"github.com/spf13/pflag"

	"ptah.run/migration/migrator"
)

const (
	// ActorOriginFlagName is the flag that says where the --actor name came
	// from, in the caller's own word.
	ActorOriginFlagName = "actor-origin"
	// RunReferenceFlagName is the flag that hands a run the caller's own
	// identifier for it.
	RunReferenceFlagName = "run-reference"
)

// RegisterActorOriginFlag registers the caller's word for where the --actor
// name came from, recorded beside it in log.
func RegisterActorOriginFlag(flags *pflag.FlagSet, target *string, log string) {
	flags.StringVar(target, ActorOriginFlagName, "",
		"Where the --"+ActorFlagName+" name came from, recorded beside it in the "+log+
			": one lowercase word such as admission, up to 64 characters (unverified; requires --"+ActorFlagName+")")
}

// RegisterRunReferenceFlag registers the caller's identifier for the run,
// stored on every entry the run writes to log.
func RegisterRunReferenceFlag(flags *pflag.FlagSet, target *string, log string) {
	flags.StringVar(target, RunReferenceFlagName, "",
		"Caller's identifier for this run, stored on every entry it writes to the "+log+
			" and reported in --json: up to 255 visible ASCII characters, such as sha256:<64 hex digits>")
}

// CheckRunAttribution refuses what a run would record about itself and could
// not record as given: a run reference or an actor origin the log would store
// differently, and an origin with no actor for it to describe. It asks no
// database, so a command calls it before it connects.
func CheckRunAttribution(actor, origin, reference string) error {
	if err := migrator.ValidateRunReference(reference); err != nil {
		return fmt.Errorf("--%s: %w", RunReferenceFlagName, err)
	}
	if err := migrator.ValidateActorOrigin(origin); err != nil {
		return fmt.Errorf("--%s: %w", ActorOriginFlagName, err)
	}
	if origin != "" && strings.TrimSpace(actor) == "" {
		return fmt.Errorf("--%s says where the --%s name came from, and no --%s was given",
			ActorOriginFlagName, ActorFlagName, ActorFlagName)
	}
	return nil
}
