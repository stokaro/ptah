package atlas

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"

	"ptah.run/internal/dblock"
)

// atlasLockTimeoutDefault is the pinned binary's --lock-timeout default on
// `schema apply`, `migrate apply` and `migrate diff`: its help prints
// `--lock-timeout duration   set how long to wait for the database lock
// (default 10s)` on all three.
const atlasLockTimeoutDefault = 10 * time.Second

// registerAtlasLockTimeoutFlag registers --lock-timeout the way the pinned
// binary does: a duration flag that defaults to [atlasLockTimeoutDefault].
//
// A duration flag is what gives a bad value the pinned binary's answer, in its
// place. Measured on 2026-09-26 on PostgreSQL 18 (stokaro/ptah#3689), the value
// is refused by the flag parser, before the verb runs and before a missing
// --url is named: `invalid argument "bogus" for "--lock-timeout" flag: time:
// invalid duration "bogus"`, and the same for an empty value and for a number
// with no unit. Zero and negative values are accepted; see [atlasLockWait].
//
// Native `ptah` keeps a string flag, where an empty value waits indefinitely
// and zero is refused: that surface has no community answer to match.
func registerAtlasLockTimeoutFlag(flags *pflag.FlagSet, target *time.Duration, usage string) {
	flags.DurationVar(target, "lock-timeout", atlasLockTimeoutDefault, usage)
}

// atlasLockWait is the wait a --lock-timeout value asks the lock layers for. A
// positive value waits that long. Zero and negative values do not wait: the
// lock is tried once, and a lock another run holds refuses the run at once.
// That is the pinned binary's reading, measured against a held lock:
// `migrate apply --lock-timeout 0` and `--lock-timeout -1s` both fail at once,
// and `--lock-timeout 2s` fails after two seconds. The lock layers read zero as
// "wait indefinitely", so zero is turned into [dblock.NoWait] here.
func atlasLockWait(timeout time.Duration) time.Duration {
	if timeout > 0 {
		return timeout
	}
	return dblock.NoWait
}

// atlasLockTimeoutFromProject reads an atlas.hcl `migration { lock_timeout }`
// value the way the pinned binary does: it sets --lock-timeout from the
// attribute, so a bad value is refused in the flag parser's words, and an
// empty attribute leaves the default in place. Measured with
// `migrate apply --env local` on the same values as the flag.
func atlasLockTimeoutFromProject(value string) (time.Duration, bool, error) {
	if value == "" {
		return 0, false, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil {
		return 0, false, fmt.Errorf("invalid argument %q for %q flag: %w", value, "--lock-timeout", err)
	}
	return timeout, true, nil
}
