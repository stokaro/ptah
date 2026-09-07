package atlas

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// The named-lock family: `--lock-name` and `--skip-lock`.
//
// ATLAS-SIDE SOURCE. Atlas's published CLI reference (atlasgo.io/cli-reference)
// registers both flags on `atlas migrate apply` and on `atlas schema apply`,
// and on no other verb:
//
//	--lock-name string   set the name of the advisory lock to use. If not set,
//	                     the default is used (atlas_migrate_execute)
//	--skip-lock          skip acquiring a database advisory lock
//
// The pinned Atlas community binary v1.3.0 registers neither, on any verb —
// `migrate apply --lock-name x` and `schema apply --skip-lock` both answer
// `unknown flag`, with `--dry-run` answering present on the same run and
// `--frobnicate-nonsense` answering unknown. So this is Pro surface adopted
// openly, sourced from the published reference rather than from the binary.
//
// The same reference does NOT register `--lock-name` on `migrate diff`,
// `migrate down` or `migrate checkpoint`: those three carry `--lock-timeout`
// and nothing else from this family. Those spellings are therefore absent here
// on purpose (stokaro/ptah#951 standing constraint: no compat flag without an
// Atlas-side source).
//
// WHY THE DEFAULT NAME IS NOT atlas_migrate_execute. Ptah's own default lock
// names (`ptah_migrate`, `ptah_schema_apply`) are unchanged, because moving
// them would silently de-serialize a fleet mid-upgrade: an older ptah-compat
// holding `ptah_migrate` and a newer one holding `atlas_migrate_execute` do not
// see each other. `--lock-name atlas_migrate_execute` is the explicit spelling
// for coordinating with Atlas, and is why the flag has to exist.

const (
	atlasLockNameFlag = "lock-name"
	atlasSkipLockFlag = "skip-lock"
)

// atlasLockOptions holds the raw named-lock flag values for one compat verb.
type atlasLockOptions struct {
	name string
	skip bool
}

// atlasLockRequest is the resolved named-lock decision handed to the lock
// machinery. A zero Name means "this verb's default lock name"; Skip means no
// lock is requested at all.
type atlasLockRequest struct {
	Name string
	Skip bool
}

// registerAtlasLockNameFlag registers `--lock-name` on a verb that Atlas
// registers it on. Type and default match the reference: a string with no
// default, where "unset" means the tool's own default lock name.
func registerAtlasLockNameFlag(flags *pflag.FlagSet, opts *atlasLockOptions) {
	flags.StringVar(&opts.name, atlasLockNameFlag, "",
		"Name of the database advisory lock to acquire; unset uses the default lock name")
}

// registerAtlasSkipLockFlag registers `--skip-lock` on a verb that Atlas
// registers it on.
func registerAtlasSkipLockFlag(flags *pflag.FlagSet, opts *atlasLockOptions) {
	flags.BoolVar(&opts.skip, atlasSkipLockFlag, false,
		"Skip acquiring the database advisory lock")
}

// resolveAtlasLockRequest turns the raw flag values into the lock decision the
// machinery receives.
//
// Two inputs are refused rather than interpreted:
//
//   - An explicit but blank `--lock-name`. Passing it through would fall back
//     to the default lock name, so a caller that meant to name a lock would
//     silently coordinate on a different one.
//   - `--lock-name` together with `--skip-lock`. There is no lock to name when
//     no lock is taken, and Atlas's reference documents no resolution order for
//     the pair. Honouring either one would drop the other silently, which is
//     the failure this whole family exists to avoid; refusing says so instead.
func resolveAtlasLockRequest(cmd *cobra.Command, opts atlasLockOptions) (atlasLockRequest, error) {
	flags := cmd.Flags()
	named := flags.Changed(atlasLockNameFlag)
	if named && strings.TrimSpace(opts.name) == "" {
		return atlasLockRequest{}, fmt.Errorf("--%s must not be empty", atlasLockNameFlag)
	}
	if named && opts.skip {
		return atlasLockRequest{}, fmt.Errorf(
			"--%s and --%s cannot be used together: --%s takes no lock, so there is no lock to name",
			atlasLockNameFlag, atlasSkipLockFlag, atlasSkipLockFlag,
		)
	}
	return atlasLockRequest{Name: strings.TrimSpace(opts.name), Skip: opts.skip}, nil
}
