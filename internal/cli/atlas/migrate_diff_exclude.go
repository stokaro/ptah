package atlas

import (
	"os"
	"strings"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/atlascompatpolicy"
)

// atlasMigrateDiffExcludeEnvVar names the objects `migrate diff` keeps out of
// both sides of the comparison, on the full compatibility surface.
//
// WHY AN ENVIRONMENT VARIABLE AND NOT A FLAG. The pinned community binary
// registers no --exclude on `migrate diff`: v1.3.0 answers `Error: unknown
// flag: --exclude` and exits 1, and the Atlas CLI reference lists no such flag
// for the command either. A flag here is one neither edition has, and the
// conformance `cli-surface` tier, which asserts flag parity with the pinned
// binary, reports it. The Atlas spelling of the same selection is the selected
// env's `exclude` list, which this command reads in both profiles; the
// variable serves a run with no atlas.hcl, or one whose env must stay as it is.
//
// WHY NOT PTAH_EXCLUDE. The generic flag binding derives that name from
// --exclude on every compatibility command that registers the flag (`schema
// apply`, `schema diff`, `schema inspect`, `schema clean`, `schema plan`) and
// on the native verbs that do. Reading it here would give the name a second
// kind of reader on a command with no such flag, keep it out of the generated
// flag reference, and let a value exported for `schema apply` change the
// migration files this command writes. No flag either command tree registers
// derives this name.
//
// The value is a comma-separated list of selectors in the grammar of the env's
// `exclude` list. A value naming at least one selector replaces that list, as
// a command-line --exclude does on the commands that have one; the patterns a
// disabled `schema.mode` implies are added either way. A value naming none --
// empty, blank, or nothing but commas and spaces -- leaves the env's list in
// place, as an empty value does for every flag twin that is not a boolean.
//
// Strict mode refuses the variable by name, even when it is empty, before any
// command runs: it is one of the presence-gated variables
// [atlascompatpolicy.Resolve] refuses.
const atlasMigrateDiffExcludeEnvVar = "PTAH_ATLAS_MIGRATE_DIFF_EXCLUDE"

// atlasMigrateDiffExcludeFromEnv returns the selectors the variable names,
// trimmed and split on commas as the filter splits them, and nil when it is
// absent or names none.
//
// The strict profile refuses a present value here as well. The ptah-compat
// process refuses it before the command tree exists, so this answer is for a
// strict tree built without that boundary: such a tree must not honor what
// strict mode refuses, and must not drop it in silence either.
func atlasMigrateDiffExcludeFromEnv(policy atlascompatpolicy.Policy) ([]string, error) {
	value, present := os.LookupEnv(atlasMigrateDiffExcludeEnvVar)
	if !present {
		return nil, nil
	}
	if err := policy.ValidateFlagEnvironment(atlasMigrateDiffExcludeEnvVar, value, "stringArray"); err != nil {
		return nil, err
	}
	var selectors []string
	for part := range strings.SplitSeq(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			selectors = append(selectors, part)
		}
	}
	return selectors, nil
}

// atlasMigrateDiffExclude resolves the exclusion of a run that selected an
// atlas.hcl env. The selectors fromEnv names replace the env's `exclude` list
// when there are any, and the patterns a disabled `schema.mode` implies are
// added either way.
func atlasMigrateDiffExclude(fromEnv []string, cfg projectconfig.Config) []string {
	values := cfg.Exclude
	if len(fromEnv) > 0 {
		values = fromEnv
	}
	return withAtlasSchemaModeExcludes(values, cfg)
}

// atlasMigrateDiffExcludeHelp is the paragraph `migrate diff --help` carries
// about [atlasMigrateDiffExcludeEnvVar], and empty in the strict profile,
// which refuses the variable.
func atlasMigrateDiffExcludeHelp(policy atlascompatpolicy.Policy) string {
	if policy.IsStrictCE() {
		return ""
	}
	return `

` + atlasMigrateDiffExcludeEnvVar + ` keeps the objects it selects out of both
sides of the comparison. It takes comma-separated selectors in the grammar of
the env's exclude list, and a value naming at least one selector replaces that
list. Strict Community Edition mode refuses the variable.`
}
