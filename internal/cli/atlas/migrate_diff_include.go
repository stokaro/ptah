package atlas

import (
	"os"
	"strings"

	"ptah.run/internal/atlascompatpolicy"
)

// atlasMigrateDiffIncludeEnvVar names the objects `migrate diff` keeps on both
// sides of the comparison, on the full compatibility surface: a positive
// selection with the meaning `schema apply --include` gives it. An object it
// does not select is neither created nor dropped, so a migration directory
// whose history built objects the desired state does not own plans nothing
// for them.
//
// It is a variable rather than a flag for the reason
// [atlasMigrateDiffExcludeEnvVar] gives: the pinned community binary registers
// no --include on `migrate diff`, and the conformance `cli-surface` tier
// asserts flag parity with it. It is not PTAH_INCLUDE either, which the generic
// flag binding derives from --include on the schema commands. atlas.hcl has no
// `include` list for this command to read, so the variable is the only
// spelling.
//
// The value is a comma-separated list of selectors in the grammar of
// `schema apply --include`. A value naming none -- empty, blank, or nothing but
// commas and spaces -- selects everything, as an unset variable does. The
// exclusion applies first, so a selected table without an index it names keeps
// its other children.
//
// Strict mode refuses the variable by name, even when it is empty, before any
// command runs: it is one of the presence-gated variables
// [atlascompatpolicy.Resolve] refuses.
const atlasMigrateDiffIncludeEnvVar = "PTAH_ATLAS_MIGRATE_DIFF_INCLUDE"

// atlasMigrateDiffIncludeFromEnv returns the selectors the variable names,
// trimmed and split on commas, and nil when it is absent or names none. The
// strict profile refuses a present value here as well, for the reason
// [atlasMigrateDiffExcludeFromEnv] gives.
func atlasMigrateDiffIncludeFromEnv(policy atlascompatpolicy.Policy) ([]string, error) {
	value, present := os.LookupEnv(atlasMigrateDiffIncludeEnvVar)
	if !present {
		return nil, nil
	}
	if err := policy.ValidateFlagEnvironment(atlasMigrateDiffIncludeEnvVar, value, "stringArray"); err != nil {
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

// atlasMigrateDiffIncludeHelp is the paragraph `migrate diff --help` carries
// about [atlasMigrateDiffIncludeEnvVar], and empty in the strict profile,
// which refuses the variable.
func atlasMigrateDiffIncludeHelp(policy atlascompatpolicy.Policy) string {
	if policy.IsStrictCE() {
		return ""
	}
	return `

` + atlasMigrateDiffIncludeEnvVar + ` keeps only the objects it selects on both
sides of the comparison, so the migration neither creates nor drops anything
else. It takes comma-separated selectors in the grammar of schema apply
--include, and applies after the exclude list. Strict Community Edition mode
refuses the variable.`
}
