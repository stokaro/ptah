package atlas

import (
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/envbool"
)

// atlasCheckScanEnvVar asks the verbs of this surface that plan a schema
// change to add a CHECK constraint without the SELECTs that scan the rows the
// table holds for one it refuses, which a ClickHouse plan writes because its
// ADD CONSTRAINT checks only the rows written after it. The native commands
// bind the same name to --skip-check-scan; this surface takes no flag the
// pinned binary lacks, so it reads the variable alone.
const atlasCheckScanEnvVar = "PTAH_SKIP_CHECK_SCAN"

// atlasCheckScan is the declaration of the variable, made once, on the verbs
// that own it. See [ptah.run/internal/envbool]. It is
// [ptah.run/internal/envbool.Gated]: the scans are statements only Ptah
// plans, on an engine the pinned community binary has no driver for, so the
// variable changes a plan that binary cannot make. It loosens a safety
// statement rather than restoring or tightening one, which is what
// [ptah.run/internal/envbool.Retained] is for, so strict mode refuses it.
var atlasCheckScan = envbool.New(atlasCheckScanEnvVar, false, envbool.Gated)

// atlasPlanningRequests are the PTAH_* planning toggles every verb of this
// surface that plans a schema change owns: the table rebuild request and the
// CHECK scan opt-out. Each verb resolves both before its first early return,
// so a malformed value of either fails every run of that verb.
type atlasPlanningRequests struct {
	rebuild, skipCheckScan bool
}

// resolveAtlasPlanningRequests reads both variables. Unset and a valid false
// keep the default; an empty or unparsable value is a configuration error.
func resolveAtlasPlanningRequests() (atlasPlanningRequests, error) {
	rebuild, err := atlasTableRebuildRequested()
	if err != nil {
		return atlasPlanningRequests{}, err
	}
	skipCheckScan, err := atlasCheckScan.Resolve()
	if err != nil {
		return atlasPlanningRequests{}, err
	}
	return atlasPlanningRequests{rebuild: rebuild, skipCheckScan: skipCheckScan}, nil
}

// applyTo returns policy with both requests in it.
func (r atlasPlanningRequests) applyTo(policy atlasschema.DiffPolicy) atlasschema.DiffPolicy {
	policy = withAtlasTableRebuild(policy, r.rebuild)
	policy.SkipCheckScan = r.skipCheckScan
	return policy
}
