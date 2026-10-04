package atlas

import (
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/envbool"
)

// atlasTableRebuildEnvVar asks the verbs of this surface that plan a schema
// change to plan a change the target cannot make in place as a table rebuild:
// on YDB, a primary key change, a column type change and SET NOT NULL. Native
// commands ask with --allow-table-rebuild; this surface takes no flag the
// pinned binary lacks, so it reads a variable instead.
const atlasTableRebuildEnvVar = "PTAH_ALLOW_TABLE_REBUILD"

// atlasTableRebuildRequest is how this surface asks for a rebuild, as the
// planner's refusal names it.
const atlasTableRebuildRequest = atlasTableRebuildEnvVar + "=1"

// atlasTableRebuild is the declaration of the variable, made once, on the
// verbs that own it. See [ptah.run/internal/envbool]. It is
// [ptah.run/internal/envbool.Gated]: the pinned community binary plans no
// table rebuild on any engine it drives, so strict mode refuses it.
var atlasTableRebuild = envbool.New(atlasTableRebuildEnvVar, false, envbool.Gated)

// atlasTableRebuildRequested reports whether the operator asked for table
// rebuilds. Unset and a valid false keep them refused; an empty or unparsable
// value is a configuration error. Each verb that plans resolves it before its
// first early return, so a malformed value fails every run of that verb.
func atlasTableRebuildRequested() (bool, error) {
	return atlasTableRebuild.Resolve()
}

// withAtlasTableRebuild returns policy with this surface's rebuild request in
// it: whether the operator asked, and the words a refusal uses to say how to
// ask. The words are set whether or not the operator asked, because the
// refusal is what an operator who did not ask reads.
func withAtlasTableRebuild(policy atlasschema.DiffPolicy, requested bool) atlasschema.DiffPolicy {
	policy.AllowTableRebuild = requested
	policy.TableRebuildRequest = atlasTableRebuildRequest
	return policy
}
