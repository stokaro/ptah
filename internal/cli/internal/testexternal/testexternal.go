// Package testexternal owns the authorization the native test commands need
// before a `.test.hcl` case may run a program. It declares the environment
// variable once and resolves it, so `ptah schema test` and
// `ptah migrations test` cannot answer the question differently.
package testexternal

import (
	"ptah.run/internal/envbool"
	"ptah.run/migration/dbtest"
)

// Allowed reports whether this invocation authorizes an external test step.
//
// Resolve it before the command does any work: a malformed value is a
// configuration error the operator already knows they changed, and letting it
// stay dormant until a run happens to reach an external step means every
// healthy run hides it.
func Allowed() (bool, error) {
	return allowExternal.Resolve()
}

// allowExternal is the declaration of the variable, made once, on the surface
// that owns it. See [ptah.run/internal/envbool].
//
// It is [ptah.run/internal/envbool.Retained]. The pinned community binary
// runs an `external` step's program with no authorization at all, so refusing
// one by default takes nothing away from Atlas compatibility and this variable
// adds no capability beyond it -- it restores what that binary already does.
// Strict compatibility therefore keeps it reachable, which is what a
// conformance run exercising `external` needs.
//
// The default is false because the step names a program on the machine running
// the suite, which is a larger authority than the rest of a test file has, and
// a boolean feature toggle opts in to the more permissive side so a typo lands
// on the safe answer.
var allowExternal = envbool.New(dbtest.AllowExternalCommandsEnvVar, false, envbool.Retained)
